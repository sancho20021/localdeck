#!/usr/bin/env python3
"""
Generates a single print-ready card as a CMYK PDF:
  - card drawn as vectors (shapes, text, QR code); only the artwork is an image,
    kept at source resolution up to ARTWORK_MAX_DPI (300)
  - bleed around the trim, filled with the card's outer edge color
  - a white strip around the bleed carrying crop marks
  - crop marks start slightly inside the bleed, with small white knockouts
    under them so they stay visible on the colored bleed
  - black text, QR code and crop marks are pure K (no rich black)
  - TrimBox / BleedBox are set, the CMYK profile is embedded as output intent
"""

from __future__ import annotations

import argparse
import io
import math
import os
from typing import List, Tuple

import pikepdf
from pikepdf import Array, Dictionary, Name, String
from PIL import Image, ImageCms
from PIL.Image import Image as PILImage
from reportlab import rl_config
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen.canvas import Canvas

from generate_card import (
    ARTWORK_DIR,
    CARD_HEIGHT_MM,
    CARD_WIDTH_MM,
    FONT_PATH,
    OUTPUT_DIR,
    QR_MARGIN_MODULES,
    CardData,
    ArtworkNeeded,
    parse_color,
    prepare_card,
    qr_box,
    qr_module_px,
    qr_modules,
)

# =============================
# CONFIG
# =============================

MM_TO_INCH = 25.4
PT_PER_INCH = 72

BLEED_MM = 3            # colored bleed beyond the trim line
MARGIN_MM = 10          # trim line to page edge (bleed + white strip)
MARK_OFFSET_MM = 2      # trim line to the start of a crop mark (inside the bleed)
MARK_LEN_MM = 7         # crop mark length
MARK_WIDTH_PT = 0.25    # crop mark stroke width
KNOCKOUT_MM = 1         # width of the white patch under a mark on the bleed

ARTWORK_MAX_DPI = 300   # what presses can resolve in photos (~2x a 150 lpi screen); never upscaled
JPEG_QUALITY = 95

DEFAULT_ICC = "/usr/share/color/icc/colord/FOGRA27L_coated.icc"

# Same typeface as FONT_PATH, but with TrueType outlines that can be embedded in a PDF
PDF_FONT_PATH = os.path.join(os.path.dirname(FONT_PATH), "static", "Montserrat-SemiBold.ttf")
PDF_FONT_NAME = "Montserrat-SemiBold"

K_BLACK = (0, 0, 0, 1)
NO_INK = (0, 0, 0, 0)

rl_config.useA85 = 0  # keep binary streams binary, ASCII85 only bloats the file

# =============================
# HELPERS
# =============================

Box = Tuple[float, float, float, float]  # left, bottom, right, top in points


def mm_to_pt(mm: float) -> float:
    return mm / MM_TO_INCH * PT_PER_INCH


class CmykConverter:
    def __init__(self, icc_path: str):
        self.transform = ImageCms.buildTransform(
            ImageCms.createProfile("sRGB"),
            ImageCms.getOpenProfile(icc_path),
            "RGB",
            "CMYK",
            renderingIntent=ImageCms.Intent.RELATIVE_COLORIMETRIC,
            flags=ImageCms.Flags.BLACKPOINTCOMPENSATION,
        )

    def image(self, img: PILImage) -> PILImage:
        return ImageCms.applyTransform(img.convert("RGB"), self.transform)

    def color(self, color: str | tuple) -> Tuple[float, float, float, float]:
        """A single RGB color as CMYK fractions (0..1)."""
        pixel = self.image(Image.new("RGB", (1, 1), color)).getpixel((0, 0))
        return tuple(v / 255 for v in pixel)


def crop_mark_segments(trim: Box, start: float, end: float) -> List[Box]:
    """
    Segments (x0, y0, x1, y1) running outward from each trim corner along the
    trim lines, from `start` to `end` points away from the trim.
    """
    left, bottom, right, top = trim
    segments = []
    for x in (left, right):
        segments.append((x, top + start, x, top + end))
        segments.append((x, bottom - start, x, bottom - end))
    for y in (bottom, top):
        segments.append((left - start, y, left - end, y))
        segments.append((right + start, y, right + end, y))
    return segments


class CardCanvas:
    """
    Maps the card's pixel coordinates (origin top-left, see CardData) onto the
    PDF page, so that the card's pixel area lands exactly on the nominal trim.
    """

    def __init__(self, canvas: Canvas, card: CardData, trim: Box):
        self.c = canvas
        self.left, _, _, self.top = trim
        self.sx = (trim[2] - trim[0]) / card["width"]
        self.sy = (trim[3] - trim[1]) / card["height"]

    def point(self, x: float, y: float) -> Tuple[float, float]:
        return self.left + x * self.sx, self.top - y * self.sy

    def rect(self, x0: float, y0: float, x1: float, y1: float) -> Tuple[float, float, float, float]:
        """Pixel box -> reportlab (x, y, width, height)."""
        left, top = self.point(x0, y0)
        right, bottom = self.point(x1, y1)
        return left, bottom, right - left, top - bottom

# =============================
# DRAWING
# =============================

def draw_artwork(cc: CardCanvas, card: CardData, cmyk: CmykConverter) -> None:
    bezel = card["bezel"]
    size_px = card["width"] - 2 * bezel
    x, y, w, h = cc.rect(bezel, bezel, bezel + size_px, bezel + size_px)

    art = card["artwork"]
    if art is None:
        cc.c.setFillColorCMYK(*cmyk.color("black"))
        cc.c.rect(x, y, w, h, stroke=0, fill=1)
        return

    max_px = math.ceil(w / PT_PER_INCH * ARTWORK_MAX_DPI)
    if art.width > max_px:
        art = art.resize((max_px, max_px), Image.LANCZOS)

    jpeg = io.BytesIO()
    cmyk.image(art).save(jpeg, "JPEG", quality=JPEG_QUALITY)
    jpeg.seek(0)
    cc.c.drawImage(ImageReader(jpeg), x, y, w, h)

    print(f"Artwork: {art.width}px -> {art.width / (w / PT_PER_INCH):.0f} dpi")


def draw_text(cc: CardCanvas, card: CardData) -> None:
    bezel = card["bezel"]

    cc.c.setFillColorCMYK(*K_BLACK)
    for line in card["layout"]["text_lines"]:
        text = line["text"]
        if not text:
            continue
        font = line["font"]
        ascent, _ = font.getmetrics()

        text_obj = cc.c.beginText()
        text_obj.setFont(PDF_FONT_NAME, font.size * cc.sy)
        # Each glyph goes where Pillow puts it: reportlab doesn't kern, Pillow does.
        # Pillow places text by its ascender line, the PDF by its baseline.
        for i, char in enumerate(text):
            text_obj.setTextOrigin(*cc.point(
                line["x"] + bezel + font.getlength(text[:i]),
                line["y"] + bezel + ascent,
            ))
            text_obj.textOut(char)
        cc.c.drawText(text_obj)


def draw_qr(cc: CardCanvas, card: CardData) -> None:
    modules = qr_modules(card["play_url"])
    n = len(modules)
    module_px = qr_module_px(n)

    qr_x, qr_y, _ = qr_box(card["layout"], card["bezel"])
    origin_x = qr_x + QR_MARGIN_MODULES * module_px
    origin_y = qr_y + QR_MARGIN_MODULES * module_px

    # One path for all modules, so viewers don't show seams between them
    path = cc.c.beginPath()
    for row, cells in enumerate(modules):
        col = 0
        while col < n:
            if not cells[col]:
                col += 1
                continue
            run_end = col
            while run_end < n and cells[run_end]:
                run_end += 1
            path.rect(*cc.rect(
                origin_x + col * module_px,
                origin_y + row * module_px,
                origin_x + run_end * module_px,
                origin_y + (row + 1) * module_px,
            ))
            col = run_end

    cc.c.setFillColorCMYK(*K_BLACK)
    cc.c.drawPath(path, stroke=0, fill=1)


def draw_crop_marks(c: Canvas, trim: Box, bleed: float) -> None:
    mark_start = mm_to_pt(MARK_OFFSET_MM)

    # White knockouts under the part of each mark that sits on the bleed
    half = mm_to_pt(KNOCKOUT_MM) / 2
    c.setFillColorCMYK(*NO_INK)
    for x0, y0, x1, y1 in crop_mark_segments(trim, mark_start, bleed):
        if x0 == x1:
            c.rect(x0 - half, min(y0, y1), 2 * half, abs(y1 - y0), stroke=0, fill=1)
        else:
            c.rect(min(x0, x1), y0 - half, abs(x1 - x0), 2 * half, stroke=0, fill=1)

    c.setStrokeColorCMYK(*K_BLACK)
    c.setLineWidth(MARK_WIDTH_PT)
    c.setLineCap(0)
    for x0, y0, x1, y1 in crop_mark_segments(trim, mark_start, mark_start + mm_to_pt(MARK_LEN_MM)):
        c.line(x0, y0, x1, y1)


def add_output_intent(pdf_path: str, icc_path: str) -> None:
    with pikepdf.open(pdf_path, allow_overwriting_input=True) as pdf:
        with open(icc_path, "rb") as f:
            icc = pikepdf.Stream(pdf, f.read())
        icc.N = 4
        profile_name = ImageCms.getProfileDescription(icc_path).strip()
        pdf.Root.OutputIntents = Array([
            Dictionary(
                Type=Name.OutputIntent,
                S=Name.GTS_PDFX,
                OutputConditionIdentifier=String(profile_name),
                Info=String(profile_name),
                DestOutputProfile=icc,
            )
        ])
        pdf.save(pdf_path)

# =============================
# MAIN
# =============================

def generate_card_with_bleed(
    track_id: str,
    output_path: str,
    color: str | tuple,
    add_picture: bool = False,
    icc_path: str = DEFAULT_ICC,
    artwork_dir: str = ARTWORK_DIR,
) -> None:
    """Raises ArtworkNeeded (see prepare_card), in which case no card is made."""
    assert MARK_OFFSET_MM < BLEED_MM, "crop marks must start inside the bleed"
    assert MARK_OFFSET_MM + MARK_LEN_MM <= MARGIN_MM, "crop marks don't fit on the page"

    card = prepare_card(track_id, add_picture, artwork_dir)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    cmyk = CmykConverter(icc_path)

    pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, PDF_FONT_PATH))

    margin = mm_to_pt(MARGIN_MM)
    bleed = mm_to_pt(BLEED_MM)
    trim_w, trim_h = mm_to_pt(CARD_WIDTH_MM), mm_to_pt(CARD_HEIGHT_MM)
    trim: Box = (margin, margin, margin + trim_w, margin + trim_h)
    bleed_box: Box = (trim[0] - bleed, trim[1] - bleed, trim[2] + bleed, trim[3] + bleed)

    c = Canvas(
        output_path,
        pagesize=(trim_w + 2 * margin, trim_h + 2 * margin),
        initialFontName=PDF_FONT_NAME,  # otherwise an unembedded Helvetica is referenced
    )
    c.setTitle(f"Card {track_id}")
    c.setTrimBox(trim)
    c.setBleedBox(bleed_box)

    cc = CardCanvas(c, card, trim)

    # Bleed and bezel: the outer edge color over the whole bleed box
    c.setFillColorCMYK(*cmyk.color(color))
    c.rect(bleed_box[0], bleed_box[1], bleed_box[2] - bleed_box[0], bleed_box[3] - bleed_box[1], stroke=0, fill=1)

    # White inside the bezel
    bezel = card["bezel"]
    c.setFillColorCMYK(*NO_INK)
    c.rect(*cc.rect(bezel, bezel, card["width"] - bezel, card["height"] - bezel), stroke=0, fill=1)

    draw_artwork(cc, card, cmyk)
    draw_text(cc, card)
    draw_qr(cc, card)
    draw_crop_marks(c, trim, bleed)

    c.showPage()
    c.save()

    add_output_intent(output_path, icc_path)
    print(f"Card saved to {output_path}")

# =============================
# CLI
# =============================

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate a print-ready CMYK PDF card with bleed and crop marks."
    )
    parser.add_argument("track_id")
    parser.add_argument("output", nargs="?")
    parser.add_argument("--add-picture", action="store_true")
    parser.add_argument("--color", default="red", type=str)
    parser.add_argument(
        "--icc",
        default=DEFAULT_ICC,
        help=f"CMYK ICC profile for the conversion (default: {DEFAULT_ICC})",
    )
    parser.add_argument("--artwork-dir", default=ARTWORK_DIR, help=f"hand-made square artwork (default: {ARTWORK_DIR})")

    args = parser.parse_args()

    output = args.output or os.path.join(OUTPUT_DIR, f"{args.track_id}.pdf")

    try:
        generate_card_with_bleed(
            args.track_id,
            output,
            color=parse_color(args.color),
            add_picture=args.add_picture,
            icc_path=args.icc,
            artwork_dir=args.artwork_dir,
        )
    except ArtworkNeeded as e:
        print(f"\n=== No card for {args.track_id}: {e} ===")
