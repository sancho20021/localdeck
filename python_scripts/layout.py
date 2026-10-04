#!/usr/bin/env python3

from __future__ import annotations

import argparse
import math
import os
from typing import List, Tuple

from PIL import Image, ImageCms, ImageDraw

from generate_card import parse_color

# ----------------------------
# CONFIG (defaults)
# ----------------------------
DPI = 300
MM_TO_INCH = 25.4

PAGE_SIZES_MM = {
    "A3": (297, 420),
    "A4": (210, 297),
    "A5": (148, 210),
}
DEFAULT_PAGE_SIZE = "A3"

MARGIN_MM = 10
GAP_MM = 0
BLEED_MM = 3
MARK_LEN_MM = 5

BORDER_COLOR = "144,2,168"
PAGE_COLOR = "white"
MARK_COLOR = "black"


def mm_to_px(mm: float, dpi: int) -> int:
    return int(mm / MM_TO_INCH * dpi)


def load_icc_bytes(icc_path: str | None) -> bytes:
    """ICC profile to tag the pages with, so the printer doesn't have to guess."""
    if icc_path:
        return ImageCms.getOpenProfile(icc_path).tobytes()
    return ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()


def parse_page_size(value: str) -> Tuple[float, float]:
    """"A3" / "a4" / a "WxH" pair in millimetres, e.g. "320x450"."""
    key = value.strip().upper()
    if key in PAGE_SIZES_MM:
        return PAGE_SIZES_MM[key]

    if "X" in key:
        try:
            w, h = (float(part) for part in key.split("X", 1))
            if w > 0 and h > 0:
                return w, h
        except ValueError:
            pass

    raise argparse.ArgumentTypeError(
        f"unknown page size {value!r}: use one of "
        f"{', '.join(PAGE_SIZES_MM)} or WxH in mm (e.g. 320x450)"
    )


def find_cards(input_dir: str) -> List[str]:
    return sorted(
        os.path.join(input_dir, f)
        for f in os.listdir(input_dir)
        if f.lower().endswith(".png")
    )


def build_pages(
    card_paths: List[str],
    page_width_mm: float,
    page_height_mm: float,
    dpi: int,
    margin_mm: float,
    gap_mm: float,
    bleed_mm: float,
    mark_len_mm: float,
    border_color,
    page_color,
    mark_color,
) -> List[Image.Image]:

    page_width_px = mm_to_px(page_width_mm, dpi)
    page_height_px = mm_to_px(page_height_mm, dpi)
    margin_px = mm_to_px(margin_mm, dpi)
    gap_px = mm_to_px(gap_mm, dpi)
    bleed_px = mm_to_px(bleed_mm, dpi)
    mark_len_px = mm_to_px(mark_len_mm, dpi)

    card_width, card_height = Image.open(card_paths[0]).size

    # ----------------------------
    # Grid math
    # ----------------------------
    usable_width = page_width_px - 2 * margin_px
    usable_height = page_height_px - 2 * margin_px

    cols = usable_width // (card_width + gap_px)
    rows = usable_height // (card_height + gap_px)

    if cols < 1 or rows < 1:
        raise RuntimeError("Card size too big")

    cards_per_page = cols * rows
    print(f"Grid: {cols} x {rows} -> {cards_per_page} per page")

    # ----------------------------
    # Pages
    # ----------------------------
    pages: List[Image.Image] = []

    for page_idx in range(math.ceil(len(card_paths) / cards_per_page)):

        page_img = Image.new("RGB", (page_width_px, page_height_px), page_color)
        draw = ImageDraw.Draw(page_img)

        page_cards = card_paths[page_idx * cards_per_page:(page_idx + 1) * cards_per_page]

        total_w = cols * card_width + (cols - 1) * gap_px
        total_h = rows * card_height + (rows - 1) * gap_px

        left = margin_px
        top = margin_px
        right = left + total_w
        bottom = top + total_h

        # ----------------------------
        # BLEED
        # ----------------------------
        draw.rectangle(
            [
                left - bleed_px,
                top - bleed_px,
                right + bleed_px,
                bottom + bleed_px
            ],
            fill=border_color,
        )

        # ----------------------------
        # Crop marks
        # ----------------------------
        for r in range(rows + 1):
            y = margin_px + r * (card_height + gap_px)

            draw.line([(left - mark_len_px, y), (left, y)], fill=mark_color, width=1)
            draw.line([(right, y), (right + mark_len_px, y)], fill=mark_color, width=1)

        for c in range(cols + 1):
            x = margin_px + c * (card_width + gap_px)

            draw.line([(x, top - mark_len_px), (x, top)], fill=mark_color, width=1)
            draw.line([(x, bottom), (x, bottom + mark_len_px)], fill=mark_color, width=1)

        # ----------------------------
        # Paste cards (alpha-safe)
        # ----------------------------
        for idx, card_path in enumerate(page_cards):

            img = Image.open(card_path).convert("RGBA")

            col = idx % cols
            row = idx // cols

            x = margin_px + col * (card_width + gap_px)
            y = margin_px + row * (card_height + gap_px)

            # Proper alpha flattening
            bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
            flat = Image.alpha_composite(bg, img).convert("RGB")

            page_img.paste(flat, (x, y))

        pages.append(page_img)

    return pages


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Lay out card PNGs onto printable pages with bleed and crop marks."
    )
    parser.add_argument("input_dir", help="directory holding the card PNGs")
    parser.add_argument("output_dir", help="directory to write the page PNGs into")
    parser.add_argument(
        "--border-color",
        default=BORDER_COLOR,
        help=f"bleed/border color: hex, R,G,B or a name (default: {BORDER_COLOR})",
    )
    parser.add_argument(
        "--page-color",
        default=PAGE_COLOR,
        help=f"page background color (default: {PAGE_COLOR})",
    )
    parser.add_argument(
        "--mark-color",
        default=MARK_COLOR,
        help=f"crop mark color (default: {MARK_COLOR})",
    )
    parser.add_argument(
        "--page-size",
        default=DEFAULT_PAGE_SIZE,
        help=f"{', '.join(PAGE_SIZES_MM)} or WxH in mm (default: {DEFAULT_PAGE_SIZE})",
    )
    parser.add_argument("--dpi", type=int, default=DPI, help=f"default: {DPI}")
    parser.add_argument("--margin-mm", type=float, default=MARGIN_MM, help=f"default: {MARGIN_MM}")
    parser.add_argument("--gap-mm", type=float, default=GAP_MM, help=f"default: {GAP_MM}")
    parser.add_argument("--bleed-mm", type=float, default=BLEED_MM, help=f"default: {BLEED_MM}")
    parser.add_argument(
        "--mark-len-mm", type=float, default=MARK_LEN_MM, help=f"default: {MARK_LEN_MM}"
    )
    parser.add_argument(
        "--icc",
        help="ICC profile to embed in the pages (default: built-in sRGB)",
    )
    parser.add_argument(
        "--no-icc",
        action="store_true",
        help="leave the pages untagged (the old behaviour)",
    )
    parser.add_argument(
        "--prefix",
        help="output filename prefix (default: derived from the page size, e.g. a3_cards_page)",
    )

    args = parser.parse_args()

    try:
        page_width_mm, page_height_mm = parse_page_size(args.page_size)
    except argparse.ArgumentTypeError as e:
        parser.error(str(e))
    prefix = args.prefix or f"{args.page_size.strip().lower().replace('x', '_')}_cards_page"

    card_paths = find_cards(args.input_dir)
    if not card_paths:
        raise RuntimeError(f"No PNG files found in {args.input_dir}")

    os.makedirs(args.output_dir, exist_ok=True)

    pages = build_pages(
        card_paths,
        page_width_mm=page_width_mm,
        page_height_mm=page_height_mm,
        dpi=args.dpi,
        margin_mm=args.margin_mm,
        gap_mm=args.gap_mm,
        bleed_mm=args.bleed_mm,
        mark_len_mm=args.mark_len_mm,
        border_color=parse_color(args.border_color),
        page_color=parse_color(args.page_color),
        mark_color=parse_color(args.mark_color),
    )

    # ----------------------------
    # Save PNG pages
    # ----------------------------
    save_kwargs = {}
    if not args.no_icc:
        save_kwargs["icc_profile"] = load_icc_bytes(args.icc)

    for i, page in enumerate(pages):
        out_path = os.path.join(args.output_dir, f"{prefix}_{i+1:03d}.png")
        page.save(out_path, "PNG", dpi=(args.dpi, args.dpi), optimize=False, **save_kwargs)
        print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
