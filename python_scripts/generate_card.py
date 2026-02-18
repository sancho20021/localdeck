#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import json
import os
import sys
from typing import List, Tuple, TypedDict

from PIL import Image, ImageDraw, ImageFont
from PIL.Image import Image as PILImage
from PIL.ImageDraw import ImageDraw as PILDraw
from PIL.ImageFont import FreeTypeFont

TEXT_CENTERING_BIAS_RATIO = 0.2

FONT_SIZE = 37

# =============================
# CONFIG
# =============================

CARD_WIDTH_MM: int = 55
CARD_HEIGHT_MM: int = 95
QR_SIZE_MM: int = 20
DPI: int = 300

FONT_PATH: str = "/home/sancho20021/.local/share/fonts/Montserrat/montserrat.semibold.otf"
OUTPUT_DIR: str = "./cards"

# card width / card_height
TOP_EMPTY_RATIO: float = CARD_WIDTH_MM / CARD_HEIGHT_MM
MARGIN_RATIO: float = 0.1
QR_MARGIN_RATIO: float = 0.5
GRAPHIC_TEXT_GAP_RATIO: float = 0.6
LINE_SPACING: int = 8


# =============================
# TYPE DEFINITIONS
# =============================

class TextLine(TypedDict):
    text: str
    font: FreeTypeFont
    x: int
    y: int


class Layout(TypedDict):
    qr_position: Tuple[int, int]
    text_lines: List[TextLine]


# =============================
# HELPERS
# =============================

def mm_to_px(mm: int) -> int:
    return int(mm * DPI / 25.4)

def qr_size_px() -> int:
    return mm_to_px(QR_SIZE_MM)

def get_metadata(track_id: str) -> Tuple[str, str]:
    result = subprocess.run(
        ["localdeck", "meta", "get", track_id, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data: dict = json.loads(result.stdout)
    return data["artist"], data["title"]


def generate_qr(url: str, output_path: str) -> None:
    subprocess.run(
        ["qrencode", "-o", output_path, "-s", "8", "-m", "2", url],
        check=True
    )


def wrap_text(
    draw: PILDraw,
    text: str,
    font: FreeTypeFont,
    max_width: int
) -> List[str]:

    words: List[str] = text.split()
    lines: List[str] = []
    current: str = ""

    for word in words:
        test = current + (" " if current else "") + word
        bbox = draw.textbbox((0, 0), test, font=font)
        w: float = bbox[2] - bbox[0]

        if w <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines


# =============================
# LAYOUT ENGINE (ALL MATH)
# =============================

def build_layout(
    width: int,
    height: int,
    artist: str,
    title: str,
    draw: PILDraw,
) -> Layout:

    margin: int = int(width * MARGIN_RATIO)
    top_empty_height: int = int(height * TOP_EMPTY_RATIO)
    max_text_width: int = width - 2 * margin

    # ----------------------------
    # 1. Prepare fonts
    # ----------------------------
    font_text: FreeTypeFont = ImageFont.truetype(FONT_PATH, FONT_SIZE)

    raw_lines: List[Tuple[str, FreeTypeFont]] = []

    text: str = f"{artist} — {title}"
    wrapped: List[str] = wrap_text(draw, text, font_text, max_text_width)
    for line in wrapped:
        raw_lines.append((line, font_text))
    raw_lines.append(("", font_text))

    # ----------------------------
    # 2. Measure text height
    # ----------------------------
    measured: List[Tuple[str, FreeTypeFont, int]] = []
    total_text_height: int = 0

    for line, font in raw_lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        h: int = int(bbox[3] - bbox[1])
        total_text_height += h + LINE_SPACING
        measured.append((line, font, h))

    # ----------------------------
    # 3. Position text block (centered with slight bias upward)
    # ----------------------------
    graphic_text_gap: int = int(margin * GRAPHIC_TEXT_GAP_RATIO)

    # Position QR at bottom first
    qr_y: int = height - margin - qr_size_px()

    # Vertical space available for text block
    text_space_top: int = top_empty_height + graphic_text_gap
    text_space_bottom: int = qr_y - graphic_text_gap
    available_space: int = text_space_bottom - text_space_top

    # Center text block in available space
    # Small tweak: bias 20% toward top
    bias_ratio: float = TEXT_CENTERING_BIAS_RATIO
    text_start_y: int = text_space_top + int((available_space - total_text_height) * (0.5 - bias_ratio))

    y: int = text_start_y

    text_lines: List[TextLine] = []
    for line, font, h in measured:
        bbox = draw.textbbox((0, 0), line, font=font)
        w: int = int(bbox[2] - bbox[0])
        x: int = (width - w) // 2

        text_lines.append({
            "text": line,
            "font": font,
            "x": x,
            "y": y,
        })

        y += h + LINE_SPACING

    # ----------------------------
    # 4. Compute remaining space
    # ----------------------------

    # ----------------------------
    # 5. Position QR at bottom
    # ----------------------------
    qr_x: int = (width - qr_size_px()) // 2
    qr_y: int = height - margin - qr_size_px()

    return {
        "qr_position": (qr_x, qr_y),
        "text_lines": text_lines,
    }


# =============================
# PURE RENDERER
# =============================

def render_card(
    width: int,
    height: int,
    layout: Layout,
    qr_img: PILImage,
    output_path: str,
) -> None:

    img: PILImage = Image.new("RGB", (width, height), "white")
    draw: PILDraw = ImageDraw.Draw(img)

    # ----------------------------
    # DRAW RED GRAPHIC AREA
    # ----------------------------
    draw.rectangle(
        [
            (0, 0),  # top-left
            (width, width)  # bottom-right
        ],
        fill="red"  # filled color
    )

    # ----------------------------
    # Draw text
    # ----------------------------
    for line in layout["text_lines"]:
        draw.text(
            (line["x"], line["y"]),
            line["text"],
            fill="black",
            font=line["font"],
        )

    # ----------------------------
    # Draw QR
    # ----------------------------
    qr_resized: PILImage = qr_img.resize((qr_size_px(), qr_size_px()), Image.LANCZOS)

    qr_x, qr_y = layout["qr_position"]
    img.paste(qr_resized, (qr_x, qr_y))

    img.save(output_path, dpi=(DPI, DPI))


# =============================
# MAIN
# =============================

def generate_card(track_id: str, output_path: str) -> None:

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    width: int = mm_to_px(CARD_WIDTH_MM)
    height: int = mm_to_px(CARD_HEIGHT_MM)

    artist, title = get_metadata(track_id)

    play_url: str = f"http://main-deck:8080/play?h={track_id}"

    qr_tmp: str = "temp_qr.png"
    generate_qr(play_url, qr_tmp)

    qr_img: PILImage = Image.open(qr_tmp).convert("RGB")

    dummy_img: PILImage = Image.new("RGB", (width, height))
    dummy_draw: PILDraw = ImageDraw.Draw(dummy_img)

    layout: Layout = build_layout(
        width,
        height,
        artist,
        title,
        dummy_draw,
    )

    render_card(width, height, layout, qr_img, output_path)

    os.remove(qr_tmp)
    print(f"Card saved to {output_path}")


# =============================
# CLI
# =============================

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python generate_card.py <track_id> [output_file]")
        sys.exit(1)

    track_id: str = sys.argv[1]

    if len(sys.argv) >= 3:
        output: str = sys.argv[2]
    else:
        output = os.path.join(OUTPUT_DIR, f"{track_id}.png")

    generate_card(track_id, output)
