#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import json
import os
import io
import argparse
import urllib.request
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

SQUARE_TOLERANCE = 0.02  # if artwork is not square-ish, don't apply it
CARD_WIDTH_MM: int = 55
CARD_HEIGHT_MM: int = 90
QR_SIZE_MM: int = 19
DPI: int = 300
BEZEL_MM: int = 1

FONT_PATH: str = "/home/sancho20021/.local/share/fonts/Montserrat/montserrat.semibold.otf"
OUTPUT_DIR: str = "./cards"

TOP_EMPTY_RATIO: float = CARD_WIDTH_MM / CARD_HEIGHT_MM
MARGIN_RATIO: float = 0.09
GRAPHIC_TEXT_GAP_RATIO: float = 0.7
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

def get_metadata(track_id: str) -> Tuple[str, str, str]:
    result = subprocess.run(
        ["localdeck", "meta", "get", track_id, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data: dict = json.loads(result.stdout)
    return data["artist"], data["title"], data["artwork"]


def generate_qr(url: str, output_path: str) -> None:
    subprocess.run(
        ["qrencode", "-o", output_path, "-s", "8", "-m", "2", url],
        check=True
    )


def fetch_image(url: str) -> PILImage:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://google.com",
        },
    )

    with urllib.request.urlopen(req) as response:
        data = response.read()

    return Image.open(io.BytesIO(data)).convert("RGB")


def is_squareish(w: int, h: int, tolerance: float) -> bool:
    return abs(w - h) / max(w, h) <= tolerance


def crop_to_square(img: PILImage) -> PILImage:
    w, h = img.size
    # print(f"image size: {w}x{h}")
    if w == h:
        return img

    if w > h:
        delta = w - h
        left = delta // 2
        right = left + h
        return img.crop((left, 0, right, h))
    else:
        delta = h - w
        top = delta // 2
        bottom = top + w
        # print(f"cropping image left=0, upper={top}, right={w}, lower={bottom}")
        return img.crop((0, top, w, bottom))


def wrap_text(draw: PILDraw, text: str, font: FreeTypeFont, max_width: int) -> List[str]:
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
# LAYOUT ENGINE
# =============================

def build_layout(width: int, height: int, artist: str, title: str, draw: PILDraw) -> Layout:

    margin: int = int(width * MARGIN_RATIO)
    top_empty_height: int = int(height * TOP_EMPTY_RATIO)
    max_text_width: int = width - 2 * margin

    font_text: FreeTypeFont = ImageFont.truetype(FONT_PATH, FONT_SIZE)

    raw_lines: List[Tuple[str, FreeTypeFont]] = []

    text: str = f"{artist} — {title}"
    wrapped: List[str] = wrap_text(draw, text, font_text, max_text_width)
    for line in wrapped:
        raw_lines.append((line, font_text))
    raw_lines.append(("", font_text))

    measured: List[Tuple[str, FreeTypeFont, int]] = []
    total_text_height: int = 0

    for line, font in raw_lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        h: int = int(bbox[3] - bbox[1])
        total_text_height += h + LINE_SPACING
        measured.append((line, font, h))

    graphic_text_gap: int = int(margin * GRAPHIC_TEXT_GAP_RATIO)

    qr_y: int = height - margin - qr_size_px()

    text_space_top: int = top_empty_height + graphic_text_gap
    text_space_bottom: int = qr_y - graphic_text_gap
    available_space: int = text_space_bottom - text_space_top

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

    qr_x: int = (width - qr_size_px()) // 2

    return {
        "qr_position": (qr_x, qr_y),
        "text_lines": text_lines,
    }

# debugging to validate pixels
def scan_pixels(
    img: PILImage,
    start_x: int,
    start_y: int,
    direction: str,
    length: int,
) -> None:
    """
    Scan pixels from (start_x, start_y) in given direction.

    direction ∈ {"right", "left", "down", "up"}

    Prints compressed color segments.
    """

    dx, dy = {
        "right": (1, 0),
        "left": (-1, 0),
        "down": (0, 1),
        "up": (0, -1),
    }[direction]

    width, height = img.size

    def color_name(px):
        if px == (255, 0, 0):
            return "R"  # red
        elif px == (255, 255, 255):
            return "W"  # white
        elif px == (0, 0, 0):
            return "B"  # black
        else:
            return "?"

    result = []

    x, y = start_x, start_y

    for i in range(length):
        if not (0 <= x < width and 0 <= y < height):
            result.append("OOB")
        else:
            px = img.getpixel((x, y))
            result.append(color_name(px))

        x += dx
        y += dy

    # compress output
    compressed = []
    prev = None
    count = 0

    for c in result:
        if c != prev:
            if prev is not None:
                compressed.append(f"{prev}x{count}")
            prev = c
            count = 1
        else:
            count += 1

    if prev is not None:
        compressed.append(f"{prev}x{count}")

    print(
        f"[SCAN] start=({start_x},{start_y}) dir={direction} len={length} -> "
        + " | ".join(compressed)
    )

# =============================
# RENDERER
# =============================

def render_card(
    width: int,
    height: int,
    layout: Layout,
    qr_img: PILImage,
    output_path: str,
    artwork_img: PILImage | None = None,
) -> None:

    img: PILImage = Image.new("RGB", (width, height), "red")
    draw: PILDraw = ImageDraw.Draw(img)

    bezel = mm_to_px(BEZEL_MM)

    draw.rectangle(
        [(bezel, bezel), (width - bezel - 1, height - bezel - 1)],
        fill="white"
    )

    payload_width = width - 2 * bezel

    # print(f"[DEBUG] card size: {width}x{height}")
    # print(f"[DEBUG] bezel: {bezel}")
    # print(f"[DEBUG] payload_width: {payload_width}")
    # print(f"[DEBUG] expected graphic square: x[{bezel}, {bezel + payload_width})")
    assert payload_width > 0
    assert bezel > 0
    assert bezel + payload_width <= width

    if artwork_img is not None:
        art_resized = artwork_img.resize(
            (payload_width, payload_width),
        )

        rw, rh = art_resized.size

        # print(f"[DEBUG] resized image size: {rw}x{rh}")
        # print(f"[DEBUG] paste position: ({bezel}, {bezel})")
        # print(f"[DEBUG] paste bottom-right: ({bezel + rw}, {bezel + rh})")
        # print(f"[DEBUG] expected bottom-right: ({bezel + payload_width}, {bezel + payload_width})")

        assert rw == payload_width, f"width mismatch: {rw} != {payload_width}"
        assert rh == payload_width, f"height mismatch: {rh} != {payload_width}"
        img.paste(art_resized, (bezel, bezel))

    else:
        draw.rectangle(
            [(bezel, bezel), (bezel + payload_width - 1, bezel + payload_width - 1)],
            fill="black"
        )
    # # Check right edge of card
    # scan_pixels(
    #     img,
    #     start_x=width - 1,
    #     start_y=bezel + 1,
    #     direction="left",
    #     length=bezel + 2,
    # )

    for line in layout["text_lines"]:
        draw.text(
            (line["x"] + bezel, line["y"] + bezel),
            line["text"],
            fill="black",
            font=line["font"],
        )

    qr_resized: PILImage = qr_img.resize(
            (qr_size_px(), qr_size_px()),
        # Image.LANCZOS
    )
    qr_x, qr_y = layout["qr_position"]
    img.paste(qr_resized, (qr_x + bezel, qr_y + bezel))

    img.save(output_path, dpi=(DPI, DPI))

# =============================
# MAIN
# =============================

def generate_card(
    track_id: str,
    output_path: str,
    add_picture: bool = False,
) -> bool:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    width: int = mm_to_px(CARD_WIDTH_MM)
    height: int = mm_to_px(CARD_HEIGHT_MM)
    bezel: int = mm_to_px(BEZEL_MM)

    payload_width = width - 2 * bezel
    payload_height = height - 2 * bezel

    artist, title, artwork_url = get_metadata(track_id)

    play_url: str = f"http://main-deck:8080/play?h={track_id}"

    qr_tmp: str = "temp_qr.png"
    generate_qr(play_url, qr_tmp)

    qr_img: PILImage = Image.open(qr_tmp).convert("RGB")

    dummy_img: PILImage = Image.new("RGB", (payload_width, payload_height))
    dummy_draw: PILDraw = ImageDraw.Draw(dummy_img)

    layout: Layout = build_layout(
        payload_width,
        payload_height,
        artist,
        title,
        dummy_draw,
    )

    artwork_img = None
    used_square_artwork = False

    if add_picture:
        img = fetch_image(artwork_url)
        w, h = img.size

        if is_squareish(w, h, SQUARE_TOLERANCE):
            artwork_img = crop_to_square(img)
            # print(f"cropped image size: {artwork_img.size}")
            used_square_artwork = True
        else:
            print(f"image size not square: {w}x{h}")
            used_square_artwork = False

    render_card(width, height, layout, qr_img, output_path, artwork_img)

    os.remove(qr_tmp)
    print(f"Card saved to {output_path}")

    return used_square_artwork

# =============================
# CLI
# =============================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("track_id")
    parser.add_argument("output", nargs="?")
    parser.add_argument("--add-picture", action="store_true")

    args = parser.parse_args()

    track_id = args.track_id
    output = args.output or os.path.join(OUTPUT_DIR, f"{track_id}.png")

    picture_applied = generate_card(
        track_id,
        output,
        add_picture=args.add_picture,
    )

    if args.add_picture and not picture_applied:
        print(f"\n=== Failed to apply picture to {track_id}  ===")
