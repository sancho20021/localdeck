#!/usr/bin/env python3

from __future__ import annotations

import subprocess
import json
import os
import io
import re
import argparse
import urllib.request
from typing import List, Tuple, TypedDict

from PIL import Image, ImageDraw, ImageFont
from PIL.Image import Image as PILImage
from PIL.ImageDraw import ImageDraw as PILDraw
from PIL.ImageFont import FreeTypeFont

TEXT_CENTERING_BIAS_RATIO = 0.1
FONT_SIZE = 37

# =============================
# CONFIG
# =============================

SQUARE_TOLERANCE = 0.02  # if artwork is not square-ish, don't apply it
CARD_WIDTH_MM: int = 55
CARD_HEIGHT_MM: int = 90
QR_SIZE_MM: int = 17
QR_MARGIN_MODULES: int = 2  # quiet zone around the QR code, included in QR_SIZE_MM
DPI: int = 300
BEZEL_MM: int = 1

FONT_PATH: str = "/home/sancho20021/.local/share/fonts/Montserrat/montserrat.semibold.otf"
OUTPUT_DIR: str = "./cards"
# Hand-made square artwork, <track_id>.<ext>; takes precedence over the metadata artwork
ARTWORK_DIR: str = "./artwork"
ARTWORK_EXTENSIONS: Tuple[str, ...] = (".png", ".jpg", ".jpeg", ".webp")

TOP_EMPTY_RATIO: float = CARD_WIDTH_MM / CARD_HEIGHT_MM
MARGIN_RATIO: float = 0.08
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


class CardData(TypedDict):
    """Everything needed to draw a card, in pixels at DPI."""
    width: int
    height: int
    bezel: int
    layout: Layout  # positions relative to the area inside the bezel
    play_url: str
    artwork: PILImage | None  # square artwork, or None for the black square


class ArtworkNeeded(Exception):
    """No usable artwork, a human has to provide it; no card should be made."""

# =============================
# HELPERS
# =============================

def mm_to_px(mm: int) -> int:
    return int(mm * DPI / 25.4)

def qr_size_px() -> int:
    return mm_to_px(QR_SIZE_MM)

def get_metadata(track_id: str) -> Tuple[str, str, str | None]:
    result = subprocess.run(
        ["localdeck", "meta", "get", track_id, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data: dict = json.loads(result.stdout)
    return data["artist"], data["title"], data.get("artwork")

def get_qr_string(track_id: str) -> str:
    """Gets the QR string for a given track_id."""
    try:
        result = subprocess.run(
            ["localdeck", "url", track_id],
            capture_output=True,
            text=True,
            check=True
        )
        # .strip() removes the newline character at the end of the output
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        # Handle cases where the track_id doesn't exist or command fails
        print(f"Error fetching URL for {track_id}: {e.stderr}")
        return ""

# Usage:
# play_url = get_play_url(track_id)


def generate_qr(url: str, output_path: str) -> None:
    subprocess.run(
        ["qrencode", "-o", output_path, "-s", "8", "-m", str(QR_MARGIN_MODULES), url],
        check=True
    )


def qr_modules(url: str) -> List[List[bool]]:
    """QR code as a grid of modules (True = dark), without the quiet zone."""
    png = subprocess.run(
        ["qrencode", "-o", "-", "-s", "1", "-m", "0", url],
        capture_output=True,
        check=True,
    ).stdout
    img = Image.open(io.BytesIO(png)).convert("L")
    w, h = img.size
    return [[img.getpixel((x, y)) < 128 for x in range(w)] for y in range(h)]


def qr_box(layout: Layout, bezel: int) -> Tuple[int, int, int]:
    """Where the QR code sits on the card: (x, y, size) in pixels, quiet zone included."""
    qr_x, qr_y = layout["qr_position"]
    return qr_x + bezel, qr_y + bezel, qr_size_px()


def qr_module_px(module_count: int) -> float:
    """Size of one module when the code plus its quiet zone fills the QR box."""
    return qr_size_px() / (module_count + 2 * QR_MARGIN_MODULES)


def fetch_image(url: str) -> Image.Image:
    # 1. Check if 'url' is actually a local file path
    if os.path.exists(url):
        return Image.open(url).convert("RGB")

    # 2. Otherwise, treat it as a network URL
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
            "Referer": "https://www.google.com/",
        },
    )

    with urllib.request.urlopen(req) as response:
        data = response.read()

    return Image.open(io.BytesIO(data)).convert("RGB")


def find_artwork_override(track_id: str, artwork_dir: str = ARTWORK_DIR) -> str | None:
    """The hand-made artwork for a track (newest one if there are several), if any."""
    if not os.path.isdir(artwork_dir):
        return None
    candidates = [
        os.path.join(artwork_dir, f)
        for f in os.listdir(artwork_dir)
        if os.path.splitext(f)[0] == track_id and os.path.splitext(f)[1].lower() in ARTWORK_EXTENSIONS
    ]
    return max(candidates, key=os.path.getmtime, default=None)


def save_original_artwork(img: PILImage, track_id: str, artwork_dir: str = ARTWORK_DIR) -> str:
    """Saves downloaded non-square artwork next to the overrides, ready to be cropped."""
    os.makedirs(artwork_dir, exist_ok=True)
    path = os.path.join(artwork_dir, f"{track_id}_original.png")
    img.save(path)
    return path


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
    color: str | tuple,
    artwork_img: PILImage | None = None,
) -> None:
    img: PILImage = Image.new("RGB", (width, height), color)
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

    qr_x, qr_y, qr_size = qr_box(layout, bezel)
    qr_resized: PILImage = qr_img.resize(
            (qr_size, qr_size),
        # Image.LANCZOS
    )
    img.paste(qr_resized, (qr_x, qr_y))

    img.save(output_path, dpi=(DPI, DPI))

# =============================
# MAIN
# =============================

def load_artwork(
    track_id: str,
    artwork_url: str | None,
    artwork_dir: str = ARTWORK_DIR,
) -> PILImage:
    """
    Square artwork for the card: the hand-made one from artwork_dir if present,
    otherwise the metadata one. Raises ArtworkNeeded, saying what to do, when
    neither is usable; downloaded non-square artwork is saved for cropping.
    """
    target = f"{artwork_dir}/{track_id}.<png|jpg>"

    override = find_artwork_override(track_id, artwork_dir)
    if override is not None:
        img = Image.open(override).convert("RGB")
        w, h = img.size
        if not is_squareish(w, h, SQUARE_TOLERANCE):
            raise ArtworkNeeded(f"{override} is {w}x{h}, not square; replace it with a square version")
        return crop_to_square(img)

    if not artwork_url:
        raise ArtworkNeeded(
            f"no artwork in metadata; save a square image as {target} (a plain white square for a white card)"
        )

    try:
        img = fetch_image(artwork_url)
    except Exception as e:
        raise ArtworkNeeded(
            f"artwork download failed ({e}); run again to retry, or save a square image as {target}"
        ) from e

    w, h = img.size
    if not is_squareish(w, h, SQUARE_TOLERANCE):
        original = save_original_artwork(img, track_id, artwork_dir)
        raise ArtworkNeeded(f"artwork is {w}x{h}, not square; crop {original} and save as {target}")
    return crop_to_square(img)


def prepare_card(track_id: str, add_picture: bool = False, artwork_dir: str = ARTWORK_DIR) -> CardData:
    """With add_picture, raises ArtworkNeeded when there is no usable artwork (see load_artwork)."""
    width: int = mm_to_px(CARD_WIDTH_MM)
    height: int = mm_to_px(CARD_HEIGHT_MM)
    bezel: int = mm_to_px(BEZEL_MM)

    payload_width = width - 2 * bezel
    payload_height = height - 2 * bezel

    artist, title, artwork_url = get_metadata(track_id)

    play_url: str = get_qr_string(track_id)

    dummy_img: PILImage = Image.new("RGB", (payload_width, payload_height))
    dummy_draw: PILDraw = ImageDraw.Draw(dummy_img)

    layout: Layout = build_layout(
        payload_width,
        payload_height,
        artist,
        title,
        dummy_draw,
    )

    artwork_img = load_artwork(track_id, artwork_url, artwork_dir) if add_picture else None

    return {
        "width": width,
        "height": height,
        "bezel": bezel,
        "layout": layout,
        "play_url": play_url,
        "artwork": artwork_img,
    }


def generate_card(
    track_id: str,
    output_path: str,
    color: str | tuple,
    add_picture: bool = False,
    artwork_dir: str = ARTWORK_DIR,
) -> None:
    """Raises ArtworkNeeded (see prepare_card), in which case no card is made."""
    card: CardData = prepare_card(track_id, add_picture, artwork_dir)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    qr_tmp: str = "temp_qr.png"
    generate_qr(card["play_url"], qr_tmp)

    qr_img: PILImage = Image.open(qr_tmp).convert("RGB")

    render_card(
        card["width"],
        card["height"],
        card["layout"],
        qr_img,
        output_path,
        color,
        card["artwork"],
    )

    os.remove(qr_tmp)
    print(f"Card saved to {output_path}")

HEX_COLOR_RE = re.compile(r"^#?([0-9a-fA-F]{3}|[0-9a-fA-F]{4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$")


def parse_color(color_input: str):
    """
    Accepts an HTML hex color (with or without "#", e.g. "2c5b02" or "#2c5b02"),
    an "R,G,B" triple, or a named color.
    Falls back to the raw string if parsing fails.
    """
    if not isinstance(color_input, str):
        return color_input

    value = color_input.strip()

    # HTML hex color, with or without the leading "#"
    match = HEX_COLOR_RE.match(value)
    if match:
        return "#" + match.group(1)

    # "R,G,B" (or "R,G,B,A") triple
    if "," in value:
        try:
            return tuple(int(c.strip()) for c in value.split(","))
        except ValueError:
            return value

    # Named color such as "red"
    return value

# =============================
# CLI
# =============================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("track_id")
    parser.add_argument("output", nargs="?")
    parser.add_argument("--add-picture", action="store_true")
    parser.add_argument("--color", default="red",type=str)
    parser.add_argument("--artwork-dir", default=ARTWORK_DIR, help=f"hand-made square artwork (default: {ARTWORK_DIR})")

    args = parser.parse_args()

    track_id = args.track_id
    output = args.output or os.path.join(OUTPUT_DIR, f"{track_id}.png")

    color = parse_color(args.color)

    try:
        generate_card(
            track_id,
            output,
            color=color,
            add_picture=args.add_picture,
            artwork_dir=args.artwork_dir,
        )
    except ArtworkNeeded as e:
        print(f"\n=== No card for {track_id}: {e} ===")
