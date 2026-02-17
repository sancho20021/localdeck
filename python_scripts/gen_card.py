#!/usr/bin/env python3

import subprocess
import json
import os
import sys
from PIL import Image, ImageDraw, ImageFont

# -----------------------------
# CONFIG
# -----------------------------

CARD_WIDTH_MM = 55
CARD_HEIGHT_MM = 85
DPI = 300

FONT_PATH = "/home/sancho20021/.local/share/fonts/Montserrat/montserrat.semibold.otf"  # CHANGE if needed
OUTPUT_DIR = "./cards"

TOP_EMPTY_RATIO = 0.50      # 60% empty at top
MAX_QR_HEIGHT_RATIO = 0.25  # QR max 35% of full height
MARGIN_RATIO = 0.08         # 8% side margins


# -----------------------------
# Helpers
# -----------------------------

def mm_to_px(mm):
    return int(mm * DPI / 25.4)


def get_metadata(track_id):
    result = subprocess.run(
        ["localdeck", "meta", "get", track_id, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    return data["artist"], data["title"]


def generate_qr(url, output_path):
    subprocess.run(
        [
            "qrencode",
            "-o", output_path,
            "-s", "8",
            "-m", "2",
            url
        ],
        check=True,
    )


def wrap_text(draw, text, font, max_width):
    words = text.split()
    lines = []
    current = ""

    for word in words:
        test = current + (" " if current else "") + word
        bbox = draw.textbbox((0, 0), test, font=font)
        w = bbox[2] - bbox[0]

        if w <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines


# -----------------------------
# Main generator
# -----------------------------

def generate_card(track_id, output_path):

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    width = mm_to_px(CARD_WIDTH_MM)
    height = mm_to_px(CARD_HEIGHT_MM)
    margin = int(width * MARGIN_RATIO)

    # Fetch metadata
    artist, title = get_metadata(track_id)
    play_url = f"http://main-deck:8080/play?h={track_id}"

    # Generate QR
    qr_tmp = "temp_qr.png"
    generate_qr(play_url, qr_tmp)
    qr_img = Image.open(qr_tmp).convert("RGB")

    # Create blank card
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    # Load fonts
    try:
        font_artist = ImageFont.truetype(FONT_PATH, 40)
        font_title = ImageFont.truetype(FONT_PATH, 50)
    except:
        print("ERROR: Could not load Montserrat font. Check FONT_PATH.")
        sys.exit(1)

    # -----------------------------
    # Layout zones
    # -----------------------------

    top_empty_height = int(height * TOP_EMPTY_RATIO)
    max_qr_height = int(height * MAX_QR_HEIGHT_RATIO)

    # Resize QR (limit by width AND max height)
    qr_ratio = min(
        (width - 2 * margin) / qr_img.width,
        max_qr_height / qr_img.height
    )

    qr_w = int(qr_img.width * qr_ratio)
    qr_h = int(qr_img.height * qr_ratio)

    qr_img = qr_img.resize((qr_w, qr_h), Image.LANCZOS)

    # -----------------------------
    # Text area between empty zone and QR
    # -----------------------------

    text_area_top = top_empty_height
    text_area_bottom = height - qr_h - margin
    available_text_height = text_area_bottom - text_area_top

    max_text_width = width - 2 * margin

    lines = []

    for text, font in [(artist, font_artist), (title, font_title)]:
        wrapped = wrap_text(draw, text, font, max_text_width)
        for line in wrapped:
            lines.append((line, font))
        lines.append(("", font))  # spacing

    # Measure text block height
    total_height = 0
    measured = []

    for line, font in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        h = bbox[3] - bbox[1]
        total_height += h + 8
        measured.append((line, font, h))

    # If text too big → shrink fonts
    if total_height > available_text_height:
        font_artist = ImageFont.truetype(FONT_PATH, 36)
        font_title = ImageFont.truetype(FONT_PATH, 44)

        lines = []
        for text, font in [(artist, font_artist), (title, font_title)]:
            wrapped = wrap_text(draw, text, font, max_text_width)
            for line in wrapped:
                lines.append((line, font))
            lines.append(("", font))

        total_height = 0
        measured = []

        for line, font in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            h = bbox[3] - bbox[1]
            total_height += h + 8
            measured.append((line, font, h))

    # Center text vertically in available area
    y = text_area_top + (available_text_height - total_height) // 2

    for line, font, h in measured:
        bbox = draw.textbbox((0, 0), line, font=font)
        w = bbox[2] - bbox[0]
        x = (width - w) // 2
        draw.text((x, y), line, fill="black", font=font)
        y += h + 8

    # -----------------------------
    # Paste QR at bottom
    # -----------------------------

    qr_x = (width - qr_w) // 2
    qr_y = height - qr_h - margin
    img.paste(qr_img, (qr_x, qr_y))

    img.save(output_path, dpi=(DPI, DPI))
    os.remove(qr_tmp)

    print(f"Card saved to {output_path}")


# -----------------------------
# CLI
# -----------------------------

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Usage: python generate_card.py <track_id> [output_file]")
        sys.exit(1)

    track_id = sys.argv[1]

    if len(sys.argv) >= 3:
        output = sys.argv[2]
    else:
        output = os.path.join(OUTPUT_DIR, f"{track_id}.png")

    generate_card(track_id, output)
