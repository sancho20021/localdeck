#!/usr/bin/env python3

from PIL import Image, ImageDraw
import os
import math

# ----------------------------
# CONFIG
# ----------------------------
INPUT_DIR = "../cards/batch5/ready"
OUTPUT_DIR = "../cards/batch5"
os.makedirs(OUTPUT_DIR, exist_ok=True)

DPI = 300
MM_TO_INCH = 25.4

A3_WIDTH_MM = 297
A3_HEIGHT_MM = 420

A3_WIDTH_PX = int(A3_WIDTH_MM / MM_TO_INCH * DPI)
A3_HEIGHT_PX = int(A3_HEIGHT_MM / MM_TO_INCH * DPI)

MARGIN_MM = 10
GAP_MM = 0

MARGIN_PX = int(MARGIN_MM / MM_TO_INCH * DPI)
GAP_PX = int(GAP_MM / MM_TO_INCH * DPI)

BLEED_MM = 3
BLEED_PX = int(BLEED_MM / MM_TO_INCH * DPI)

MARK_LEN_MM = 5
MARK_LEN_PX = int(MARK_LEN_MM / MM_TO_INCH * DPI)

# ----------------------------
# Load PNGs
# ----------------------------
png_files = sorted([
    os.path.join(INPUT_DIR, f)
    for f in os.listdir(INPUT_DIR)
    if f.lower().endswith(".png")
])

if not png_files:
    raise RuntimeError("No PNG files found")

sample_img = Image.open(png_files[0])
card_width, card_height = sample_img.size

# ----------------------------
# Grid math
# ----------------------------
usable_width = A3_WIDTH_PX - 2 * MARGIN_PX
usable_height = A3_HEIGHT_PX - 2 * MARGIN_PX

cols = usable_width // (card_width + GAP_PX)
rows = usable_height // (card_height + GAP_PX)

if cols < 1 or rows < 1:
    raise RuntimeError("Card size too big")

cards_per_page = cols * rows
print(f"Grid: {cols} x {rows} -> {cards_per_page} per page")

# ----------------------------
# Pages
# ----------------------------
pages = []

for page_idx in range(math.ceil(len(png_files) / cards_per_page)):

    # RGB canvas (FIXED)
    page_img = Image.new("RGB", (A3_WIDTH_PX, A3_HEIGHT_PX), (255, 255, 255))
    draw = ImageDraw.Draw(page_img)

    page_cards = png_files[page_idx * cards_per_page:(page_idx + 1) * cards_per_page]

    total_w = cols * card_width + (cols - 1) * GAP_PX
    total_h = rows * card_height + (rows - 1) * GAP_PX

    left = MARGIN_PX
    top = MARGIN_PX
    right = left + total_w
    bottom = top + total_h

    # ----------------------------
    # BLEED
    # ----------------------------
    draw.rectangle(
        [
            left - BLEED_PX,
            top - BLEED_PX,
            right + BLEED_PX,
            bottom + BLEED_PX
        ],
        fill="red",
    )

    # ----------------------------
    # Crop marks
    # ----------------------------
    for r in range(rows + 1):
        y = MARGIN_PX + r * (card_height + GAP_PX)

        draw.line([(left - MARK_LEN_PX, y), (left, y)], fill=(0, 0, 0), width=1)
        draw.line([(right, y), (right + MARK_LEN_PX, y)], fill=(0, 0, 0), width=1)

    for c in range(cols + 1):
        x = MARGIN_PX + c * (card_width + GAP_PX)

        draw.line([(x, top - MARK_LEN_PX), (x, top)], fill=(0, 0, 0), width=1)
        draw.line([(x, bottom), (x, bottom + MARK_LEN_PX)], fill=(0, 0, 0), width=1)

    # ----------------------------
    # Paste cards (alpha-safe)
    # ----------------------------
    for idx, card_path in enumerate(page_cards):

        img = Image.open(card_path).convert("RGBA")

        col = idx % cols
        row = idx // cols

        x = MARGIN_PX + col * (card_width + GAP_PX)
        y = MARGIN_PX + row * (card_height + GAP_PX)

        # Proper alpha flattening
        bg = Image.new("RGBA", img.size, (255, 255, 255, 255))
        flat = Image.alpha_composite(bg, img).convert("RGB")

        page_img.paste(flat, (x, y))

    pages.append(page_img)

# ----------------------------
# Save PNG pages
# ----------------------------
for i, page in enumerate(pages):
    out_path = os.path.join(OUTPUT_DIR, f"a3_cards_page_{i+1:03d}.png")
    page.save(out_path, "PNG", dpi=(DPI, DPI), optimize=False)
    print(f"Saved: {out_path}")