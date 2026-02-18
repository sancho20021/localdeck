#!/usr/bin/env python3
from PIL import Image
import os
import math

# ----------------------------
# CONFIG
# ----------------------------
INPUT_DIR = "../cards/batch3/ready"
OUTPUT_DIR = "../cards/batch3/toprint"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# A3 at 300 dpi: 297 x 420 mm → pixels
DPI = 300
MM_TO_INCH = 25.4
A3_WIDTH_MM = 297
A3_HEIGHT_MM = 420
A3_WIDTH_PX = int(A3_WIDTH_MM / MM_TO_INCH * DPI)
A3_HEIGHT_PX = int(A3_HEIGHT_MM / MM_TO_INCH * DPI)

MARGIN_MM = 15
MARGIN_PX = int(MARGIN_MM / MM_TO_INCH * DPI)

GAP_MM = 3
GAP_PX = int(GAP_MM / MM_TO_INCH * DPI)

# ----------------------------
# Load all PNGs
# ----------------------------
png_files = sorted([
    os.path.join(INPUT_DIR, f)
    for f in os.listdir(INPUT_DIR)
    if f.lower().endswith(".png")
])

if not png_files:
    raise RuntimeError("No PNG files found in input directory")

sample_img = Image.open(png_files[0])
card_width, card_height = sample_img.size

# ----------------------------
# Compute grid
# ----------------------------
usable_width = A3_WIDTH_PX - 2 * MARGIN_PX + GAP_PX
usable_height = A3_HEIGHT_PX - 2 * MARGIN_PX + GAP_PX

cols = usable_width // (card_width + GAP_PX)
rows = usable_height // (card_height + GAP_PX)

if cols < 1 or rows < 1:
    raise RuntimeError("Card size too big for A3 with margins/gap")

cards_per_page = cols * rows
print(f"Grid: {cols} x {rows} → {cards_per_page} per page")

# ----------------------------
# Place cards on pages
# ----------------------------
pages = []

for page_idx in range(math.ceil(len(png_files) / cards_per_page)):
    page_img = Image.new("RGB", (A3_WIDTH_PX, A3_HEIGHT_PX), color=(255, 0, 0))  # full red background

    page_cards = png_files[page_idx * cards_per_page:(page_idx + 1) * cards_per_page]

    for idx, card_path in enumerate(page_cards):
        img = Image.open(card_path)

        col = idx % cols
        row = idx // cols

        x = MARGIN_PX + col * (card_width + GAP_PX)
        y = MARGIN_PX + row * (card_height + GAP_PX)

        page_img.paste(img, (x, y))

    pages.append(page_img)

# ----------------------------
# Save all pages as single PDF
# ----------------------------
pdf_path = os.path.join(OUTPUT_DIR, "a3_cards.pdf")
pages[0].save(
    pdf_path,
    "PDF",
    resolution=DPI,
    save_all=True,
    append_images=pages[1:],
)
print(f"Saved PDF: {pdf_path}")
