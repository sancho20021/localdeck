#!/usr/bin/env python3
"""
Merges every PDF in a folder into <folder>/cards.pdf, one after another in
track id order. Pages are copied as they are: nothing is re-encoded or
resampled, and page boxes (TrimBox / BleedBox) are kept.
"""

import argparse
import os
import re
from typing import List

import pikepdf

OUTPUT_NAME = "cards.pdf"


def natural_key(name: str) -> List:
    """So that 9.pdf comes before 10.pdf."""
    return [int(part) if part.isdigit() else part.lower() for part in re.split(r"(\d+)", name)]


def find_pdfs(folder: str) -> List[str]:
    names = [
        f for f in os.listdir(folder)
        if f.lower().endswith(".pdf") and f != OUTPUT_NAME
    ]
    return [os.path.join(folder, f) for f in sorted(names, key=natural_key)]


def merge_pdfs(paths: List[str], output_path: str) -> None:
    merged = pikepdf.Pdf.new()
    sources = []

    for path in paths:
        src = pikepdf.open(path)
        sources.append(src)  # must stay open until the merged file is saved
        merged.pages.extend(src.pages)

        # Keep the colour profile the cards were made for (same for all of them)
        if "/OutputIntents" not in merged.Root and "/OutputIntents" in src.Root:
            merged.Root.OutputIntents = merged.copy_foreign(src.make_indirect(src.Root.OutputIntents))

    merged.docinfo["/Title"] = "Cards"
    merged.save(output_path)

    for src in sources:
        src.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Merge all PDFs in a folder into {OUTPUT_NAME}.")
    parser.add_argument("folder")

    args = parser.parse_args()

    paths = find_pdfs(args.folder)
    if not paths:
        raise SystemExit(f"No PDF files found in {args.folder}")

    output = os.path.join(args.folder, OUTPUT_NAME)
    merge_pdfs(paths, output)
    print(f"Merged {len(paths)} PDFs into {output}")
