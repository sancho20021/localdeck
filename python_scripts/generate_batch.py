#!/usr/bin/env python3

import sys
import os
from generate_card import generate_card  # import from your existing script

def main(track_file: str, output_dir: str) -> None:
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Read all track IDs
    with open(track_file, "r") as f:
        track_ids = [line.strip() for line in f if line.strip()]

    print(f"Found {len(track_ids)} track IDs in {track_file}")

    # Generate cards
    for track_id in track_ids:
        output_path = os.path.join(output_dir, f"{track_id}.png")
        print(f"Generating card for {track_id} -> {output_path}")
        try:
            generate_card(track_id, output_path)
        except Exception as e:
            print(f"Error generating card for {track_id}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_cards.py <track_file.txt> <output_dir>")
        sys.exit(1)

    track_file = sys.argv[1]
    output_dir = sys.argv[2]

    main(track_file, output_dir)
