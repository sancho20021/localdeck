#!/usr/bin/env python3

import sys
import os
import argparse
from generate_card import generate_card


def main(tracks_file: str, output_dir: str, add_picture: bool) -> None:
    os.makedirs(output_dir, exist_ok=True)

    with open(tracks_file, "r") as f:
        track_ids = [line.strip() for line in f if line.strip()]

    print(f"Found {len(track_ids)} track IDs in {tracks_file}")
    print(f"Add picture: {add_picture}")

    failures = []

    for track_id in track_ids:
        output_path = os.path.join(output_dir, f"{track_id}.png")
        print(f"\nGenerating card for {track_id} -> {output_path}")

        try:
            picture_applied = generate_card(
                track_id,
                output_path,
                add_picture=add_picture,
            )

            if add_picture and not picture_applied:
                failures.append((track_id, "picture not applied"))

        except Exception as e:
            print(f"Error generating card for {track_id}: {e}")
            failures.append((track_id, str(e)))

    # Summary
    if failures:
        print("\n=== FAILURES ===")
        for tid, reason in failures:
            print(f"{tid}: {reason}")
    else:
        print("\nAll cards generated successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tracks_file")
    parser.add_argument("output_dir")
    parser.add_argument("--add-picture", action="store_true")

    args = parser.parse_args()

    main(args.tracks_file, args.output_dir, args.add_picture)