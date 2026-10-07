#!/usr/bin/env python3

import os
import argparse
from typing import Callable, Dict

from generate_card import (
    ARTWORK_DIR,
    ArtworkNeeded,
    find_artwork_override,
    generate_card,
    parse_color,
)


TO_FIX_FILE = "to_fix.txt"
TO_FIX_HEADER = "# Cards that still need attention (track_id<TAB>reason); they are retried on every run.\n"


def load_to_fix(output_dir: str) -> Dict[str, str]:
    path = os.path.join(output_dir, TO_FIX_FILE)
    if not os.path.exists(path):
        return {}
    to_fix = {}
    with open(path, "r") as f:
        for line in f:
            line = line.rstrip("\n")
            if line and not line.startswith("#"):
                track_id, _, reason = line.partition("\t")
                to_fix[track_id] = reason
    return to_fix


def save_to_fix(output_dir: str, to_fix: Dict[str, str]) -> None:
    path = os.path.join(output_dir, TO_FIX_FILE)
    if not to_fix:
        if os.path.exists(path):
            os.remove(path)
        return
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write(TO_FIX_HEADER)
        for track_id, reason in to_fix.items():
            f.write(f"{track_id}\t{reason}\n")
    os.replace(tmp, path)


def is_done(output_path: str, override: str | None, needs_fix: bool, force: bool) -> bool:
    """
    A card is done if it exists, isn't listed in to_fix.txt, and no hand-made
    artwork was added or changed after it was generated.
    """
    if force or needs_fix or not os.path.exists(output_path):
        return False
    return override is None or os.path.getmtime(override) <= os.path.getmtime(output_path)


def run_batch(
    generate: Callable[..., None],
    extension: str,
    tracks_file: str,
    output_dir: str,
    add_picture: bool,
    color: tuple | str,
    artwork_dir: str = ARTWORK_DIR,
    force: bool = False,
) -> None:
    os.makedirs(output_dir, exist_ok=True)

    with open(tracks_file, "r") as f:
        track_ids = [line.strip() for line in f if line.strip()]

    print(f"Found {len(track_ids)} track IDs in {tracks_file}")
    print(f"Add picture: {add_picture}")

    # Survives between runs: what's still undone, and why
    to_fix = load_to_fix(output_dir)

    generated = []
    skipped = []
    needs_artwork = []
    failures = []

    for track_id in track_ids:
        output_path = os.path.join(output_dir, f"{track_id}{extension}")
        override = find_artwork_override(track_id, artwork_dir) if add_picture else None

        if is_done(output_path, override, track_id in to_fix, force):
            skipped.append(track_id)
            continue

        print(f"\nGenerating card for {track_id} -> {output_path}")

        # A listed card's file was never right (or is half-written): remove it, so that
        # every card file in the output folder is a good one
        if track_id in to_fix and os.path.exists(output_path):
            os.remove(output_path)

        # Listed until proven fine, so an interrupted run leaves no card looking done
        to_fix[track_id] = to_fix.get(track_id, "interrupted")
        save_to_fix(output_dir, to_fix)

        try:
            generate(
                track_id,
                output_path,
                add_picture=add_picture,
                color=color,
                artwork_dir=artwork_dir,
            )
            generated.append(track_id)
            del to_fix[track_id]

        except ArtworkNeeded as e:
            print(f"No card: {e}")
            needs_artwork.append((track_id, str(e)))
            to_fix[track_id] = str(e)

        except Exception as e:
            print(f"Error generating card for {track_id}: {e}")
            failures.append((track_id, str(e)))
            to_fix[track_id] = f"failed: {e}"

        save_to_fix(output_dir, to_fix)

    # Summary
    print(f"\n=== SUMMARY ===")
    print(
        f"Tracks: {len(track_ids)} - generated: {len(generated)}, needs artwork: {len(needs_artwork)}, "
        f"failed: {len(failures)}, already done: {len(skipped)}"
    )

    sections = [
        ("NEEDS ARTWORK (no card made)", needs_artwork),
        ("FAILURES", failures),
    ]
    for title, items in sections:
        if items:
            print(f"\n=== {title} ===")
            for tid, reason in items:
                print(f"{tid}: {reason}")

    if to_fix:
        print(f"\n{len(to_fix)} card(s) listed in {os.path.join(output_dir, TO_FIX_FILE)}, they are retried on every run.")
    else:
        print("\nAll cards done.")


def main(generate: Callable[..., None], extension: str) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tracks_file")
    parser.add_argument("output_dir")
    parser.add_argument("--add-picture", action="store_true")
    parser.add_argument("--color", default="red",type=str)
    parser.add_argument(
        "--artwork-dir",
        default=ARTWORK_DIR,
        help=f"hand-made square artwork, <track_id>.<png|jpg> (default: {ARTWORK_DIR})",
    )
    parser.add_argument("--force", action="store_true", help="regenerate cards that are already done")

    args = parser.parse_args()
    color = parse_color(args.color)

    run_batch(
        generate,
        extension,
        args.tracks_file,
        args.output_dir,
        args.add_picture,
        color,
        artwork_dir=args.artwork_dir,
        force=args.force,
    )


if __name__ == "__main__":
    main(generate_card, ".png")
