import json
import os
import re
import subprocess
import sys
from typing import Tuple


def clear_screen() -> None:
    """Clears the terminal screen across platforms."""
    os.system("cls" if os.name == "nt" else "clear")


def get_unprinted_tracks() -> list[str]:
    """Fetches all unprinted track IDs from localdeck."""
    result = subprocess.run(
        ["localdeck", "print", "list"],
        capture_output=True,
        text=True,
        check=True,
    )

    # Filter out empty lines or summary text, parsing bullet points or raw IDs
    track_ids = []
    for line in result.stdout.strip().splitlines():
        clean_line = line.strip().lstrip("•").strip()
        if clean_line.isdigit():
            track_ids.append(clean_line)

    return track_ids


def get_metadata(track_id: str) -> Tuple[str, str, str]:
    """Retrieves metadata for a specific track ID."""
    result = subprocess.run(
        ["localdeck", "meta", "get", track_id, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    data: dict = json.loads(result.stdout)
    return data["artist"], data["title"], data["artwork"]


def mark_printed(track_id: str) -> None:
    """Marks a track as printed in localdeck."""
    subprocess.run(
        ["localdeck", "print", "mark", track_id],
        capture_output=True,
        text=True,
        check=True,
    )


def find_track_id(alias_or_hash: str) -> str | None:
    """Runs `localdeck find <hash>` and extracts the numeric track ID."""
    result = subprocess.run(
        ["localdeck", "find", alias_or_hash],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        return None

    # Looks for a line starting with digits (e.g., "539 at:")
    match = re.search(r"^\s*(\d+)\s+at:", result.stdout, re.MULTILINE)
    if match:
        return match.group(1)

    return None


def process_interactive():
    print("Fetching unprinted tracks...")
    try:
        track_ids = get_unprinted_tracks()
    except subprocess.CalledProcessError as e:
        print(f"Error executing localdeck command: {e}", file=sys.stderr)
        sys.exit(1)

    if not track_ids:
        print("No unprinted tracks found! You're all caught up.")
        return

    total = len(track_ids)
    marked_count = 0

    for idx, track_id in enumerate(track_ids, start=1):
        # Clear screen for a fresh view per prompt
        clear_screen()

        try:
            artist, title, _ = get_metadata(track_id)
            track_info = f"'{artist}' - '{title}'"
        except Exception:
            track_info = "<Metadata unavailable>"

        # Interactive loop for single track
        while True:
            user_input = input(
                f"[{idx}/{total}] Track #{track_id}: {track_info} - Printed? [y/n/q]: "
            ).strip().lower()

            if user_input == "y":
                mark_printed(track_id)
                marked_count += 1
                break
            elif user_input == "n":
                break
            elif user_input == "q":
                clear_screen()
                print("Process aborted by user.")
                print(f"Summary: Marked {marked_count} track(s) as printed.")
                return
            else:
                print("Invalid input. Please enter 'y', 'n', or 'q'.")

    clear_screen()
    print(f"Finished! Marked a total of {marked_count} track(s) as printed.")


def process_file(file_path: str):
    """Processes a file containing hashes/aliases and marks them printed."""
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)

    with open(file_path, "r", encoding="utf-8") as f:
        aliases = [line.strip() for line in f if line.strip()]

    total = len(aliases)
    if total == 0:
        print(f"File '{file_path}' is empty.")
        return

    marked_count = 0
    failed_count = 0

    for idx, alias in enumerate(aliases, start=1):
        clear_screen()
        print(f"Processing Batch File [{idx}/{total}]")
        print("-" * 50)
        print(f"Lookup Hash/Alias: {alias}")

        track_id = find_track_id(alias)

        if track_id:
            mark_printed(track_id)
            marked_count += 1
            print(f"✓ Resolved to Track #{track_id} -> Marked as PRINTED.")
        else:
            failed_count += 1
            print(f"✗ Failed to find TrackId for: {alias}")

    print("\n" + "=" * 50)
    print(
        f"Done! Successfully marked: {marked_count} | Failed/Not found: {failed_count}"
    )


def main():
    if len(sys.argv) > 1:
        process_file(sys.argv[1])
    else:
        process_interactive()


if __name__ == "__main__":
    main()
