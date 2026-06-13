#!/usr/bin/env python3
import re
import subprocess
import sys


def run_command(cmd):
    """Runs a system command and returns (stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True
        )
        return result.stdout.strip(), None
    except subprocess.CalledProcessError as e:
        err = e.stderr.strip() if e.stderr else e.output.strip()
        return None, err


def parse_missing_output(output_text):
    """Parses the specific multi-line 'localdeck check missing' format.

    Returns a list of dicts: [{'track_id': '566', 'clean_path':
    'music/techno/...'}, ...]
    """
    missing_items = []

    # Regex to extract blocks starting with a Track ID followed by 'Unavailable locations:'
    # This safely captures the Track ID and the path line even with sizes underneath
    block_pattern = re.compile(
        r"^(\d+)\s*\nUnavailable locations:\s*\n\s*-\s*USB\([^)]+\)/(.*?)\s*$",
        re.MULTILINE,
    )

    for match in block_pattern.finditer(output_text):
        track_id = match.group(1)
        raw_path = match.group(2)

        # Strip any trailing whitespace or residual artifacts from the path line
        clean_path = raw_path.strip()

        missing_items.append({"track_id": track_id, "clean_path": clean_path})

    return missing_items


def purge_missing_tracks():
    print(" Fetching missing tracks from localdeck...")
    # Adjust "localdeck check missing" if your CLI command uses a slightly different syntax
    stdout, error = run_command(["localdeck", "check", "missing"])

    if error:
        print(f"❌ Error running 'localdeck check missing': {error}")
        sys.exit(1)

    if not stdout or "Unavailable locations" not in stdout:
        print(" No missing tracks found. Everything is clean!")
        return

    missing_tracks = parse_missing_output(stdout)

    if not missing_tracks:
        print("⚠️ Found missing entries, but parsing logic failed to extract them.")
        print("Raw output sample:\n", stdout[:300])
        sys.exit(1)

    print(f" Found {len(missing_tracks)} missing tracks to clean up.\n")

    success_count = 0
    fail_count = 0

    for item in missing_tracks:
        track_id = item["track_id"]
        target_path = item["clean_path"]

        print(f"Processing Track {track_id} -> Dropping relative path: {target_path}")

        # Execute the forget command targeting the relative path layout
        forget_stdout, forget_err = run_command(["localdeck", "forget", target_path])

        if forget_err:
            print(f"  ❌ System Error on forget: {forget_err}")
            fail_count += 1
            continue

        # Robust Success Check: Verify your tool reported affected updates
        # Matches your tool's logic: "Affected tracks: 1" or similar positive confirmation
        if "Affected tracks: 0" in forget_stdout or "No tracks located" in forget_stdout:
            print("  ⚠️ Tool reported 0 tracks affected. Check if path matches exactly.")
            fail_count += 1
        else:
            # Prints the tool's response cleanly (e.g., "Forget operation completed...")
            summary = [line.strip() for line in forget_stdout.splitlines() if "Affected" in line or "Removed" in line]
            print(f"  ✅ Success: {', '.join(summary) if summary else 'Path removed'}")
            success_count += 1

    print("\n--- Summary Clean Report ---")
    print(f" Successfully forgotten:  {success_count} tracks")
    print(f" Failed or skipped paths: {fail_count} entries")


if __name__ == "__main__":
    purge_missing_tracks()
