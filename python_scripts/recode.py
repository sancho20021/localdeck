#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

# audio extensions to scan
AUDIO_EXTENSIONS = {".m4a"}


def run_command(cmd, shell_output=False):
    """Helper to run system commands and return stdout/stderr or raise errors."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=True
        )
        return result.stdout.strip(), None
    except subprocess.CalledProcessError as e:
        return None, e.stderr.strip() if e.stderr else e.output.strip()


def get_track_id(filename_stem):
    """Finds the track_id by passing the filename query into 'deck find'."""
    stdout, error = run_command(["localdeck", "find", filename_stem])
    if error:
        return None, f"deck find crashed: {error}"
    if not stdout or "No tracks found" in stdout:
        return None, "Not found in database"

    # Parse your layout output: "404 at:\n    - /path/to/file"
    lines = stdout.splitlines()
    if lines:
        first_line = lines[0].strip()
        # Extract the ID assuming it's the first word on the line (e.g. "404")
        potential_id = first_line.split()[0]
        if potential_id.isdigit():
            return int(potential_id), None

    return None, "Could not extract numeric track_id from output"


def process_directory(target_dir):
    target_path = Path(target_dir).resolve()
    if not target_path.is_dir():
        print(f"Error: {target_path} is not a valid directory.")
        sys.exit(1)

    fixed_dir = target_path / "fixed"
    fixed_dir.mkdir(exist_ok=True)

    print(f"--- Processing: {target_path} ---")
    print(f"--- Output directory: {fixed_dir} ---\n")

    success_count = 0
    fail_count = 0

    # Scan files directly in the target directory (non-recursive to avoid loops)
    for item in target_path.iterdir():
        if item.is_file() and item.suffix.lower() in AUDIO_EXTENSIONS:
            print(f"Processing: {item.name}")

            # 1. Look up the existing track ID using the tool
            track_id, search_err = get_track_id(item.stem)
            if search_err:
                print(f"  [SKIP] {search_err}")
                fail_count += 1
                continue

            output_file = fixed_dir / item.name

            # 2. Run FFmpeg stream copy to clean up the container properties
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",  # Overwrite existing output files
                "-i",
                str(item),
                "-c",
                "copy",
                str(output_file),
            ]

            _, ffmpeg_err = run_command(ffmpeg_cmd)
            if ffmpeg_err and not output_file.exists():
                print(f"  [ERROR] FFmpeg failed on remuxing: {ffmpeg_err}")
                fail_count += 1
                continue

            # 3. Link the fixed copy back to the database track ID using your tools
            relative_output_path = f"./fixed/{item.name}"
            # Execute command relative to current processing directory context
            try:
                result = subprocess.run(
                    ["localdeck", "add", str(track_id), relative_output_path],
                    cwd=str(target_path),
                    capture_output=True,
                    text=True,
                    check=True,
                )
                print(f"  [OK] {result.stdout.strip()}")
                success_count += 1
            except subprocess.CalledProcessError as e:
                err_msg = e.stderr.strip() if e.stderr else e.output.strip()
                print(f"  [ERROR] Failed to link via deck add: {err_msg}")
                fail_count += 1

    print("\n--- Summary Report ---")
    print(f"Successfully processed and linked: {success_count} files")
    print(f"Failed or skipped items:           {fail_count} files")


if __name__ == "__main__":
    # Pull path from args or default to local directory context
    directory_to_scan = sys.argv[1] if len(sys.argv) > 1 else "."
    process_directory(directory_to_scan)
