#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path

def verify_hashes(file_path):
    hashes_file = Path(file_path)

    if not hashes_file.is_file():
        print(f"❌ Error: File '{hashes_file}' not found.")
        sys.exit(1)

    print("=== Starting Hash Verification ===")
    print(f"Reading hashes from: {hashes_file}")
    print("-" * 40)

    success_count = 0
    fail_count = 0

    # Open and process the file line by line
    with open(hashes_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            # Trim leading/trailing spaces and newlines
            current_hash = line.strip()

            # Skip empty lines
            if not current_hash:
                continue

            try:
                # Call localdeck find <hash>
                result = subprocess.run(
                    ["localdeck", "find", current_hash],
                    capture_output=True,
                    text=True,
                    check=False  # Don't crash if localdeck returns a non-zero exit code
                )

                # Combine stdout and stderr just in case your tool prints errors to stderr
                output = (result.stdout + result.stderr).strip()

                if "at:" in output:
                    # Extract the first word/number from the output line safely
                    track_num = output.split('\n')[0].split()[0]
                    print(f"Line {line_num} [{current_hash}]: ✅ Found (Track {track_num})")
                    success_count += 1

                elif "No tracks found" in output:
                    print(f"Line {line_num} [{current_hash}]: ❌ Missing (No tracks found)")
                    fail_count += 1

                else:
                    print(f"Line {line_num} [{current_hash}]: ⚠️ Unexpected Output:")
                    print(f"    {output}")
                    fail_count += 1

            except FileNotFoundError:
                print("❌ Error: 'localdeck' CLI tool is not installed or not in your system PATH.")
                sys.exit(1)
            except Exception as e:
                print(f"Line {line_num} [{current_hash}]: ❌ Script Error running command: {e}")
                fail_count += 1

    print("-" * 40)
    print("=== Final Verification Summary ===")
    print(f"Total Verified:   {success_count}")
    print(f"Total Missing:    {fail_count}")

    # Exit cleanly or signal failure to bash if things are missing
    if fail_count > 0:
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: ./verify_hashes.py <path_to_hashes_file.txt>")
        sys.exit(1)

    verify_hashes(sys.argv[1])
