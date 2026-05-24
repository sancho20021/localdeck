import sys
from pathlib import Path

def filter_new_tracks(old_file_path: str, new_file_path: str):
    # Convert to Path objects to handle directories and suffixes
    old_p = Path(old_file_path)
    new_p = Path(new_file_path)
    output_p = new_p.parent / "filtered_new_tracks.txt"

    # 1. Load old tracks into a set for fast O(1) lookups
    # .strip() removes whitespace and newlines
    if old_p.exists():
        with open(old_p, 'r') as f:
            old_tracks = {line.strip() for line in f if line.strip()}
    else:
        print(f"Warning: {old_file_path} not found. Treating as empty.")
        old_tracks = set()

    # 2. Identify tracks in new_tracks not present in old_tracks
    unique_new_tracks = []
    with open(new_p, 'r') as f:
        for line in f:
            track = line.strip()
            if track and track not in old_tracks:
                unique_new_tracks.append(track)

    # 3. Write results to a file in the same directory as new_tracks
    with open(output_p, 'w') as f:
        f.write("\n".join(unique_new_tracks) + "\n")

    print(f"Done! {len(unique_new_tracks)} unique tracks saved to: {output_p}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python script.py <old_tracks_file> <new_tracks_file>")
    else:
        filter_new_tracks(sys.argv[1], sys.argv[2])