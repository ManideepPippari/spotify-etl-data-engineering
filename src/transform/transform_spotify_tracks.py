"""
Simple local transformation script.

Usage:
    python transform_spotify_tracks.py input.csv output.csv
"""

import sys
import csv
from pathlib import Path


def transform(input_path: Path, output_path: Path):
    with input_path.open("r", encoding="utf-8") as f_in, \
         output_path.open("w", encoding="utf-8", newline="") as f_out:

        reader = csv.DictReader(f_in)
        fieldnames = list(reader.fieldnames) + ["duration_minutes"]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            try:
                duration_ms = float(row.get("duration_ms", 0))
                row["duration_minutes"] = round(duration_ms / 60000.0, 2)
            except Exception:
                row["duration_minutes"] = ""

            writer.writerow(row)

    print(f"Transformed file written to: {output_path}")


if _name_ == "_main_":
    if len(sys.argv) != 3:
        print("Usage: python transform_spotify_tracks.py input.csv output.csv")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    output_file = Path(sys.argv[2])

    transform(input_file, output_file)