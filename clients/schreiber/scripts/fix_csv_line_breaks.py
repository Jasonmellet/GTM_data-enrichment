#!/usr/bin/env python3
import os

INPUT_PATH = "../data/Schreiber Sheet 5_11 test - Sheet1 (2).csv"
OUTPUT_PATH = "../data/Schreiber Sheet 5_11 test - Sheet1 (2).fixed.csv"


def fix_csv(input_path: str, output_path: str) -> None:
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.read().splitlines()

    fixed_rows = []
    buffer = ""
    quote_count = 0

    for raw_line in lines:
        line = raw_line
        if buffer:
            buffer += "\n" + line
            quote_count += line.count('"')
            if quote_count % 2 == 0:
                merged = buffer.replace("\r", " ").replace("\n", " ")
                fixed_rows.append(" ".join(merged.split()))
                buffer = ""
                quote_count = 0
        else:
            quote_count = line.count('"')
            if quote_count % 2 == 0:
                fixed_rows.append(" ".join(line.replace("\r", " ").replace("\n", " ").split()))
                quote_count = 0
            else:
                buffer = line

    if buffer:
        merged = buffer.replace("\r", " ").replace("\n", " ")
        fixed_rows.append(" ".join(merged.split()))

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        for row in fixed_rows:
            f.write(row + "\n")

    print(f"✅ Fixed CSV written to: {output_path}")


if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ File not found: {INPUT_PATH}")
    else:
        fix_csv(INPUT_PATH, OUTPUT_PATH)
