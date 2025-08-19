#!/usr/bin/env python3
import os
import csv

CSV_PATH = os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test.expanded.csv")


def main():
    if not os.path.exists(CSV_PATH):
        print(f"❌ File not found: {CSV_PATH}")
        return
    with open(CSV_PATH, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        print("❌ CSV empty")
        return
    header = rows[0]
    # If a Contact id already exists, do nothing
    if any(h.strip().lower() == "contact id" for h in header):
        print("ℹ️  'Contact id' already present; no changes made")
        return
    # Insert as first column
    header = ["Contact id"] + header
    out_rows = [header]
    for idx, row in enumerate(rows[1:], start=1):
        out_rows.append([str(idx)] + row)
    with open(CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(out_rows)
    print(f"✅ Added 'Contact id' to CSV with {len(out_rows)-1} IDs")


if __name__ == "__main__":
    main()


