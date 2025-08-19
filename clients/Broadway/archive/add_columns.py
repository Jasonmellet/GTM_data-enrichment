#!/usr/bin/env python3
import os
import csv

INPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test.csv")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test.expanded.csv")

NEW_COLUMNS = [
    # Company-level enrichment
    "Company Phone",
    "Company Linkedin Url",
    "Facebook Url",
    "Instagram Url",
    "YouTube Url",
    "TikTok Url",
    "Twitter/X Url",
    "Accreditations",           # e.g., ACA
    "Founded Year",
    "Enrollment Size",
    "Price Range",              # e.g., $-$$$ or numeric range
    "Session Length",           # e.g., 1 week, 2 weeks, multi-week
    "Season",                   # summer / year-round
    "Age Range",                # e.g., 6–17
    "Overnight/Day",
    "Camp Category",            # unified taxonomy
    "Primary Location",         # city/state summary
    # Contact-level enrichment
    "Contact Linkedin Url",
    "Alternate Phone",
    "Email Status",             # valid/catch-all/bounced/unverifiable
    "Email Confidence",         # high/med/low
    "Role Group",               # Owner/Director/Program/Registrar/Marketing
    # Provenance
    "Source Verified URL",
    "Verified On",
    "Notes",
]


def read_rows(path):
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames


def write_rows(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File not found: {INPUT_FILE}")
        return
    rows, headers = read_rows(INPUT_FILE)

    # Append new columns that are missing
    fieldnames = list(headers)
    for col in NEW_COLUMNS:
        if col not in fieldnames:
            fieldnames.append(col)

    # Ensure each row has keys for all new columns
    for r in rows:
        for col in NEW_COLUMNS:
            r.setdefault(col, "")

    write_rows(OUTPUT_FILE, fieldnames, rows)
    print(f"✅ Expanded CSV written to: {OUTPUT_FILE}")
    print(f"➕ Added columns: {', '.join([c for c in NEW_COLUMNS if c not in headers])}")


if __name__ == "__main__":
    main()


