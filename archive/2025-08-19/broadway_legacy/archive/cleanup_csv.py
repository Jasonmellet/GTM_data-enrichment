#!/usr/bin/env python3
import os
import re
import csv
from urllib.parse import urlparse

INPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test - Sheet1.csv")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test - Sheet1.cleaned.csv")
REPORT_FILE = os.path.join(os.path.dirname(__file__), "../outputs/CLEANUP_REPORT.md")


EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
PHONE_DIGITS_RE = re.compile(r"\D+")


def normalize_whitespace(value: str) -> str:
    if value is None:
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ")
    return " ".join(s.split()).strip()


def normalize_website(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    # Add scheme if missing
    if not re.match(r"^[a-zA-Z]+://", url):
        url = "https://" + url
    try:
        p = urlparse(url)
        if not p.netloc:
            return ""
        # Lowercase host, keep path/query
        netloc = p.netloc.lower()
        return f"{p.scheme}://{netloc}{p.path or ''}{('?' + p.query) if p.query else ''}"
    except Exception:
        return ""


def normalize_email(email: str) -> str:
    e = (email or "").strip()
    if not e:
        return ""
    if EMAIL_RE.match(e):
        return e
    return ""


def normalize_phone(phone: str) -> str:
    s = (phone or "").strip()
    if not s:
        return ""
    digits = PHONE_DIGITS_RE.sub("", s)
    # Keep US-like 10 or 11-digit (leading 1) numbers; else return original digits
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
    return digits


def load_rows(path: str):
    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), reader.fieldnames


def write_rows(path: str, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File not found: {INPUT_FILE}")
        return
    rows, headers = load_rows(INPUT_FILE)

    # Track metrics
    total = len(rows)
    deduped_by_email = 0
    email_invalid = 0
    url_fixed = 0
    phone_fixed = 0
    whitespace_fixed = 0

    # Ensure consistent headers (preserve original order)
    fieldnames = headers

    seen_emails = set()
    cleaned = []

    for r in rows:
        original = dict(r)
        # Normalize all text fields whitespace
        for k, v in list(r.items()):
            nv = normalize_whitespace(v)
            if nv != (v or ""):
                whitespace_fixed += 1
            r[k] = nv

        # Email
        if 'Email' in r:
            raw_email = r['Email']
            ne = normalize_email(raw_email)
            if ne != raw_email:
                if raw_email:
                    email_invalid += 1
                r['Email'] = ne

        # Website
        website_col = None
        for cand in ('Website URL', 'Website', 'website', 'URL'):
            if cand in r:
                website_col = cand
                break
        if website_col:
            raw_url = r[website_col]
            nu = normalize_website(raw_url)
            if nu != raw_url:
                url_fixed += 1
                r[website_col] = nu

        # Phone columns
        for phone_col in ('Mobile Phone Number', 'Phone', 'Direct Phone Number'):
            if phone_col in r:
                rp = r[phone_col]
                np = normalize_phone(rp)
                if np != rp:
                    phone_fixed += 1
                    r[phone_col] = np

        # Deduplicate by email if present
        email_key = r.get('Email', '')
        if email_key:
            if email_key in seen_emails:
                deduped_by_email += 1
                continue
            seen_emails.add(email_key)

        cleaned.append(r)

    write_rows(OUTPUT_FILE, fieldnames, cleaned)

    # Report
    kept = len(cleaned)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("# Broadway CSV Cleanup Report\n\n")
        f.write(f"- Source file: {os.path.basename(INPUT_FILE)}\n")
        f.write(f"- Cleaned file: {os.path.basename(OUTPUT_FILE)}\n")
        f.write(f"- Rows in: {total}\n")
        f.write(f"- Rows out: {kept}\n")
        f.write(f"- Deduplicated by email: {deduped_by_email}\n")
        f.write(f"- Fields whitespace-normalized: {whitespace_fixed}\n")
        f.write(f"- Invalid/normalized emails: {email_invalid}\n")
        f.write(f"- Websites normalized: {url_fixed}\n")
        f.write(f"- Phones normalized: {phone_fixed}\n")

    print(f"✅ Cleaned CSV written to: {OUTPUT_FILE}")
    print(f"📄 Report: {REPORT_FILE}")


if __name__ == "__main__":
    main()


