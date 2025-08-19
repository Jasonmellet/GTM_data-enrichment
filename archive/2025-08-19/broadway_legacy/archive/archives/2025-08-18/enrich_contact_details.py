#!/usr/bin/env python3
import os
import json
import argparse
import time
import re
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

CSV_FILE_PATH = "../data/Summer Camp Enrichment Sample Test.expanded.csv"
PERPLEXITY_API_KEY = os.getenv('BROADWAY_PERPLEXITY_API_KEY') or os.getenv('PERPLEXITY_API_KEY')
PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"

CSV_READ_KWARGS = {
    'engine': 'python',
    'quotechar': '"',
    'escapechar': '\\',
    'on_bad_lines': 'skip',
    'dtype': str,
    'keep_default_na': False
}


def _clean_text(value: str) -> str:
    if value is None:
        return ""
    s = str(value).replace("\r", " ").replace("\n", " ")
    return " ".join(s.split()).strip()


def ask_perplexity(prompt: str, model: str = "sonar-pro", retries: int = 2, timeout: int = 30) -> str:
    headers = {"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": [{"role": "user", "content": prompt}]}
    for attempt in range(retries + 1):
        try:
            r = requests.post(PERPLEXITY_URL, headers=headers, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(1.0 + attempt)
                continue
            return ""
        except Exception:
            if attempt == retries:
                return ""
            time.sleep(1.0 + attempt)


def enrich_details(company_name: str, website: str) -> dict:
    prompt = f"""
Using the official sources (start with the given website), extract the following for the organization:
Company: {company_name}
Website: {website}

Return compact JSON with keys:
- company_phone (string)
- company_linkedin_url (string)
- facebook_url (string)
- instagram_url (string)
- youtube_url (string)
- tiktok_url (string)
- twitter_url (string)
- street_address (string)
- city (string)
- state_region (string)
- postal_code (string)
- country (string)
- source_verified_url (the canonical page used)
- verified_on (YYYY-MM-DD)
- confidence (high/medium/low)

If any field is not found, leave it empty and set confidence accordingly.
"""
    raw = ask_perplexity(prompt)
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {"_raw": raw}
        data["_raw"] = raw
        return data
    except Exception:
        return {"_raw": raw}


def fast_scrape_site(website: str, timeout: int = 10) -> dict:
    """Lightweight scraper: fetch homepage and common contact/about pages; extract socials, phones, address-like lines."""
    out = {
        'company_phone': '',
        'company_linkedin_url': '',
        'facebook_url': '',
        'instagram_url': '',
        'youtube_url': '',
        'tiktok_url': '',
        'twitter_url': '',
        'street_address': '',
        'city': '',
        'state_region': '',
        'postal_code': '',
        'country': '',
        'source_verified_url': '',
        'verified_on': time.strftime('%Y-%m-%d'),
        'confidence': 'medium'
    }
    if not website:
        return out
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible)'} )
    paths = ["", "/contact", "/contact-us", "/about", "/about-us"]
    hrefs = []
    text_blob = ""
    base = website.rstrip('/')
    for p in paths:
        url = base + p
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code == 200 and len(r.text) > 0:
                text_blob += "\n" + r.text
                hrefs += re.findall(r'href\s*=\s*"([^"]+)"', r.text, re.IGNORECASE)
                if not out['source_verified_url']:
                    out['source_verified_url'] = url
        except Exception:
            continue

    def pick_href(domain_key: str):
        for h in hrefs:
            if domain_key in h:
                return h
        return ''

    out['company_linkedin_url'] = pick_href('linkedin.com/company') or pick_href('linkedin.com/school') or ''
    out['facebook_url'] = pick_href('facebook.com')
    out['instagram_url'] = pick_href('instagram.com')
    out['youtube_url'] = pick_href('youtube.com') or pick_href('youtu.be')
    out['tiktok_url'] = pick_href('tiktok.com')
    out['twitter_url'] = pick_href('twitter.com') or pick_href('x.com')

    # phone
    phone_matches = re.findall(r'(\+?1?[\s\-\.]?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})', text_blob)
    if phone_matches:
        out['company_phone'] = phone_matches[0]

    # very light address capture (lines with street keywords)
    addr_match = re.search(r'(\d+\s+[^\n,<]+\b(St|Street|Rd|Road|Ave|Avenue|Blvd|Lane|Ln|Drive|Dr)\b[^\n,<]*)', text_blob, re.IGNORECASE)
    if addr_match:
        out['street_address'] = addr_match.group(1)
    # zip
    zip_match = re.search(r'\b\d{5}(?:-\d{4})?\b', text_blob)
    if zip_match:
        out['postal_code'] = zip_match.group(0)
    return out


def find_contact_linkedin(first_name: str, last_name: str, company_name: str) -> str:
    if not (first_name and last_name and company_name):
        return ''
    q = f"Find the LinkedIn public profile URL for {first_name} {last_name} who works at {company_name}. Return only the URL."
    resp = ask_perplexity(q)
    # Extract first URL-like token
    m = re.search(r'https?://[^\s\)]+', resp)
    return m.group(0) if m else ''


def write_details(df: pd.DataFrame, row_index: int, details: dict) -> pd.DataFrame:
    mapping = {
        'Company Phone': details.get('company_phone', ''),
        'Company Linkedin Url': details.get('company_linkedin_url', ''),
        'Facebook Url': details.get('facebook_url', ''),
        'Instagram Url': details.get('instagram_url', ''),
        'YouTube Url': details.get('youtube_url', ''),
        'TikTok Url': details.get('tiktok_url', ''),
        'Twitter/X Url': details.get('twitter_url', ''),
        'Street Address': details.get('street_address', ''),
        'City': details.get('city', ''),
        'State/Region*': details.get('state_region', ''),
        'Postal Code': details.get('postal_code', ''),
        'Country/Region': details.get('country', ''),
        'Source Verified URL': details.get('source_verified_url', ''),
        'Verified On': details.get('verified_on', ''),
        'Notes': details.get('confidence', ''),
    }
    for col, val in mapping.items():
        if col not in df.columns:
            df[col] = ""
        df.at[row_index, col] = _clean_text(val)
    return df


def main():
    parser = argparse.ArgumentParser(description="Enrich single row contact details by row index")
    parser.add_argument("--row", type=int, required=True, help="Zero-based row index in CSV")
    args = parser.parse_args()

    if not PERPLEXITY_API_KEY:
        print("❌ PERPLEXITY_API_KEY not configured")
        return
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV not found: {CSV_FILE_PATH}")
        return

    df = pd.read_csv(CSV_FILE_PATH, **CSV_READ_KWARGS)
    if args.row < 0 or args.row >= len(df):
        print(f"❌ Row index {args.row} out of range (0..{len(df)-1})")
        return
    r = df.iloc[args.row]
    company_name = r.get('Company Name', '')
    website = r.get('Website URL', '')
    print(f"🔎 Enriching details for row {args.row}: {company_name} ({website})")
    # fast scrape first for speed
    details = fast_scrape_site(website)
    # fallback to LLM if sparse
    if not any(details.get(k) for k in ['company_phone','company_linkedin_url','facebook_url','instagram_url','youtube_url','tiktok_url','twitter_url','street_address','postal_code']):
        llm_details = enrich_details(company_name, website)
        # merge llm over scrape if empty
        for k, v in llm_details.items():
            if isinstance(v, str) and v and k in details and not details.get(k):
                details[k] = v
    # person linkedin
    if not _clean_text(df.at[args.row, 'Contact Linkedin Url'] if 'Contact Linkedin Url' in df.columns else ''):
        contact_li = find_contact_linkedin(r.get('First Name',''), r.get('Last Name',''), company_name)
        if contact_li:
            if 'Contact Linkedin Url' not in df.columns:
                df['Contact Linkedin Url'] = ""
            df.at[args.row, 'Contact Linkedin Url'] = contact_li
    df = write_details(df, args.row, details)
    df.to_csv(CSV_FILE_PATH, index=False)
    print("✅ Contact details enriched and written to CSV")


if __name__ == "__main__":
    main()


