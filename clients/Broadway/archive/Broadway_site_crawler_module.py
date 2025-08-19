#!/usr/bin/env python3
"""
Enhanced contact enrichment crawler with database persistence.
Crawls websites, extracts contact details, and persists to PostgreSQL.
"""

import os
import sys
import csv
import json
import time
import random
import argparse
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any, Set
from urllib.parse import urljoin, urlparse
import psycopg
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import re
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection


CSV_PATH = "../data/Summer Camp Enrichment Sample Test.expanded.csv"


SOCIAL_DOMAINS = {
    "facebook": ["facebook.com"],
    "instagram": ["instagram.com"],
    "youtube": ["youtube.com", "youtu.be"],
    "tiktok": ["tiktok.com"],
    "twitter": ["twitter.com", "x.com"],
    "linkedin": ["linkedin.com"],
}


PHONE_REGEX = re.compile(r"(?:(?:\+?1[-.\s]?)?)?(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}")
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# Common generic inboxes that are poor for outreach
GENERIC_EMAIL_PREFIXES = {
    "info", "contact", "hello", "admin", "office", "support", "sales",
    "team", "help", "hi", "enquiries", "enquiry", "general"
}

def is_generic_email(email: Optional[str]) -> bool:
    if not email:
        return True
    try:
        local_part = (email or "").split("@", 1)[0].lower()
        return local_part in GENERIC_EMAIL_PREFIXES
    except Exception:
        return False


def derive_company_from_website(website_url: str) -> str:
    """Derive a plausible company/camp name from a website URL when none is provided."""
    try:
        p = urlparse(normalize_url(website_url) or website_url)
        host = (p.netloc or '').lower()
        if host.startswith('www.'):
            host = host[4:]
        label = host.split(':',1)[0].split('.')
        # Prefer second-level label if present
        core = label[-2] if len(label) >= 2 else (label[0] if label else '')
        core = re.sub(r"[^a-z0-9]+", " ", core).strip()
        if not core:
            return ''
        # Title-case words
        return " ".join(w.capitalize() for w in core.split())
    except Exception:
        return ''

# Attribute regexes
PRICE_REGEX = re.compile(r"\$\s?\d{2,4}(?:,\d{3})?(?:\s?-\s?\$?\d{2,4}(?:,\d{3})?)?")
WEEKS_REGEX = re.compile(r"(\d+\s?(?:to|\-|–|—)?\s?\d*)\s+weeks?", re.I)
AGE_REGEX = re.compile(r"ages?\s*(\d{1,2})\s*(?:\-|to|–|—)\s*(\d{1,2})", re.I)


class UsageStats:
    def __init__(self) -> None:
        self.http_fetches: int = 0
        self.pages_rendered: int = 0
        self.sitemaps_discovered: int = 0
        self.perplexity_calls: int = 0
        self.perplexity_cost_usd: float = 0.0
        self.maps_findplace_calls: int = 0
        self.maps_details_calls: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "http_fetches": self.http_fetches,
            "pages_rendered": self.pages_rendered,
            "sitemaps_discovered": self.sitemaps_discovered,
            "perplexity_calls": self.perplexity_calls,
            "perplexity_cost_usd": round(self.perplexity_cost_usd, 4),
            "maps_findplace_calls": self.maps_findplace_calls,
            "maps_details_calls": self.maps_details_calls,
        }


def estimate_costs(stats: UsageStats) -> Dict[str, float]:
    # Defaults can be overridden by env vars
    # Google Places: ~$17 per 1000 calls → $0.017 per call (approx)
    maps_unit_cost = float(os.getenv("MAPS_EST_COST_PER_CALL", "0.017"))
    # Perplexity: unknown; placeholder $0.005 per call unless overridden
    px_unit_cost = float(os.getenv("PERPLEXITY_EST_COST_PER_CALL", "0.005"))
    px_cost = stats.perplexity_cost_usd if getattr(stats, "perplexity_cost_usd", 0.0) else px_unit_cost * stats.perplexity_calls
    maps_cost = maps_unit_cost * (stats.maps_findplace_calls + stats.maps_details_calls)
    return {
        "maps_cost_est_usd": round(maps_cost, 4),
        "perplexity_cost_est_usd": round(px_cost, 4),
        "total_cost_est_usd": round(maps_cost + px_cost, 4),
    }


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })
    return session


def normalize_url(url: str) -> Optional[str]:
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    if not url.startswith("http"):
        url = "https://" + url
    return url.rstrip("/")


def fetch(session: requests.Session, url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        resp = session.get(url, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return resp
    except requests.RequestException:
        return None
    return None


def render_html_with_playwright(url: str, timeout_ms: int = 12000) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(timeout_ms)
            page.goto(url)
            # wait for network idle-ish
            page.wait_for_load_state("domcontentloaded")
            html = page.content()
            context.close()
            browser.close()
            return html
    except Exception:
        return None


def discover_sitemaps(session: requests.Session, base_url: str, stats: Optional[UsageStats] = None) -> List[str]:
    sitemaps: List[str] = []
    robots = fetch(session, f"{base_url}/robots.txt")
    if robots and robots.text:
        for line in robots.text.splitlines():
            if line.lower().startswith("sitemap:"):
                sm = line.split(":", 1)[1].strip()
                sitemaps.append(sm)
    # common fallbacks
    for path in ["/sitemap.xml", "/sitemap_index.xml", "/sitemap-index.xml", "/sitemap1.xml"]:
        url = f"{base_url}{path}"
        if url not in sitemaps:
            resp = fetch(session, url)
            if resp:
                sitemaps.append(url)
    if stats is not None:
        stats.sitemaps_discovered += len(sitemaps)
    return sitemaps


def analyze_robots_and_sitemaps(session: requests.Session, base_url: str, sitemaps: List[str]) -> Dict[str, str]:
    summary: Dict[str, str] = {}
    # robots
    robots_url = f"{base_url}/robots.txt"
    robots_block = "unknown"
    try:
        r = fetch(session, robots_url)
        if r and r.text:
            text = r.text.lower()
            ua_blocks = []
            current_ua = None
            blocked_all = False
            for line in text.splitlines():
                line = line.strip()
                if line.startswith('user-agent:'):
                    current_ua = line.split(':',1)[1].strip()
                if line.startswith('disallow:') and (current_ua in ('*', None)):
                    val = line.split(':',1)[1].strip()
                    ua_blocks.append(val)
            if '/' in ua_blocks:
                robots_block = 'blocked_all'
            elif ua_blocks:
                robots_block = 'partial'
            else:
                robots_block = 'allowed'
        else:
            robots_block = 'missing'
    except Exception:
        robots_block = 'error'
    summary['robots_overview'] = robots_block
    # sitemap
    sitemap_over = f"sitemaps={len(sitemaps)}"
    summary['sitemap_overview'] = sitemap_over
    return summary


def parse_sitemap_urls(xml_text: str) -> List[str]:
    urls: List[str] = []
    try:
        soup = BeautifulSoup(xml_text, "xml")
        for loc in soup.find_all("loc"):
            if loc.text:
                urls.append(loc.text.strip())
    except Exception:
        pass
    return urls


def candidate_pages_from_home(session: requests.Session, base_url: str, verbose: bool = False, stats: Optional[UsageStats] = None) -> Set[str]:
    candidates: Set[str] = set()
    home = fetch(session, base_url)
    if not home:
        return candidates
    if stats is not None:
        stats.http_fetches += 1
    candidates.add(base_url)
    soup = BeautifulSoup(home.text, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            href = base_url + href
        if href.startswith(base_url):
            text = (a.get_text(" ") or "").lower()
            if any(k in href.lower() for k in ["contact", "about", "team", "staff", "location", "directions", "privacy", "terms"]):
                candidates.add(href.rstrip("/"))
            elif any(k in text for k in ["contact", "about", "team", "staff", "location", "directions"]):
                candidates.add(href.rstrip("/"))
    # common guesses
    for guess in ["/contact", "/contact-us", "/contactus", "/connect", "/about", "/about-us", "/team", "/staff", "/location", "/locations", "/directions"]:
        candidates.add((base_url + guess).rstrip("/"))
    if verbose:
        print(json.dumps({"debug": "candidates", "urls": sorted(list(candidates))[:50]}, indent=2))
    return candidates


def extract_social_links(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        low = href.lower()
        for key, domains in SOCIAL_DOMAINS.items():
            if any(d in low for d in domains):
                # prefer first-found, keep canonical URL
                if key == "twitter":
                    out.setdefault("Twitter/X Url", href)
                elif key == "facebook":
                    out.setdefault("Facebook Url", href)
                elif key == "instagram":
                    out.setdefault("Instagram Url", href)
                elif key == "youtube":
                    out.setdefault("YouTube Url", href)
                elif key == "tiktok":
                    out.setdefault("TikTok Url", href)
                elif key == "linkedin":
                    if "/company/" in low or "/school/" in low:
                        out.setdefault("Company Linkedin Url", href)
    return out


def extract_phones_emails(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    text = soup.get_text(" ", strip=True)
    phone = None
    email = None
    # mailto first
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if href.startswith("tel:") and not phone:
            phone = re.sub(r"[^+\d]", "", href.replace("tel:", "").strip())
        if href.startswith("mailto:") and not email:
            email = href.replace("mailto:", "").strip()
    # regex fallback
    if not phone:
        m = PHONE_REGEX.search(text)
        if m:
            phone = m.group(0)
    if not email:
        m = EMAIL_REGEX.search(text)
        if m:
            email = m.group(0)
    return phone, email


def extract_contacts_from_html(soup: BeautifulSoup) -> List[Dict[str, str]]:
    contacts: List[Dict[str, str]] = []
    # Extract mailto contacts and nearby names
    for a in soup.find_all("a", href=True):
        href_val = a["href"].strip().lower()
        if href_val.startswith("mailto:"):
            email_val = a["href"].replace("mailto:", "").strip()
            parent = a.find_parent()
            context_text = parent.get_text(" ", strip=True) if parent else a.get_text(" ", strip=True)
            name_match = re.search(r"([A-Z][a-z]+\s+[A-Z][a-z]+)", context_text)
            name_val = name_match.group(1) if name_match else ""
            contacts.append({"name": name_val, "email": email_val, "title": "", "phone": "", "linkedin": ""})
    # Extract LinkedIn profile links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "linkedin.com/in/" in href:
            text = a.get_text(" ", strip=True)
            contacts.append({"name": text, "email": "", "title": "", "phone": "", "linkedin": href})
    # Deduplicate
    unique: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str, str]] = set()
    for c in contacts:
        key = (c.get("name", ""), c.get("email", ""), c.get("linkedin", ""))
        if key not in seen and (c.get("email") or c.get("linkedin") or c.get("name")):
            seen.add(key)
            unique.append(c)
    return unique[:3]


def extract_business_attributes_from_text(text: str) -> Dict[str, str]:
    updates: Dict[str, str] = {}
    # Price range
    prices = PRICE_REGEX.findall(text)
    if prices:
        updates.setdefault("Price Range", ", ".join(sorted(set(prices))[:3]))
    # Session length (weeks)
    weeks = WEEKS_REGEX.findall(text)
    if weeks:
        norm = [w if isinstance(w, str) else "-".join([x for x in w if x]) for w in weeks]
        updates.setdefault("Session Length", ", ".join(sorted(set(norm))[:3]) + " weeks")
    # Age range
    m = AGE_REGEX.search(text)
    if m:
        updates.setdefault("Age Range", f"{m.group(1)}-{m.group(2)}")
    # Season
    low = text.lower()
    if "summer" in low:
        updates.setdefault("Season", "Summer")
    # Overnight/Day
    if "sleepaway" in low or "overnight" in low:
        updates.setdefault("Overnight/Day", "Overnight")
    elif "day camp" in low or "day-camp" in low:
        updates.setdefault("Overnight/Day", "Day")
    return updates


def extract_business_attributes_from_page(soup: BeautifulSoup) -> Dict[str, str]:
    text = soup.get_text(" ", strip=True)
    return extract_business_attributes_from_text(text)


def extract_postal_address_from_jsonld(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "{}")
        except Exception:
            continue
        # could be list or object
        candidates = data if isinstance(data, list) else [data]
        for node in candidates:
            addr = node.get("address") if isinstance(node, dict) else None
            if isinstance(addr, dict):
                out.setdefault("Street Address", addr.get("streetAddress", ""))
                out.setdefault("City", addr.get("addressLocality", ""))
                out.setdefault("Postal Code", addr.get("postalCode", ""))
                out.setdefault("Country/Region", addr.get("addressCountry", ""))
    return {k: v for k, v in out.items() if v}


def persist_enrichment_to_db(contact_id: int, enrichment_data: Dict[str, Any]) -> bool:
    """Persist enrichment data to PostgreSQL database.
    
    Prioritizes direct emails over generic ones when updating contacts.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Resolve org/contact: treat incoming contact_id as a DB contact_id; if not found, try CSV mapping
                cur.execute("""
                    SELECT org_id, contact_id FROM silver.contacts WHERE contact_id = %s
                """, (contact_id,))
                row = cur.fetchone()
                if row is None:
                    # try csv mapping table
                    try:
                        cur.execute("""
                            SELECT org_id, contact_id FROM silver.csv_contact_map WHERE csv_contact_id = %s
                        """, (contact_id,))
                        row = cur.fetchone()
                    except Exception:
                        row = None
                if row is None:
                    print(f"❌ No org/contact mapping found for id {contact_id}")
                    return False
                org_id, db_contact_id = row[0], row[1]
                print(f"📊 Persisting enrichment for contact_id {contact_id} (org_id: {org_id})")
                
                # Persist website data
                if 'website' in enrichment_data:
                    cur.execute("""
                        INSERT INTO silver.websites 
                        (org_id, url, status_code, platform_hint, last_crawled_at, js_rendered, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (org_id) DO UPDATE SET
                            url = EXCLUDED.url,
                            status_code = EXCLUDED.status_code,
                            platform_hint = EXCLUDED.platform_hint,
                            last_crawled_at = EXCLUDED.last_crawled_at,
                            js_rendered = EXCLUDED.js_rendered,
                            metadata = EXCLUDED.metadata,
                            updated_at = now()
                        RETURNING website_id
                    """, (
                        org_id,
                        enrichment_data['website'].get('url'),
                        enrichment_data['website'].get('status_code'),
                        enrichment_data['website'].get('platform_hint'),
                        datetime.now(),
                        enrichment_data['website'].get('js_rendered', False),
                        json.dumps(enrichment_data['website'].get('metadata', {}))
                    ))
                    website_id = cur.fetchone()[0]
                    print(f"  ✅ Website persisted (ID: {website_id})")
                
                # Persist socials
                if 'socials' in enrichment_data:
                    for platform, url in enrichment_data['socials'].items():
                        if url and url.strip():
                            cur.execute("""
                                INSERT INTO silver.socials 
                                (org_id, platform, url, verified_at)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (org_id, platform) DO UPDATE SET
                                    url = EXCLUDED.url,
                                    verified_at = EXCLUDED.verified_at,
                                    updated_at = now()
                                RETURNING social_id
                            """, (org_id, platform, url, datetime.now()))
                            social_id = cur.fetchone()[0]
                            print(f"  ✅ {platform} social persisted (ID: {social_id})")
                
                # Persist emails - prioritize direct emails over generic ones
                if 'emails' in enrichment_data:
                    # First check if there's a direct email (non-generic) in the list
                    direct_emails = [email for email in enrichment_data['emails'] if email and email.strip() and not is_generic_email(email)]
                    generic_emails = [email for email in enrichment_data['emails'] if email and email.strip() and is_generic_email(email)]
                    
                    # Process direct emails first, then generic ones
                    for email in (direct_emails or generic_emails):
                        if email and email.strip():
                            cur.execute("""
                                INSERT INTO silver.emails 
                                (org_id, contact_id, email, source, verified_at)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (org_id, contact_id, email) DO UPDATE SET
                                    source = EXCLUDED.source,
                                    verified_at = EXCLUDED.verified_at,
                                    updated_at = now()
                                RETURNING email_id
                            """, (org_id, db_contact_id, email, 'crawler', datetime.now()))
                            email_id = cur.fetchone()[0]
                            print(f"  ✅ Email {email} persisted (ID: {email_id})")
                
                # Persist phones
                if 'phones' in enrichment_data:
                    for phone in enrichment_data['phones']:
                        if phone and phone.strip():
                            cur.execute("""
                                INSERT INTO silver.phones 
                                (org_id, contact_id, phone_e164, phone_formatted, source, verified_at)
                                VALUES (%s, %s, %s, %s, %s, %s)
                                ON CONFLICT (org_id, contact_id, phone_e164) DO UPDATE SET
                                    phone_formatted = EXCLUDED.phone_formatted,
                                    source = EXCLUDED.source,
                                    verified_at = EXCLUDED.verified_at,
                                    updated_at = now()
                                RETURNING phone_id
                            """, (org_id, db_contact_id, phone, phone, 'crawler', datetime.now()))
                            phone_id = cur.fetchone()[0]
                            print(f"  ✅ Phone {phone} persisted (ID: {phone_id})")
                
                # Update location with business status and parsed address fields
                if 'location' in enrichment_data:
                    cur.execute("""
                        UPDATE silver.locations 
                        SET business_status = COALESCE(%s, business_status),
                            maps_verified_phone = COALESCE(%s, maps_verified_phone),
                            maps_verified_address = COALESCE(%s, maps_verified_address),
                            street = COALESCE(%s, street),
                            city = COALESCE(%s, city),
                            region = COALESCE(%s, region),
                            postal_code = COALESCE(%s, postal_code),
                            country = COALESCE(%s, country),
                            updated_at = now()
                        WHERE org_id = %s
                    """, (
                        enrichment_data['location'].get('business_status'),
                        enrichment_data['location'].get('maps_verified_phone'),
                        enrichment_data['location'].get('maps_verified_address'),
                        enrichment_data['location'].get('Street Address'),
                        enrichment_data['location'].get('City'),
                        enrichment_data['location'].get('State/Region') or enrichment_data['location'].get('region'),
                        enrichment_data['location'].get('Postal Code'),
                        enrichment_data['location'].get('Country/Region'),
                        org_id
                    ))
                    if cur.rowcount > 0:
                        print(f"  ✅ Location updated with business status")
                
                # Persist provenance
                if 'provenance' in enrichment_data:
                    cur.execute("""
                        INSERT INTO silver.provenance 
                        (org_id, source, method, metadata, collected_at)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING provenance_id
                    """, (
                        org_id,
                        'crawler',
                        'web_scraping',
                        json.dumps(enrichment_data['provenance']),
                        datetime.now()
                    ))
                    provenance_id = cur.fetchone()[0]
                    print(f"  ✅ Provenance persisted (ID: {provenance_id})")
                
                # Persist API usage
                if 'api_usage' in enrichment_data:
                    for call in enrichment_data['api_usage']:
                        cur.execute("""
                            INSERT INTO silver.api_usage 
                            (org_id, api_name, cost_usd, raw_stats)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id
                        """, (
                            org_id,
                            call.get('api_name'),
                            call.get('cost_usd', 0.0),
                            json.dumps(call)
                        ))
                        usage_id = cur.fetchone()[0]
                        print(f"  ✅ API usage {call.get('api_name')} persisted (ID: {usage_id})")
                
                conn.commit()
                return True
                
    except Exception as e:
        print(f"❌ Error persisting to database: {e}")
        return False

def update_row_in_csv(csv_file_path: str, contact_id: int, updates: Dict[str, Any]) -> bool:
    """Update CSV row with enrichment data and persist to database."""
    try:
        # Read the CSV
        rows = []
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            fieldnames = reader.fieldnames
            
            for row in reader:
                if int(row['Contact id']) == contact_id:
                    # Update the row with enrichment data
                    for key, value in updates.items():
                        if key in fieldnames:
                            row[key] = value
                    
                    # Prepare enrichment data for database persistence
                    enrichment_data = {
                        'website': {
                            'url': row.get('Website URL') or row.get('Company Website'),
                            'status_code': 200 if row.get('Website Status') == 'Active' else 404,
                            'platform_hint': 'unknown',
                            'js_rendered': False,
                            'metadata': {'title': row.get('Company Name', '')}
                        },
                        'socials': {
                            'facebook': row.get('Facebook Url'),
                            'instagram': row.get('Instagram Url'),
                            'youtube': row.get('YouTube Url'),
                            'tiktok': row.get('TikTok Url'),
                            'twitter': row.get('Twitter/X Url'),
                            'linkedin': row.get('Company Linkedin Url')
                        },
                        'emails': [row.get('Contact Email'), row.get('Contact 2 Email'), row.get('Contact 3 Email')],
                        'phones': [row.get('Company Phone'), row.get('Alternate Phone')],
                        'location': {
                            'business_status': row.get('Business Status'),
                            'maps_verified_phone': row.get('Maps Verified Phone') == 'True',
                            'maps_verified_address': row.get('Maps Verified Address') == 'True'
                        },
                        'provenance': {
                            'crawled_at': datetime.now().isoformat(),
                            'user_agent': 'Mozilla/5.0',
                            'success': True,
                            'notes': row.get('Notes', '')
                        },
                        'api_usage': []
                    }
                    
                    # Add Maps API usage if present
                    if row.get('Maps Place ID'):
                        enrichment_data['api_usage'].append({
                            'api_name': 'google_maps',
                            'endpoint': 'place_details',
                            'cost_usd': 0.017,
                            'tokens_used': 0,
                            'response_time_ms': 150,
                            'success': True,
                            'metadata': {'place_id': row.get('Maps Place ID')}
                        })
                    
                    # Persist to database
                    persist_success = persist_enrichment_to_db(contact_id, enrichment_data)
                    if persist_success:
                        print(f"✅ Database persistence completed for contact_id {contact_id}")
                    else:
                        print(f"❌ Database persistence failed for contact_id {contact_id}")
                
                rows.append(row)
        
        # Write back to CSV
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating CSV: {e}")
        return False


def crawl_and_enrich_for_contact(session: requests.Session, website_url: str, verbose: bool = False, render: bool = False, stats: Optional[UsageStats] = None) -> Dict[str, str]:
    base = normalize_url(website_url)
    if not base:
        return {}
    # strip path to domain/base
    try:
        from urllib.parse import urlparse
        p = urlparse(base)
        base_url = f"{p.scheme}://{p.netloc}"
    except Exception:
        base_url = base

    verified_map: Dict[str, str] = {}
    updates: Dict[str, str] = {}

    # sitemap discovery
    sitemaps = discover_sitemaps(session, base_url, stats=stats)
    robots_summary = analyze_robots_and_sitemaps(session, base_url, sitemaps)
    sitemap_urls: List[str] = []
    for sm in sitemaps:
        resp = fetch(session, sm)
        if resp and resp.text:
            sitemap_urls.extend(parse_sitemap_urls(resp.text))
    if verbose:
        print(json.dumps({"debug": "sitemaps", "sitemaps": sitemaps, "count_urls": len(sitemap_urls)}, indent=2))

    # candidate pages
    candidates = set()
    candidates |= candidate_pages_from_home(session, base_url, verbose=verbose, stats=stats)
    # add top N from sitemap that look relevant
    for url in sitemap_urls[:100]:
        low = url.lower()
        if any(k in low for k in ["contact", "about", "team", "staff", "location", "directions", "privacy"]):
            candidates.add(url.rstrip("/"))

    # visit candidates and collect signals
    small_content_count = 0
    fetched_any = False
    home_source_url = None
    for url in list(candidates)[:25]:
        resp = fetch(session, url)
        if not resp:
            if verbose:
                print(json.dumps({"debug": "fetch_failed", "url": url}, indent=2))
            continue
        fetched_any = True
        if stats is not None:
            stats.http_fetches += 1
        if url == base_url and not home_source_url:
            home_source_url = url
        content = resp.text or ""
        if render and len(content) < 500:
            rendered = render_html_with_playwright(url)
            if rendered:
                content = rendered
                if verbose:
                    print(json.dumps({"debug": "rendered", "url": url, "len": len(content)}, indent=2))
                if stats is not None:
                    stats.pages_rendered += 1
        if verbose:
            print(json.dumps({"debug": "fetched", "url": url, "status": resp.status_code, "len": len(content)}, indent=2))
        if 200 <= getattr(resp, 'status_code', 0) < 300:
            updates.setdefault('Website Status', 'Active')
        elif resp.status_code in (401, 403, 429):
            updates.setdefault('Website Status', 'Blocked')
        elif resp.status_code:
            updates.setdefault('Website Status', str(resp.status_code))
        if len(content) <= 200:
            small_content_count += 1
        soup = BeautifulSoup(content, "html.parser")

        # socials
        socials = extract_social_links(soup)
        for k, v in socials.items():
            if k not in updates:
                updates[k] = v
                verified_map.setdefault(k, url)

        # phone + email
        phone, email = extract_phones_emails(soup)
        if phone and "Company Phone" not in updates:
            updates["Company Phone"] = phone
            verified_map.setdefault("Company Phone", url)
        if email and "Email" not in updates:
            updates["Email"] = email
            verified_map.setdefault("Email", url)

        # postal address via JSON-LD
        addr = extract_postal_address_from_jsonld(soup)
        for k in ["Street Address", "City", "Postal Code", "Country/Region"]:
            if addr.get(k) and k not in updates:
                updates[k] = addr[k]
                verified_map.setdefault(k, url)

        # business attributes
        attrs = extract_business_attributes_from_page(soup)
        for k in ["Price Range", "Session Length", "Season", "Age Range", "Overnight/Day"]:
            if attrs.get(k) and k not in updates:
                updates[k] = attrs[k]
                verified_map.setdefault(k, url)

        # additional contacts from HTML
        html_contacts = extract_contacts_from_html(soup)
        for i, c in enumerate(html_contacts[:2]):
            n = i + 2
            if c.get("name") and f"Contact {n} Name" not in updates:
                updates[f"Contact {n} Name"] = c.get("name", "")
                verified_map.setdefault(f"Contact {n} Name", url)
            if c.get("title") and f"Contact {n} Title" not in updates:
                updates[f"Contact {n} Title"] = c.get("title", "")
                verified_map.setdefault(f"Contact {n} Title", url)
            if c.get("email") and f"Contact {n} Email" not in updates:
                updates[f"Contact {n} Email"] = c.get("email", "")
                verified_map.setdefault(f"Contact {n} Email", url)
            if c.get("phone") and f"Contact {n} Phone" not in updates:
                updates[f"Contact {n} Phone"] = c.get("phone", "")
                verified_map.setdefault(f"Contact {n} Phone", url)
            if c.get("linkedin") and f"Contact {n} Linkedin Url" not in updates:
                updates[f"Contact {n} Linkedin Url"] = c.get("linkedin", "")
                verified_map.setdefault(f"Contact {n} Linkedin Url", url)

        # be polite
        time.sleep(0.2)

    # provenance fields
    today = datetime.now().date().isoformat()
    if updates:
        # pick one source URL to record; prefer where phone or linkedin was found
        preferred_keys = ["Company Phone", "Company Linkedin Url", "Facebook Url", "Instagram Url", "YouTube Url", "TikTok Url", "Twitter/X Url", "Email"]
        source_url = None
        for k in preferred_keys:
            if k in verified_map:
                source_url = verified_map[k]
                break
        if not source_url and verified_map:
            source_url = next(iter(verified_map.values()))
        if source_url:
            updates.setdefault("Source Verified URL", source_url)
        updates.setdefault("Verified On", today)
        # attach robots/sitemap overview
        for k,v in robots_summary.items():
            updates.setdefault(k.replace('_',' ').title(), v)
        # If email is present but generic, mark and try Perplexity person lookup
        generic = is_generic_email(updates.get("Email"))
        if updates.get("Email") and generic:
            updates.setdefault("Email Status", "generic")
            updates.setdefault("Email Confidence", "low")
            # attempt person lookup using CSV context
            updates.setdefault("Notes", (updates.get("Notes") or "") + " | generic email; attempting direct email lookup")
        if "Email" in updates and not generic and "Email Status" not in updates:
            updates["Email Status"] = "found"
            updates.setdefault("Email Confidence", "medium")
        if "Email" not in updates and "Email Status" not in updates:
            updates["Email Status"] = "missing"
            updates.setdefault("Email Confidence", "low")
        return updates

    # If we fetched pages but all looked tiny/placeholder, mark status accordingly
    if fetched_any and small_content_count >= 3 and not updates:
        status_updates: Dict[str, str] = {}
        status_updates["Website Status"] = "placeholder_or_blocked"
        status_updates["Business Status"] = "possibly_closed"
        if home_source_url:
            status_updates["Source Verified URL"] = home_source_url
        status_updates["Verified On"] = today
        status_updates.setdefault("Notes", "site returned minimal content across pages; likely placeholder or blocked")
        return status_updates
    return updates


def perplexity_lookup(domain_or_url: str, verbose: bool = False, stats: Optional[UsageStats] = None) -> Dict[str, str]:
    api_key = os.getenv("BROADWAY_PERPLEXITY_API_KEY") or os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        return {}
    try:
        taxonomy = (
            "Formats & Seasons: Summer day camps, Overnight sleepaway camps, Residential camps, Day-only commuter camps, Half-day camps, "
            "Weekend mini-camps, Weeklong intensives, Two-week sessions, Extended-session camps (3+ weeks), Spring break camps, Winter break camps, "
            "Fall camps, Holiday camps, Year-round enrichment camps, Family camps, Parent–child camps, Travel/expedition camps, International travel camps, Virtual/online camps, Hybrid (online + on-site) camps; "
            "Audience & Community: Pre-K camps, Kindergarten readiness camps, Elementary school camps, Middle school camps, High school camps, Teen leadership camps, College prep camps, Coed camps, Girls-only camps, Boys-only camps, Christian camps, Catholic camps, Jewish/JCC camps, Interfaith camps, Muslim youth camps, LGBTQ+ inclusive camps, BIPOC-centered camps, Military family camps, Foster youth camps, Refugee/immigrant youth camps; "
            "Performing Arts: Theater/drama camps, Musical theater camps, Acting technique camps, Improv comedy camps, Playwriting/scriptwriting camps, Stagecraft/technical theater camps, Costume & set design camps, Dance camps, Ballet camps, Jazz dance camps, Hip-hop dance camps, Tap dance camps, Contemporary/modern dance camps, Choreography camps, Music camps, Choir/voice camps, Band & orchestra camps, Piano/keyboard camps, Guitar/strings camps, Songwriting & music production camps; "
            "Visual, Media & Design: Visual arts camps, Drawing & painting camps, Sculpture & ceramics camps, Printmaking camps, Photography camps, Film photography/darkroom camps, Filmmaking & video production camps, Animation (2D/3D) camps, Stop-motion animation camps, Graphic design camps, Illustration & comics/manga camps, Fashion design & sewing camps, Textile & fiber arts camps, Architecture & design camps, Interior design camps, UX/UI design camps, Digital media/content creator camps, Podcasting & broadcasting camps, Journalism & media literacy camps, Advertising/creative strategy camps; "
            "STEM & Tech: STEM/STEAM camps, Coding/programming camps, Game design & development camps, Robotics camps, AI & machine learning camps, Data science & analytics camps, Cybersecurity camps, Web & app development camps, Electronics & circuits camps, Engineering design camps, Mechanical engineering camps, Electrical engineering camps, Aerospace/rocketry camps, Drone & UAV camps, 3D printing & CAD camps, Maker/makerspace camps, Science discovery camps, Biology & life sciences camps, Chemistry & physics camps, Astronomy & space science camps; "
            "Academics, Language & Leadership: Academic enrichment camps, Reading & literacy camps, Creative writing camps, Debate & public speaking camps, Model United Nations camps, Social studies & civics camps, Financial literacy & entrepreneurship camps, Business & startup camps, Math enrichment camps, Test prep (SAT/ACT) camps, Study skills & executive function camps, Foreign language immersion camps, Spanish immersion camps, French immersion camps, Mandarin Chinese immersion camps, American Sign Language (ASL) camps, History & museum camps, Law & mock trial camps, Medical/health science explorer camps, Leadership & service-learning camps; "
            "Outdoors, Nature & Animals: Outdoor adventure camps, Backpacking & camping skills camps, Hiking & trail exploration camps, Survival skills/bushcraft camps, Orienteering & navigation camps, Climbing & bouldering camps, Wilderness first aid camps, Environmental science & ecology camps, Conservation & stewardship camps, Sustainability/green living camps, Farm & agriculture camps, Ranch & horsemanship camps, Equestrian/horseback riding camps, Animal care & veterinary explorer camps, Marine biology & ocean camps, Sailing & seamanship camps, Kayaking & canoeing camps, Fishing & angling camps, River & whitewater adventure camps, National parks expedition camps; "
            "Sports & Movement: Multi-sport day camps, Soccer camps, Basketball camps, Baseball camps, Softball camps, Volleyball camps, Football (non-contact/flag) camps, Tennis camps, Pickleball camps, Golf camps, Gymnastics camps, Cheerleading & dance team camps, Track & field camps, Cross-country running camps, Swimming camps, Diving camps, Rowing/crew camps, Lacrosse camps, Field hockey camps, Ice hockey camps; "
            "Inclusion, Wellness & Support: Inclusive/special needs camps, Autism spectrum (ASD) camps, ADHD support camps, Learning differences (LD/Dyslexia) camps, Speech & language development camps, Social skills development camps, Sensory-friendly camps, Adaptive sports camps, Visually impaired/Blind camps, Deaf/Hard-of-hearing camps, Wheelchair-accessible camps, Diabetes camps, Cancer survivor camps, Cardiac/heart-healthy camps, Asthma & allergy-aware camps, Bereavement & grief support camps, Trauma-informed resilience camps, Mindfulness & yoga wellness camps, Nutrition & healthy habits camps, Independent living/life skills camps."
        )
        q = (
            f"For {domain_or_url}, extract official contact details, social links, and core business attributes. Then, assign one or more categories strictly from this taxonomy: {taxonomy}. "
            f"Return JSON with keys: company_phone, facebook_url, instagram_url, youtube_url, tiktok_url, twitter_url, linkedin_company_url, "
            f"street_address, city, postal_code, country, email, source_url, price_range, session_length, season, age_range, overnight_day, camp_category, founded_year, enrollment_size, accreditations, "
            f"contacts: [ {{name, title, email, phone, linkedin}} ] (up to 3), definitive_categories (comma-separated list from the taxonomy)."
        )
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": os.getenv("PPLX_MODEL", "sonar"),
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "You extract official contact and social links only. Return JSON only."},
                    {"role": "user", "content": q},
                ],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return {}
        text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        usage = resp.json().get("usage", {})
        try:
            if stats is not None:
                stats.perplexity_calls += 1
                stats.perplexity_cost_usd += float(usage.get("cost", {}).get("total_cost", 0.0) or 0.0)
        except Exception:
            pass
        # try to locate JSON in content
        m = re.search(r"\{[\s\S]*\}", text)
        data = json.loads(m.group(0)) if m else json.loads(text)
        updates: Dict[str, str] = {}
        mapping = [
            ("company_phone", "Company Phone"),
            ("facebook_url", "Facebook Url"),
            ("instagram_url", "Instagram Url"),
            ("youtube_url", "YouTube Url"),
            ("tiktok_url", "TikTok Url"),
            ("twitter_url", "Twitter/X Url"),
            ("linkedin_company_url", "Company Linkedin Url"),
            ("street_address", "Street Address"),
            ("city", "City"),
            ("postal_code", "Postal Code"),
            ("country", "Country/Region"),
            ("email", "Email"),
            ("price_range", "Price Range"),
            ("session_length", "Session Length"),
            ("season", "Season"),
            ("age_range", "Age Range"),
            ("overnight_day", "Overnight/Day"),
            ("camp_category", "Camp Category"),
            ("founded_year", "Founded Year"),
            ("enrollment_size", "Enrollment Size"),
            ("accreditations", "Accreditations"),
            ("definitive_categories", "Definitive Categories"),
        ]
        for src, dst in mapping:
            val = (data.get(src) or "").strip() if isinstance(data, dict) else ""
            if val:
                updates[dst] = val
        # parse contacts array if present
        if isinstance(data, dict):
            contacts = data.get("contacts")
            if isinstance(contacts, list):
                for i, c in enumerate(contacts[:2]):
                    if not isinstance(c, dict):
                        continue
                    n = i + 2
                    name_v = (c.get("name") or "").strip()
                    title_v = (c.get("title") or "").strip()
                    email_v = (c.get("email") or "").strip()
                    phone_v = (c.get("phone") or "").strip()
                    li_v = (c.get("linkedin") or "").strip()
                    if name_v:
                        updates[f"Contact {n} Name"] = name_v
                    if title_v:
                        updates[f"Contact {n} Title"] = title_v
                    if email_v:
                        updates[f"Contact {n} Email"] = email_v
                    if phone_v:
                        updates[f"Contact {n} Phone"] = phone_v
                    if li_v:
                        updates[f"Contact {n} Linkedin Url"] = li_v
        src_url = (data.get("source_url") or "").strip() if isinstance(data, dict) else ""
        if src_url:
            updates["Source Verified URL"] = src_url
        if verbose and updates:
            print(json.dumps({"debug": "perplexity_updates", "updates": updates}, indent=2))
        if stats is not None:
            stats.perplexity_calls += 1
        # set statuses
        if "Email" in updates:
            updates.setdefault("Email Status", "found")
            updates.setdefault("Email Confidence", "medium")
        return updates
    except Exception:
        return {}


def perplexity_person_email_lookup(first_name: str, last_name: str, company_name: str, domain_or_url: str, verbose: bool = False, stats: Optional[UsageStats] = None) -> Dict[str, str]:
    """Ask Perplexity for the best direct email for a named person at a company.
    Returns {"Direct Email": email, "Email Confidence": level} when confident.
    """
    api_key = os.getenv("BROADWAY_PERPLEXITY_API_KEY") or os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        return {}
    try:
        person = f"{first_name} {last_name}".strip()
        q = (
            f"{person} {company_name} email address. Also search for 'camp contact details'. "
            f"Use web results to find the best direct professional email. The official company domain is likely related to {domain_or_url}. "
            "Return strictly JSON with keys: direct_email, confidence (high|medium|low), source_url."
        )
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": os.getenv("PPLX_MODEL", "sonar"),
                "temperature": 0.1,
                "messages": [
                    {"role": "system", "content": "Return JSON only. If unknown, return {}."},
                    {"role": "user", "content": q},
                ],
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return {}
        text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        m = re.search(r"\{[\s\S]*\}", text)
        data = json.loads(m.group(0)) if m else json.loads(text or "{}")
        email = (data.get("direct_email") or "").strip()
        if email and EMAIL_REGEX.fullmatch(email):
            out = {
                "Direct Email": email,
                "Email Confidence": (data.get("confidence") or "medium").lower(),
            }
            src = (data.get("source_url") or "").strip()
            if src:
                out["Source Verified URL"] = src
            try:
                if stats is not None:
                    stats.perplexity_calls += 1
                    usage = resp.json().get("usage", {})
                    stats.perplexity_cost_usd += float(usage.get("cost", {}).get("total_cost", 0.0) or 0.0)
            except Exception:
                pass
            return out
        return {}
    except Exception:
        return {}


def perplexity_find_primary_contact(company_name: str, domain_or_url: str, verbose: bool = False, stats: Optional[UsageStats] = None) -> Dict[str, str]:
    """Use Perplexity to identify the primary camp contact (director/owner).
    Returns keys: First Name, Last Name, Role Title, Source Verified URL
    """
    api_key = os.getenv("BROADWAY_PERPLEXITY_API_KEY") or os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        return {}
    try:
        q = (
            f"Who is the summer camp director or owner for {company_name}? "
            f"Prefer official sources (site:{domain_or_url}). "
            "Return strictly JSON with keys: person_name, first_name, last_name, role_title, source_url."
        )
        models = [os.getenv("PPLX_MODEL", "sonar"), "sonar-pro"]
        resp = None
        for mdl in models:
            r = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": mdl,
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": "Return JSON only."},
                        {"role": "user", "content": q},
                    ],
                },
                timeout=30,
            )
            if r.status_code == 200:
                resp = r
                break
        if not resp:
            return {}
        text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        m = re.search(r"\{[\s\S]*\}", text)
        data = json.loads(m.group(0)) if m else json.loads(text or "{}")
        person_name = (data.get("person_name") or "").strip()
        first = (data.get("first_name") or "").strip()
        last = (data.get("last_name") or "").strip()
        role = (data.get("role_title") or "").strip()
        src = (data.get("source_url") or "").strip()
        out: Dict[str, str] = {}
        if first:
            out["First Name"] = first
        if last:
            out["Last Name"] = last
        if role:
            out["Job Title"] = role
        if src:
            out["Source Verified URL"] = src
        try:
            if stats is not None:
                stats.perplexity_calls += 1
                usage = resp.json().get("usage", {})
                stats.perplexity_cost_usd += float(usage.get("cost", {}).get("total_cost", 0.0) or 0.0)
        except Exception:
            pass
        return out
    except Exception:
        return {}


def maps_lookup(company_name: str, row_context: Dict[str, str], verbose: bool = False, stats: Optional[UsageStats] = None) -> Dict[str, str]:
    api_key = os.getenv("BROADWAY_GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        return {}
    try:
        city = (row_context.get("City") or "").strip()
        region = (row_context.get("State/Region*") or row_context.get("State/Region") or "").strip()
        country = (row_context.get("Country/Region") or "").strip()
        website = (row_context.get("Website URL") or "").strip()
        query_parts = [company_name]
        if city:
            query_parts.append(city)
        if region:
            query_parts.append(region)
        if country:
            query_parts.append(country)
        if website:
            query_parts.append(website)
        query = " ".join(query_parts)

        fp_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        fp_params = {
            "input": query,
            "inputtype": "textquery",
            "fields": "place_id,name,business_status,formatted_address",
            "key": api_key,
        }
        fp_resp = requests.get(fp_url, params=fp_params, timeout=20)
        fp_json = fp_resp.json() if fp_resp.status_code == 200 else {}
        if stats is not None:
            stats.maps_findplace_calls += 1
        candidates = fp_json.get("candidates") or []
        if not candidates:
            return {}
        place_id = candidates[0].get("place_id")
        business_status = candidates[0].get("business_status")

        details_url = "https://maps.googleapis.com/maps/api/place/details/json"
        details_params = {
            "place_id": place_id,
            "fields": "name,formatted_phone_number,formatted_address,address_components,website,business_status,place_id",
            "key": api_key,
        }
        d_resp = requests.get(details_url, params=details_params, timeout=20)
        d_json = d_resp.json() if d_resp.status_code == 200 else {}
        if stats is not None:
            stats.maps_details_calls += 1
        result = d_json.get("result") or {}

        updates: Dict[str, str] = {}
        if place_id:
            updates["Maps Place ID"] = place_id
        if result.get("formatted_phone_number"):
            updates["Maps Verified Phone"] = result["formatted_phone_number"]
        if result.get("formatted_address"):
            updates["Maps Verified Address"] = result["formatted_address"]
        # Parse address components to populate location fields
        comps = result.get("address_components") or []
        comp_map = {}
        for c in comps:
            for t in c.get("types", []):
                comp_map.setdefault(t, []).append(c.get("long_name") or c.get("short_name") or "")
        street_num = (comp_map.get("street_number") or [""])[0]
        route = (comp_map.get("route") or [""])[0]
        locality = (comp_map.get("locality") or comp_map.get("postal_town") or [""])[0]
        admin1 = (comp_map.get("administrative_area_level_1") or [""])[0]
        postal = (comp_map.get("postal_code") or [""])[0]
        country = (comp_map.get("country") or [""])[0]
        street = " ".join([street_num, route]).strip()
        if street:
            updates.setdefault("Street Address", street)
        if locality:
            updates.setdefault("City", locality)
        if admin1:
            updates.setdefault("State/Region", admin1)
        if postal:
            updates.setdefault("Postal Code", postal)
        if country:
            updates.setdefault("Country/Region", country)
        if business_status or result.get("business_status"):
            status = (result.get("business_status") or business_status or "").lower()
            if status == "opERATIONAL".lower():
                updates.setdefault("Business Status", "open")
            elif status == "closed_permanently":
                updates.setdefault("Business Status", "closed_permanently")
            elif status == "closed_temporarily":
                updates.setdefault("Business Status", "closed_temporarily")
        if verbose and updates:
            print(json.dumps({"debug": "maps_updates", "updates": updates}, indent=2))
        return updates
    except Exception:
        return {}


def load_contact_row(csv_path: str, contact_id: int) -> Dict[str, str]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("Contact id", "")).strip() == str(contact_id):
                return row
    raise ValueError(f"Contact id {contact_id} not found")


def main():
    parser = argparse.ArgumentParser(description="Crawl site to enrich contact details for a single Contact id")
    parser.add_argument("--id", type=int, required=True, help="Contact id to process (1-based)")
    parser.add_argument("--csv", type=str, default=CSV_PATH, help="Path to expanded CSV")
    parser.add_argument("--verbose", action="store_true", help="Print debug output")
    parser.add_argument("--render", action="store_true", help="Use Playwright to render JS pages if needed")
    parser.add_argument("--verify-with-maps", action="store_true", help="Verify phone/address/status with Google Maps if key present")
    parser.add_argument("--maps-max-calls", type=int, default=2, help="Max Google Maps lookups per run")
    parser.add_argument("--show-usage", action="store_true", help="Print usage and cost estimates")
    parser.add_argument("--force-taxonomy", action="store_true", help="Always call Perplexity to fill Definitive Categories")
    parser.add_argument("--update-csv", action="store_true", help="Update the CSV file with enrichment data")
    args = parser.parse_args()

    row = load_contact_row(args.csv, args.id)
    website = row.get("Website URL", "")
    if not website:
        print(f"No Website URL for Contact id {args.id}")
        sys.exit(0)

    session = build_session()
    stats = UsageStats()
    print("[1/4] crawl: start")
    updates = crawl_and_enrich_for_contact(session, website, verbose=args.verbose, render=args.render, stats=stats)
    # trigger Perplexity if we have too little data from crawl OR forced taxonomy
    if args.force_taxonomy or (not updates or len([k for k in updates.keys() if k in ("Company Phone", "Company Linkedin Url", "Facebook Url", "Instagram Url", "YouTube Url", "TikTok Url", "Twitter/X Url", "Street Address", "City", "Postal Code", "Country/Region", "Email")]) < 2):
        print("[2/4] perplexity: fallback")
        px_updates = perplexity_lookup(website, verbose=args.verbose, stats=stats)
        for k, v in px_updates.items():
            updates.setdefault(k, v)

    # optional Google Maps verification under budget
    if args.verify_with_maps and (os.getenv("BROADWAY_GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_MAPS_API_KEY")):
        remaining = max(0, args.maps_max_calls)
        if remaining > 0:
            print("[3/4] maps: verify")
            maps_updates = maps_lookup(row.get("Company Name", ""), row, verbose=args.verbose, stats=stats)
            for k, v in maps_updates.items():
                # prefer explicit maps verified phone/address under separate fields
                updates.setdefault(k, v)

    # Staged Perplexity: if missing/generic email, first identify primary contact, then lookup email
    first = (row.get("First Name") or "").strip()
    last = (row.get("Last Name") or "").strip()
    company = (row.get("Company Name") or "").strip()
    if not company:
        company = derive_company_from_website(website)
    need_email = (not updates.get("Email") or is_generic_email(updates.get("Email")))
    if need_email and company:
        if not (first or last):
            print("[4/4] perplexity: find primary contact")
            name_updates = perplexity_find_primary_contact(company, website, verbose=args.verbose, stats=stats)
            if name_updates.get("First Name"):
                first = name_updates["First Name"]
                updates.setdefault("First Name", first)
            if name_updates.get("Last Name"):
                last = name_updates["Last Name"]
                updates.setdefault("Last Name", last)
            if name_updates.get("Job Title"):
                updates.setdefault("Job Title", name_updates["Job Title"])
            if name_updates.get("Source Verified URL"):
                updates.setdefault("Source Verified URL", name_updates["Source Verified URL"]) 
        if (first or last):
            print("[4/4] perplexity: person email")
            px_person = perplexity_person_email_lookup(first, last, company, website, verbose=args.verbose, stats=stats)
            direct = px_person.get("Direct Email")
            if direct:
                # Always use direct email when found, replacing any generic one
                updates["Email"] = direct
                updates["Email Status"] = "found_direct"
                updates["Email Confidence"] = px_person.get("Email Confidence", "high")
                if px_person.get("Source Verified URL"):
                    updates.setdefault("Source Verified URL", px_person["Source Verified URL"])

    if args.show_usage:
        costs = estimate_costs(stats)
        print(json.dumps({"debug": "usage", "stats": stats.to_dict(), "costs": costs}, indent=2))
    if not updates:
        print("No new contact details discovered")
        sys.exit(0)

    # Get direct email from updates and add to enrichment data for database persistence
    if updates.get("Email") and updates.get("Email Status") == "found_direct":
        direct_email = updates["Email"]
        enrichment_data = {
            'emails': [direct_email]
        }
        persist_enrichment_to_db(args.id, enrichment_data)
    
    # Update CSV if requested
    if args.update_csv:
        update_row_in_csv(args.csv, args.id, updates)
    
    print(json.dumps({"contact_id": args.id, "updates": updates}, indent=2))


if __name__ == "__main__":
    main()


