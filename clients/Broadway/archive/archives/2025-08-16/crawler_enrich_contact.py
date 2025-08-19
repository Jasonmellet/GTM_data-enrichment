import argparse
import csv
import datetime
import json
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os


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
        self.maps_findplace_calls: int = 0
        self.maps_details_calls: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "http_fetches": self.http_fetches,
            "pages_rendered": self.pages_rendered,
            "sitemaps_discovered": self.sitemaps_discovered,
            "perplexity_calls": self.perplexity_calls,
            "maps_findplace_calls": self.maps_findplace_calls,
            "maps_details_calls": self.maps_details_calls,
        }


def estimate_costs(stats: UsageStats) -> Dict[str, float]:
    # Defaults can be overridden by env vars
    # Google Places: ~$17 per 1000 calls → $0.017 per call (approx)
    maps_unit_cost = float(os.getenv("MAPS_EST_COST_PER_CALL", "0.017"))
    # Perplexity: unknown; placeholder $0.005 per call unless overridden
    px_unit_cost = float(os.getenv("PERPLEXITY_EST_COST_PER_CALL", "0.005"))
    return {
        "maps_cost_est_usd": round(maps_unit_cost * (stats.maps_findplace_calls + stats.maps_details_calls), 4),
        "perplexity_cost_est_usd": round(px_unit_cost * stats.perplexity_calls, 4),
        "total_cost_est_usd": round(maps_unit_cost * (stats.maps_findplace_calls + stats.maps_details_calls) + px_unit_cost * stats.perplexity_calls, 4),
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


def update_row_in_csv(csv_path: str, contact_id: int, updates: Dict[str, str]) -> None:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    # find by Contact id column (string match)
    target_index = None
    for i, row in enumerate(rows):
        if str(row.get("Contact id", "")).strip() == str(contact_id):
            target_index = i
            break
    if target_index is None:
        raise ValueError(f"Contact id {contact_id} not found")

    for k, v in updates.items():
        if k not in fieldnames:
            fieldnames.append(k)
        rows[target_index][k] = v

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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
    today = datetime.date.today().isoformat()
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
        if "Email" in updates and "Email Status" not in updates:
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
                "model": "sonar-small-online",
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
            "fields": "name,formatted_phone_number,formatted_address,website,business_status,place_id",
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

    if args.show_usage:
        costs = estimate_costs(stats)
        print(json.dumps({"debug": "usage", "stats": stats.to_dict(), "costs": costs}, indent=2))
    if not updates:
        print("No new contact details discovered")
        sys.exit(0)

    update_row_in_csv(args.csv, args.id, updates)
    print(json.dumps({"contact_id": args.id, "updates": updates}, indent=2))


if __name__ == "__main__":
    main()


