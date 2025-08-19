## How to Build a Website Crawler (Production-Ready)

This guide distills practical patterns from the crawlers in this repo, with `CRAWLER/Primary_Scripts/seo_crawler_3.2.py` as the most comprehensive reference. It covers architecture, etiquette, JavaScript rendering, sitemap discovery, content/SEO extraction, PageSpeed Insights integration, SPA handling, reporting, and testing.

### 1) Goals and Non-Goals
- **Goal**: Reliably fetch pages, respect sites, extract structured SEO+content data, and produce actionable reports.
- **Non-goals**: Full-browser scraping at scale without consent; bypassing bot protections.

### 2) Prerequisites
- **Python** 3.10+
- **Packages**: `requests`, `beautifulsoup4`, `selenium`, `playwright`, `python-dotenv` (and others listed in `requirements.txt`)
- **Optional**: Chrome + ChromeDriver (for Selenium), Playwright Chromium (`playwright install chromium`)
- **APIs**: Optional PageSpeed Insights key (`PAGESPEED_INSIGHTS_API_KEY` in `.env`)

### 3) Crawler Etiquette and Safety
- **robots.txt**: Parse and honor `Disallow` and `Crawl-delay`.
- **User-Agent**: Use a descriptive UA string with contact URL.
- **Rate limits**: Add delays; also respect robots `crawl-delay` if present.
- **Retries**: Backoff on `429` and 5xx; honor `Retry-After`.
- **HTTPS**: Prefer HTTPS; verify TLS when possible.

### 4) High-Level Architecture
- **Fetching layer**: `requests.Session` with retry/backoff.
- **Discovery layer**: Sitemaps + shallow page crawl as fallback.
- **Rendering layer**: Prefer Playwright; fallback to Selenium; finally requests-only.
- **Extraction layer**: Parse HTML with BeautifulSoup for meta, schema, headings, links, images, content.
- **Enrichment**: Optional PageSpeed Insights per URL (budgeted).
- **Analysis**: Create issue punch list + score.
- **Persistence**: Write human TXT and compact JSON reports.

### 5) Reliable HTTP Sessions
- Use a single `requests.Session` with `urllib3.Retry` for resilience (429/5xx). Keep headers consistent.

```python
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests

def build_retrying_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "HEAD"],
                  respect_retry_after_header=True)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
```

### 6) robots.txt and Crawl-Delay
- Fetch `/robots.txt`; use `RobotFileParser`.
- Compute an effective delay: `max(custom_delay, robots_crawl_delay)`.

### 7) URL Normalization
- Normalize scheme to HTTPS; lowercase host; ensure trailing slash for directories; ignore query/fragment when deduplicating.

### 8) Sitemap Discovery (First-Class)
- Check `robots.txt` for `Sitemap:` lines.
- If missing, try common paths like `sitemap.xml`, `sitemap_index.xml`, `sitemap-posts.xml`, etc.
- Parse both `sitemapindex` and `urlset` formats; recurse indices.

### 9) JavaScript Rendering Strategy
- Try Playwright first (faster, modern). If unavailable, fallback to Selenium. If both fail, proceed requests-only.
- After rendering, use the fully rendered HTML for extraction and link discovery.

### 10) SPA Detection and Virtual Pages
- Heuristics: many words, few traditional links, vendor indicators (e.g., Canva), large dynamic DOM.
- Extract logical content sections and model each as a "virtual page" to analyze content blocks individually when navigation is JS-driven.

### 11) Page Extraction Checklist
- **Meta**: title, description, keywords, robots, canonical.
- **Open Graph / Twitter Cards**.
- **Language**: `<html lang>` and related meta.
- **Viewport**: detect mobile-friendly meta.
- **Schema**: parse JSON-LD; collect `@type`s; capture weak spots (short description, http images/logos).
- **Headings**: `h1`..`h6` lists.
- **Links**: internal/external, nofollow, image links, `javascript:` links; summary counts and detail sample.
- **Images**: count, alt coverage, detail with dimensions.
- **Content**: robust text extraction from `main/article/content` containers; word count and simple keyword frequencies.

### 12) PageSpeed Insights (Optional)
- Query PSI for `mobile` and `desktop`; extract performance score and CWV: LCP, CLS, TTI, Speed Index; store verification links.
- Budget calls (e.g., only top N pages) to keep reports small and within quotas.

### 13) Issue Generation and Scoring
- Generate issues like: missing/duplicate H1, missing/long meta description, missing schema (`Organization`, `WebPage`, `WebSite`), low alt coverage, thin content, low PSI scores, missing canonical/lang.
- Score pages by deducting points based on severity and add small bonuses for best practices.

### 14) Crawl Loop and Queueing
- Seed with sitemap URLs; always ensure homepage present.
- Maintain a queue with `(url, depth)`; dedupe by normalized URL and respect `max_depth`/`max_pages` and a queue cap.
- Extract links from rendered HTML for non-SPA sites; push eligible internal links while obeying robots and limits.
- Emit periodic progress stats.

### 15) Outputs
- **TXT report**: domain summary, sitemap findings, crawled URLs, per-page analysis with LLM readiness, issues grouped by severity, PSI summary, schema issues, image details.
- **JSON report (optimized)**: compact per-page fields with optional PSI (sampled), headings, images summary, schema types, issues, content preview.
- **JSON report (minimal)**: essentials only for very small size.

### 16) Configuration Knobs
- `MAX_PAGES`, `MAX_DEPTH`, `DELAY`, `MAX_QUEUE_SIZE`.
- Toggles: `use_selenium`/`playwright`, `pagespeed_enabled`.
- Headers: UA string; accept, language; TLS verification.

### 17) Error Handling & Resilience
- Retry/backoff on network errors and timeouts; switch UAs on `403`.
- Gracefully fall back (Playwright → Selenium → requests-only).
- Continue crawl even if a page fails; record status and reason.

### 18) Testing
- Unit-test sitemap discovery and page analysis invariants (status code, title/description presence where expected).
- Smoke-test JS rendering environment (`chromedriver --version`, `playwright install chromium`).

### 19) Extensibility Ideas
- Add broken-link verification, canonical loops, hreflang mapping, XML feed discovery.
- Deeper content semantics (entity extraction), image file size checks, CLS/LCP opportunities (from PSI audits).
- Parallelization with polite concurrency + per-host rate limiting.

### 20) Running the Crawler (Interactive)
- Ensure dependencies are installed (see `requirements.txt`).
- Optionally add `.env` with `PAGESPEED_INSIGHTS_API_KEY`.
- Run the v3.2 crawler and follow prompts:

```bash
python CRAWLER/Primary_Scripts/seo_crawler_3.2.py
```

Reports will be written under `CRAWLER/Primary_responses/Seo_crawler_3.2/`.

### 21) Files in This Repo Worth Reviewing
- **Core crawler**: `CRAWLER/Primary_Scripts/seo_crawler_3.2.py`
- **Earlier/variant crawlers**: `CRAWLER/Primary_Scripts/seo_crawler_v3.1.py`, `CRAWLER/seo_crawler_v4.1_group.py`, `CRAWLER/Seo_crawler_v4_solo.py`
- **Sitemap tools/tests**: `CRAWLER/test_sitemap.py`, `CRAWLER/sitemap_audit_crawler.py`
- **PageSpeed helpers/tests**: `CRAWLER/Test_scripts/pagespeed_api_checker.py`, `CRAWLER/Test_scripts/test_pagespeed_api.py`
- **Planning doc**: `CRAWLER/V3.2_UPGRADE_PLAN.md`

Use `seo_crawler_3.2.py` as the canonical template for production features and robustness.


