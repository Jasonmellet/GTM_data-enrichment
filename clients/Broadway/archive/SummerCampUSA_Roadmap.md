# SummerCampUSA – PostgreSQL Stack Roadmap

Note: This is the end-to-end plan. Some items are not yet implemented in this repo; we will cross‑check and execute step‑by‑step.

## 0) Naming & Architecture at a Glance

- **Database:** `summercamp_usa`
- **Schemas (Medallion style):**
  - `raw` → unmodeled inputs (HTML, JSON, CSV, APIs)
  - `bronze` → parsed/normalized fields, minimal typing
  - `silver` → relational model (orgs/locations/contacts), deduped, enriched
  - `gold` → ready-to-use exports (outreach views, dashboards)
- **Services:** Postgres + pgBouncer, Redis (queues/cache), MinIO/S3 (snapshots), Prefect/Dagster (or Airflow) for orchestration, Metabase/Superset for BI.
- **Repos:** mono-repo with `/ingest`, `/parse`, `/enrich`, `/match`, `/score`, `/export`, `/infra`, `/dbt` (optional), `/tests`.

## 1) Data Model (Silver Layer, 3NF + JSONB for flexible bits)

```sql
-- Core entities
CREATE TABLE silver.organizations (
  org_id            BIGSERIAL PRIMARY KEY,
  legal_name        TEXT NOT NULL,
  display_name      TEXT,
  website_domain    TEXT,            -- tldextract root
  org_type          TEXT,            -- nonprofit, private, public, church-affiliated, franchise
  chain_id          TEXT,            -- e.g., YMCA, JCC, 4-H (nullable)
  ein               TEXT,            -- if nonprofit
  created_at        TIMESTAMPTZ DEFAULT now(),
  updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE silver.locations (
  location_id       BIGSERIAL PRIMARY KEY,
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  name              TEXT,            -- “Camp XYZ at Lakeview”
  street            TEXT,
  city              TEXT,
  region            TEXT,            -- state
  postal_code       TEXT,
  country           TEXT DEFAULT 'US',
  latitude          NUMERIC(9,6),
  longitude         NUMERIC(9,6),
  gmaps_place_id    TEXT,
  business_status   TEXT,            -- open, seasonal, closed
  source_confidence INT,             -- 0-100 geo/address confidence
  created_at        TIMESTAMPTZ DEFAULT now(),
  updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE silver.websites (
  website_id        BIGSERIAL PRIMARY KEY,
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  url               TEXT NOT NULL,
  status_code       INT,
  platform_hint     TEXT,            -- CampMinder, UltraCamp, ACTIVE, Sawyer, custom, etc.
  last_crawled_at   TIMESTAMPTZ,
  js_rendered       BOOLEAN,
  metadata          JSONB            -- title, meta, JSON-LD extracted
);

CREATE TABLE silver.contacts (
  contact_id        BIGSERIAL PRIMARY KEY,
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  location_id       BIGINT REFERENCES silver.locations(location_id),
  full_name         TEXT,
  first_name        TEXT,
  last_name         TEXT,
  role_title        TEXT,
  role_normalized   TEXT,            -- enum-like: camp_director, registrar, program_dir, comms, etc.
  linkedin_url      TEXT,
  last_seen_at      TIMESTAMPTZ,
  created_at        TIMESTAMPTZ DEFAULT now(),
  updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE silver.emails (
  email_id          BIGSERIAL PRIMARY KEY,
  contact_id        BIGINT REFERENCES silver.contacts(contact_id),
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  email             TEXT,
  source_url        TEXT,
  is_role_account   BOOLEAN,         -- info@, registrar@, etc.
  verification      TEXT,            -- deliverable, risky, catch-all, unknown
  mx_ok             BOOLEAN,
  dmarc_present     BOOLEAN,
  spf_present       BOOLEAN,
  last_verified_at  TIMESTAMPTZ
);

CREATE TABLE silver.phones (
  phone_id          BIGSERIAL PRIMARY KEY,
  contact_id        BIGINT REFERENCES silver.contacts(contact_id),
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  phone_e164        TEXT,
  type              TEXT,            -- main, admissions, director, mobile
  source_url        TEXT,
  last_verified_at  TIMESTAMPTZ
);

CREATE TABLE silver.socials (
  social_id         BIGSERIAL PRIMARY KEY,
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  platform          TEXT,            -- facebook, instagram, youtube, tiktok, x
  handle            TEXT,
  url               TEXT,
  followers         INT,
  last_seen_at      TIMESTAMPTZ
);

CREATE TABLE silver.categories (
  category_id       BIGSERIAL PRIMARY KEY,
  slug              TEXT UNIQUE,     -- e.g., musical-theater, robotics
  label             TEXT
);

CREATE TABLE silver.org_categories (
  org_id            BIGINT REFERENCES silver.organizations(org_id),
  category_id       BIGINT REFERENCES silver.categories(category_id),
  confidence        INT,
  PRIMARY KEY (org_id, category_id)
);

CREATE TABLE silver.provenance (
  prov_id           BIGSERIAL PRIMARY KEY,
  org_id            BIGINT,
  location_id       BIGINT,
  contact_id        BIGINT,
  field_name        TEXT,
  value_hash        TEXT,
  source_url        TEXT,
  snapshot_uri      TEXT,            -- s3/minio path to HTML/PNG
  captured_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE silver.scoring (
  org_id            BIGINT PRIMARY KEY REFERENCES silver.organizations(org_id),
  fit_score         INT,             -- 0-100 (your “who we want”)
  fit_reason        TEXT,
  outreach_score    INT,             -- 0-100 (data completeness to safely email today)
  outreach_blocker  TEXT,            -- e.g., catch-all, form-only, no staff listed
  last_scored_at    TIMESTAMPTZ
);

CREATE TABLE silver.suppressions (
  suppression_id    BIGSERIAL PRIMARY KEY,
  org_id            BIGINT,
  contact_id        BIGINT,
  email             TEXT,
  reason            TEXT,            -- opt-out, hard bounce, complaint
  source            TEXT,
  suppressed_at     TIMESTAMPTZ DEFAULT now()
);

-- Helpful indexes
CREATE INDEX ON silver.organizations (lower(legal_name));
CREATE INDEX ON silver.organizations (website_domain);
CREATE INDEX ON silver.locations (gmaps_place_id);
CREATE INDEX ON silver.emails (lower(email));
CREATE INDEX ON silver.contacts (role_normalized);
```

**Gold views (examples):**

- `gold.outreach_today_v`: best contact per org with verified email + phone + role; ready for sequencing.
- `gold.verification_queue_v`: candidates needing re-verification (≥180 days old or new domains).
- `gold.coverage_by_state_v`: KPIs for ops.

## 2) Ingestion → Parsing → Enrichment (Pipeline Overview)

**Phase A — Seeding**

- Inputs: ACA directory, state licensing lists, parks & rec catalogs, school district enrichment, faith networks (dioceses), Boy/Girl Scouts/4-H councils, university youth programs, aggregator directories (for discovery only).
- Normalize seeds into `raw.seeds` with minimally: `name, raw_website, street, city, state, zip, notes, source_url`.

**Phase B — Crawl & Render**

- Fetch homepage + `/about`, `/contact`, `/team`, `/staff`, `/leadership`, sitemap(s).
- Heuristic render (Playwright) only if:
  - < 1,500 visible chars, or
  - > 30% links are `#`/javascript:, or
  - `noscript` mentions JS required.
- Extract: emails, phones, people blocks, JSON-LD (`Organization`, `Place`, `PostalAddress`, `sameAs`), platform fingerprints (CampMinder/UltraCamp/ACTIVE/Sawyer), social links.

**Phase C — Maps/Geo Binding**

- Resolve address with Google Places (or OSM fallback) → get `gmaps_place_id`, lat/long, status.
- Bind location to org by Place ID; if multiple sites, create additional `locations` rows.

**Phase D — People & Email**

- Role lexicon targeting high-value titles:
  - `camp_director`, `registrar`, `program_director`, `youth_programs_director`, `marketing_comms`, `enrollment_admissions`, `site_director`
- Email finding path:
  1. On-site mailto and plain-text.
  2. Format inference from domain patterns (fn.ln@, first@, role@).
  3. Verification pass (NeverBounce/ZeroBounce/DeBounce or in-house SMTP check).
  4. MX/DNS hygiene (MX present, SPF/DMARC presence flags).

**Phase E — Social & Platform Enrichment**

- Pull FB/IG/YT/TikTok handles from `sameAs` and page footers; capture follower counts when available.
- Platform hints:
  - `CampMinder`/`UltraCamp`/`ACTIVE` URLs, CSS/JS signatures → scrape their contact/FAQ/staff PDFs where allowed.

**Phase F — Nonprofit & Legal**

- If EIN known or likely nonprofit: look up IRS 990 (officers/directors names), store in `contacts` with `role_normalized = board/officer` and low outreach priority.

## 3) Entity Resolution & De-dupe (Bronze→Silver)

**Matching keys:**

1. **Org key:** `website_domain` + fuzzy `legal_name` (Jaro-Winkler/RapidFuzz threshold ~0.92)
2. **Location key:** `gmaps_place_id` (primary); fallback on `(street, city, state, postal)` normalized via libpostal
3. **Person key:** `(lower(full_name), org_id)` or authoritative profile URL

**Rules:**

- If same Place ID across different names → one org, multiple aliases (store alias in `organizations_alias` if useful).
- Chain handling: set `chain_id` for YMCA/JCC/4-H etc.; treat each branch as separate location under the same chain.

## 4) Scoring (Fit + Outreach Readiness)

**Fit Score (0–100)** — “Should we sell to them?”

- Category fit (weighted by your ICP) … 35
- Program breadth (multi-program vs single) … 10
- Audience served (K-12, teen leadership, theater/arts inclination) … 15
- Location viability (state target, density) … 10
- Brand signals (active socials, recent updates) … 10
- Chain influence (multipliers for network buys) … 10
- Historical outcomes (reply/meeting/buy history) … 10

**Outreach-Readiness (0–100)** — “Can we email today?”

- Verified named email (deliverable) … 35
- Role mailbox (registrar@ etc.) if no named … 15
- MX present + non-catch-all … 10
- Work phone present … 10
... (truncated for brevity in this preview)

```
This document continues with sections 5–15 as provided, including orchestration, exports, crawler enhancements, QA/observability, compliance, infrastructure, timeline/milestones, repo structure, role lexicon, export columns, and immediate next steps.
```
