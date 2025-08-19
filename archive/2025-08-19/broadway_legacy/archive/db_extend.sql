-- API usage logging
CREATE TABLE IF NOT EXISTS silver.api_usage (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT,
  contact_id_csv INT,
  org_id BIGINT,
  source TEXT,            -- crawler | perplexity | maps | verify_email
  calls INT DEFAULT 0,
  cost_usd NUMERIC(10,4) DEFAULT 0,
  started_at TIMESTAMPTZ DEFAULT now(),
  finished_at TIMESTAMPTZ,
  notes TEXT,
  raw_stats JSONB
);
CREATE INDEX IF NOT EXISTS idx_api_usage_run ON silver.api_usage (run_id);
CREATE INDEX IF NOT EXISTS idx_api_usage_org ON silver.api_usage (org_id);
CREATE INDEX IF NOT EXISTS idx_api_usage_source ON silver.api_usage (source);

-- Categories
CREATE TABLE IF NOT EXISTS silver.categories (
  category_id BIGSERIAL PRIMARY KEY,
  slug TEXT UNIQUE,
  label TEXT
);
CREATE TABLE IF NOT EXISTS silver.org_categories (
  org_id BIGINT REFERENCES silver.organizations(org_id),
  category_id BIGINT REFERENCES silver.categories(category_id),
  confidence INT,
  PRIMARY KEY (org_id, category_id)
);

-- Scoring
CREATE TABLE IF NOT EXISTS silver.scoring (
  org_id BIGINT PRIMARY KEY REFERENCES silver.organizations(org_id),
  fit_score INT,
  fit_reason TEXT,
  outreach_score INT,
  outreach_blocker TEXT,
  last_scored_at TIMESTAMPTZ DEFAULT now()
);

-- Gold helper views (optional later)
