CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.organizations (
  org_id BIGSERIAL PRIMARY KEY,
  legal_name TEXT NOT NULL,
  display_name TEXT,
  website_domain TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.locations (
  location_id BIGSERIAL PRIMARY KEY,
  org_id BIGINT REFERENCES silver.organizations(org_id),
  street TEXT, city TEXT, region TEXT, postal_code TEXT, country TEXT DEFAULT 'US',
  gmaps_place_id TEXT,
  business_status TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.contacts (
  contact_id BIGSERIAL PRIMARY KEY,
  org_id BIGINT REFERENCES silver.organizations(org_id),
  full_name TEXT,
  first_name TEXT,
  last_name TEXT,
  role_title TEXT,
  linkedin_url TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.emails (
  email_id BIGSERIAL PRIMARY KEY,
  contact_id BIGINT REFERENCES silver.contacts(contact_id),
  org_id BIGINT REFERENCES silver.organizations(org_id),
  email TEXT,
  source_url TEXT,
  verification TEXT,
  last_verified_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver.phones (
  phone_id BIGSERIAL PRIMARY KEY,
  contact_id BIGINT REFERENCES silver.contacts(contact_id),
  org_id BIGINT REFERENCES silver.organizations(org_id),
  phone_e164 TEXT,
  type TEXT,
  source_url TEXT,
  last_verified_at TIMESTAMPTZ
);

CREATE OR REPLACE VIEW gold.outreach_today_v AS
SELECT o.org_id,
       COALESCE(o.display_name, o.legal_name) AS display_name,
       l.city, l.region AS state,
       c.full_name, c.role_title,
       e.email,
       p.phone_e164
FROM silver.organizations o
LEFT JOIN silver.locations l ON l.org_id = o.org_id
LEFT JOIN silver.contacts c ON c.org_id = o.org_id
LEFT JOIN silver.emails e ON e.org_id = o.org_id AND e.contact_id = c.contact_id
LEFT JOIN silver.phones p ON p.org_id = o.org_id AND p.contact_id = c.contact_id;

CREATE INDEX IF NOT EXISTS idx_org_name ON silver.organizations (lower(legal_name));
CREATE INDEX IF NOT EXISTS idx_email_lower ON silver.emails (lower(email));
