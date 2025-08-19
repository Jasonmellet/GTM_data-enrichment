-- SQL script to add proper constraints to the silver.emails table
-- This will prevent duplicate emails for the same contact

-- First, create a temporary table to hold unique emails (keeping the highest quality ones)
CREATE TEMP TABLE temp_emails AS
SELECT DISTINCT ON (contact_id, email) 
    email_id,
    org_id,
    contact_id,
    email,
    source,
    verified_at,
    created_at,
    updated_at
FROM silver.emails
ORDER BY contact_id, email, source DESC, updated_at DESC;

-- Drop the existing view that depends on the emails table
DROP VIEW IF EXISTS gold.outreach_today_v CASCADE;

-- Drop the existing table
DROP TABLE IF EXISTS silver.emails CASCADE;

-- Recreate the table with proper constraints
CREATE TABLE silver.emails (
    email_id BIGSERIAL PRIMARY KEY,
    org_id BIGINT REFERENCES silver.organizations(org_id) ON DELETE CASCADE,
    contact_id BIGINT REFERENCES silver.contacts(contact_id) ON DELETE CASCADE,
    email TEXT NOT NULL,
    source TEXT,
    verified_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    -- Add a unique constraint for contact_id + email combination
    UNIQUE(contact_id, email)
);

-- Reinsert the data
INSERT INTO silver.emails (
    email_id,
    org_id,
    contact_id,
    email,
    source,
    verified_at,
    created_at,
    updated_at
)
SELECT 
    email_id,
    org_id,
    contact_id,
    email,
    source,
    verified_at,
    created_at,
    updated_at
FROM temp_emails;

-- Reset the sequence
SELECT setval('silver.emails_email_id_seq', (SELECT MAX(email_id) FROM silver.emails));

-- Create an index for faster lookups
CREATE INDEX IF NOT EXISTS idx_emails_contact_id ON silver.emails(contact_id);
CREATE INDEX IF NOT EXISTS idx_emails_org_id ON silver.emails(org_id);
CREATE INDEX IF NOT EXISTS idx_emails_email ON silver.emails(email);

-- Drop the temporary table
DROP TABLE temp_emails;

-- Log the change
INSERT INTO silver.provenance (
    org_id,
    source,
    method,
    metadata,
    collected_at
)
VALUES (
    NULL,
    'manual',
    'sql',
    '{"action": "Added unique constraint to silver.emails table and deduplicated existing entries"}',
    CURRENT_TIMESTAMP
);

-- Recreate the gold view with improved email prioritization
CREATE OR REPLACE VIEW gold.outreach_today_v AS
SELECT DISTINCT ON (o.org_id)
    o.org_id,
    o.name AS organization_name,
    o.city,
    o.state,
    o.business_status,
    l.maps_verified_address,
    l.maps_verified_phone,
    c.first_name || ' ' || c.last_name AS contact_name,
    c.job_title,
    e.email,
    p.phone_number,
    s.fit_score,
    s.outreach_readiness,
    s.scoring_notes,
    string_agg(DISTINCT cat.label, ', ') AS categories,
    (SELECT label FROM silver.categories WHERE category_id = o.primary_category_id) AS primary_category,
    (SELECT COUNT(*) FROM silver.socials WHERE org_id = o.org_id) AS social_count,
    w.status AS website_status,
    o.last_enriched_at
FROM 
    silver.organizations o
LEFT JOIN 
    silver.contacts c ON o.org_id = c.org_id
LEFT JOIN 
    silver.emails e ON c.contact_id = e.contact_id
LEFT JOIN 
    silver.phones p ON c.contact_id = p.contact_id
LEFT JOIN 
    silver.locations l ON o.org_id = l.org_id
LEFT JOIN 
    silver.websites w ON o.org_id = w.org_id
LEFT JOIN 
    silver.scoring s ON o.org_id = s.org_id
LEFT JOIN 
    silver.org_categories oc ON o.org_id = oc.org_id
LEFT JOIN 
    silver.categories cat ON oc.category_id = cat.category_id
WHERE 
    e.email IS NOT NULL OR p.phone_number IS NOT NULL
GROUP BY 
    o.org_id, o.name, o.city, o.state, o.business_status, 
    l.maps_verified_address, l.maps_verified_phone,
    c.first_name, c.last_name, c.job_title,
    e.email, p.phone_number, 
    s.fit_score, s.outreach_readiness, s.scoring_notes,
    o.primary_category_id, w.status, o.last_enriched_at
ORDER BY 
    o.org_id,
    -- Prioritize rows with direct emails (non-generic)
    CASE WHEN e.email NOT LIKE 'info@%' AND 
              e.email NOT LIKE 'contact@%' AND 
              e.email NOT LIKE 'hello@%' AND 
              e.email NOT LIKE 'support@%' THEN 1 
         ELSE 0 
    END DESC,
    -- Then prioritize any email
    CASE WHEN e.email IS NOT NULL THEN 1 ELSE 0 END DESC,
    -- Then prioritize phone
    CASE WHEN p.phone_number IS NOT NULL THEN 1 ELSE 0 END DESC,
    -- Finally prioritize by outreach readiness
    s.outreach_readiness DESC;