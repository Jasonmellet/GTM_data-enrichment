-- Recreate the gold view with improved email prioritization
CREATE OR REPLACE VIEW gold.outreach_today_v AS
SELECT DISTINCT ON (o.org_id)
    o.org_id,
    o.legal_name AS organization_name,
    l.city,
    l.region AS state,
    l.business_status,
    l.maps_verified_address,
    l.maps_verified_phone,
    c.first_name || ' ' || c.last_name AS contact_name,
    c.role_title,
    e.email,
    p.phone_formatted AS phone_number,
    s.fit_score,
    s.outreach_readiness,
    s.scoring_notes,
    string_agg(DISTINCT cat.label, ', ') AS categories,
    (SELECT label FROM silver.categories WHERE category_id = 
        (SELECT MIN(category_id) FROM silver.org_categories WHERE org_id = o.org_id)
    ) AS primary_category,
    (SELECT COUNT(*) FROM silver.socials WHERE org_id = o.org_id) AS social_count,
    w.status_code AS website_status,
    w.last_crawled_at AS last_enriched_at
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
    e.email IS NOT NULL OR p.phone_formatted IS NOT NULL
GROUP BY 
    o.org_id, o.legal_name, l.city, l.region, l.business_status, 
    l.maps_verified_address, l.maps_verified_phone,
    c.first_name, c.last_name, c.role_title,
    e.email, p.phone_formatted, 
    s.fit_score, s.outreach_readiness, s.scoring_notes,
    w.status_code, w.last_crawled_at
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
    CASE WHEN p.phone_formatted IS NOT NULL THEN 1 ELSE 0 END DESC,
    -- Finally prioritize by outreach readiness
    s.outreach_readiness DESC;