-- SQL query to view comprehensive contact enrichment status
-- This provides a complete overview of contact data quality and enrichment status

SELECT 
    o.org_id,
    o.legal_name AS organization_name,
    
    -- Contact details
    c.contact_id,
    c.full_name AS contact_name,
    c.role_title,
    c.linkedin_url,
    
    -- Email information
    e.email,
    e.is_direct AS is_direct_email,
    e.source AS email_source,
    e.verified_at AS email_verified_at,
    
    -- Phone information
    p.phone_formatted AS phone_number,
    p.source AS phone_source,
    
    -- Location details
    l.city,
    l.region AS state,
    l.business_status,
    l.maps_verified_address,
    l.maps_verified_phone,
    
    -- Website details
    w.url AS website,
    w.status_code AS website_status,
    w.last_crawled_at,
    
    -- Scoring information
    s.fit_score,
    s.outreach_readiness,
    s.scoring_notes,
    
    -- Categories
    string_agg(DISTINCT cat.label, ', ') AS categories,
    
    -- Provenance (most recent)
    (
        SELECT source || ' (' || method || ')' 
        FROM silver.provenance 
        WHERE org_id = o.org_id 
        ORDER BY collected_at DESC 
        LIMIT 1
    ) AS latest_data_source,
    
    -- Last update timestamps
    o.updated_at AS org_updated_at,
    c.updated_at AS contact_updated_at,
    e.updated_at AS email_updated_at,
    
    -- Social media count
    (SELECT COUNT(*) FROM silver.socials WHERE org_id = o.org_id) AS social_count
    
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
GROUP BY 
    o.org_id, o.legal_name, 
    c.contact_id, c.full_name, c.role_title, c.linkedin_url,
    e.email, e.is_direct, e.source, e.verified_at,
    p.phone_formatted, p.source,
    l.city, l.region, l.business_status, l.maps_verified_address, l.maps_verified_phone,
    w.url, w.status_code, w.last_crawled_at,
    s.fit_score, s.outreach_readiness, s.scoring_notes,
    o.updated_at, c.updated_at, e.updated_at
ORDER BY 
    -- Prioritize rows with direct emails
    CASE WHEN e.is_direct = true THEN 1 ELSE 0 END DESC,
    -- Then prioritize any email
    CASE WHEN e.email IS NOT NULL THEN 1 ELSE 0 END DESC,
    -- Then by organization name
    o.legal_name;
