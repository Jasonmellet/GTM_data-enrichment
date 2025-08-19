#!/usr/bin/env python3
"""
Update scoring logic to incorporate new enrichment data.
Version 3: Includes crawler data, Maps verification, and business status.
"""

import os
import sys
import psycopg
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

def update_scoring_v3():
    """Update scoring with new enrichment data."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                print("🔄 Updating scoring with enrichment data...")
                
                # Update scoring for all organizations
                cur.execute("""
                    UPDATE silver.scoring 
                    SET 
                        fit_score = subquery.fit_score,
                        outreach_readiness = subquery.outreach_readiness,
                        scoring_notes = subquery.scoring_notes,
                        updated_at = now()
                    FROM (
                        SELECT 
                            o.org_id,
                            -- Fit Score: Base 50, +10 for each category, -20 for closed businesses
                            CASE 
                                WHEN l.business_status = 'closed_permanently' THEN 0
                                WHEN l.business_status = 'possibly_closed' THEN 10
                                WHEN l.business_status = 'closed_temporarily' THEN 20
                                ELSE 50 + COALESCE(cat_count.cat_count * 10, 0)
                            END as fit_score,
                            
                            -- Outreach Readiness: Heavily penalize generic emails, boost direct emails
                            CASE 
                                WHEN l.business_status = 'closed_permanently' THEN 0
                                WHEN l.business_status = 'possibly_closed' THEN 5
                                WHEN l.business_status = 'closed_temporarily' THEN 10
                                -- Direct email (not generic) + phone + maps verification = highest score
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id AND e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%')
                                     AND EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id)
                                     AND (l.maps_verified_phone = true OR l.maps_verified_address = true) THEN 85
                                -- Direct email + phone = high score
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id AND e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%')
                                     AND EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id) THEN 70
                                -- Generic email + phone + maps verification = moderate score
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id)
                                     AND EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id)
                                     AND (l.maps_verified_phone = true OR l.maps_verified_address = true) THEN 50
                                -- Generic email + phone = lower score
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id)
                                     AND EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id) THEN 35
                                -- Direct email only = still useful
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id AND e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%') THEN 30
                                -- Generic email only = much less useful
                                WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id) THEN 15
                                -- Phone only = limited value
                                WHEN EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id) THEN 10
                                ELSE 0
                            END as outreach_readiness,
                            
                            -- Scoring notes
                            CASE 
                                WHEN l.business_status = 'closed_permanently' THEN 'Business permanently closed'
                                WHEN l.business_status = 'possibly_closed' THEN 'Business possibly closed (placeholder site)'
                                WHEN l.business_status = 'closed_temporarily' THEN 'Business temporarily closed'
                                ELSE CONCAT(
                                    'Categories: ', COALESCE(cat_count.cat_count, 0), 
                                    ', Email: ', CASE 
                                        WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id AND e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%') THEN 'Direct'
                                        WHEN EXISTS(SELECT 1 FROM silver.emails e WHERE e.org_id = o.org_id) THEN 'Generic'
                                        ELSE 'No' END,
                                    ', Phone: ', CASE WHEN EXISTS(SELECT 1 FROM silver.phones p WHERE p.org_id = o.org_id) THEN 'Yes' ELSE 'No' END,
                                    ', Maps Verified: ', CASE WHEN l.maps_verified_phone = true OR l.maps_verified_address = true THEN 'Yes' ELSE 'No' END
                                )
                            END as scoring_notes
                        FROM silver.organizations o
                        LEFT JOIN silver.locations l ON l.org_id = o.org_id
                        LEFT JOIN (
                            SELECT org_id, COUNT(*) as cat_count 
                            FROM silver.org_categories 
                            GROUP BY org_id
                        ) cat_count ON cat_count.org_id = o.org_id
                    ) subquery
                    WHERE silver.scoring.org_id = subquery.org_id
                """)
                
                updated_count = cur.rowcount
                print(f"✅ Updated scoring for {updated_count} organizations")
                
                # Update the gold view
                cur.execute("""
                    CREATE OR REPLACE VIEW gold.outreach_today_v AS
                    WITH best_email AS (
                        SELECT 
                            e.org_id,
                            e.contact_id,
                            e.email,
                            ROW_NUMBER() OVER (
                                PARTITION BY e.org_id, e.contact_id
                                ORDER BY
                                    -- Prefer direct emails over generic ones
                                    CASE WHEN e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%' THEN 0 ELSE 1 END,
                                    -- Then by verification date (newest first)
                                    e.verified_at DESC NULLS LAST
                            ) as rn
                        FROM silver.emails e
                    ),
                    best_contact AS (
                        SELECT 
                            c.org_id,
                            c.contact_id,
                            c.full_name,
                            c.role_title,
                            ROW_NUMBER() OVER (
                                PARTITION BY c.org_id
                                ORDER BY
                                    -- prefer rows that have a direct email
                                    (CASE WHEN be.email IS NOT NULL AND be.email NOT LIKE 'info@%' AND be.email NOT LIKE 'contact@%' AND be.email NOT LIKE 'hello@%' THEN 1 ELSE 0 END) DESC,
                                    -- then prefer rows that have any email
                                    (CASE WHEN e.email IS NOT NULL AND e.email <> '' THEN 1 ELSE 0 END) DESC,
                                    -- then prefer rows that have a phone
                                    (CASE WHEN p.phone_e164 IS NOT NULL AND p.phone_e164 <> '' THEN 1 ELSE 0 END) DESC,
                                    -- then highest readiness and fit
                                    COALESCE(s.outreach_readiness, 0) DESC,
                                    COALESCE(s.fit_score, 0) DESC,
                                    c.contact_id ASC
                            ) AS rn
                        FROM silver.contacts c
                        LEFT JOIN best_email be ON be.org_id = c.org_id AND be.contact_id = c.contact_id AND be.rn = 1
                        LEFT JOIN silver.emails e ON e.org_id = c.org_id AND e.contact_id = c.contact_id
                        LEFT JOIN silver.phones p ON p.org_id = c.org_id AND p.contact_id = c.contact_id
                        LEFT JOIN silver.scoring s ON s.org_id = c.org_id
                    )
                    SELECT DISTINCT ON (o.org_id)
                        o.org_id,
                        COALESCE(o.display_name, o.legal_name) AS display_name,
                        l.city, l.region AS state,
                        l.business_status,
                        l.maps_verified_phone,
                        l.maps_verified_address,
                        bc.full_name, bc.role_title,
                        be.email,
                        p.phone_e164,
                        s.fit_score,
                        s.outreach_readiness,
                        s.scoring_notes,
                        -- Categories
                        (SELECT STRING_AGG(cat.label, ', ' ORDER BY cat.label) 
                         FROM silver.org_categories oc 
                         JOIN silver.categories cat ON cat.category_id = oc.category_id 
                         WHERE oc.org_id = o.org_id) as categories,
                        -- Primary category
                        (SELECT cat.label 
                         FROM silver.org_categories oc 
                         JOIN silver.categories cat ON cat.category_id = oc.category_id 
                         WHERE oc.org_id = o.org_id 
                         ORDER BY oc.category_id 
                         LIMIT 1) as primary_category,
                        -- Social media presence
                        (SELECT COUNT(*) FROM silver.socials WHERE org_id = o.org_id) as social_count,
                        -- Website status
                        w.status_code as website_status,
                        -- Last enrichment
                        GREATEST(
                            COALESCE(w.last_crawled_at, '1900-01-01'::timestamptz),
                            COALESCE(l.updated_at, '1900-01-01'::timestamptz),
                            COALESCE(s.updated_at, '1900-01-01'::timestamptz)
                        ) as last_enriched_at
                    FROM silver.organizations o
                    LEFT JOIN silver.locations l ON l.org_id = o.org_id
                    LEFT JOIN silver.scoring s ON s.org_id = o.org_id
                    LEFT JOIN best_contact bc ON bc.org_id = o.org_id AND bc.rn = 1
                    LEFT JOIN best_email be ON be.org_id = o.org_id AND be.contact_id = bc.contact_id AND be.rn = 1
                    LEFT JOIN silver.phones p ON p.org_id = o.org_id AND p.contact_id = bc.contact_id
                    LEFT JOIN silver.websites w ON w.org_id = o.org_id
                    WHERE s.outreach_readiness >= 25  -- Show contacts with basic readiness
                    ORDER BY o.org_id, s.outreach_readiness DESC, s.fit_score DESC
                """)
                
                print("✅ Updated gold.outreach_today_v view")
                
                # Export the updated outreach list
                cur.execute("SELECT * FROM gold.outreach_today_v")
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                
                # Write to CSV
                import csv
                output_path = "clients/Broadway/outputs/outreach_pilot_v3.csv"
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)
                
                print(f"✅ Exported {len(rows)} outreach-ready contacts to {output_path}")
                
                # Show summary
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_orgs,
                        COUNT(CASE WHEN s.outreach_readiness >= 50 THEN 1 END) as outreach_ready,
                        COUNT(CASE WHEN s.fit_score >= 70 THEN 1 END) as high_fit,
                        AVG(s.outreach_readiness) as avg_readiness,
                        AVG(s.fit_score) as avg_fit
                    FROM silver.organizations o
                    LEFT JOIN silver.scoring s ON s.org_id = o.org_id
                """)
                
                summary = cur.fetchone()
                print(f"\n📊 Scoring Summary:")
                print(f"  Total Organizations: {summary[0]}")
                print(f"  Outreach Ready (≥50): {summary[1]}")
                print(f"  High Fit (≥70): {summary[2]}")
                print(f"  Avg Outreach Readiness: {summary[3]:.1f}")
                print(f"  Avg Fit Score: {summary[4]:.1f}")
                
                conn.commit()
                return True
                
    except Exception as e:
        print(f"❌ Error updating scoring: {e}")
        return False

if __name__ == "__main__":
    success = update_scoring_v3()
    if success:
        print("🎉 Scoring update completed successfully!")
    else:
        print("❌ Scoring update failed!")
