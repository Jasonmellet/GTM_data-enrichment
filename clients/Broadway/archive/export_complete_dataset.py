#!/usr/bin/env python3
"""
Export complete enriched dataset to CSV spreadsheet.
Combines all data from organizations, contacts, enrichment, and scoring.
"""

import os
import sys
import csv
import psycopg
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

def export_complete_dataset():
    """Export all enriched data to a comprehensive CSV."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                print("🔄 Exporting complete enriched dataset...")
                
                # Get all enriched data with comprehensive joins
                cur.execute("""
                    SELECT 
                        -- Organization Core
                        o.org_id,
                        o.legal_name,
                        o.display_name,
                        o.website_domain,
                        
                        -- Location & Business Status
                        l.street,
                        l.city,
                        l.region as state,
                        l.postal_code,
                        l.country,
                        l.gmaps_place_id,
                        l.business_status,
                        l.maps_verified_phone,
                        l.maps_verified_address,
                        
                        -- Contact Information
                        c.contact_id,
                        c.full_name,
                        c.role_title,
                        
                        -- Email & Phone
                        e.email,
                        e.source as email_source,
                        e.verified_at as email_verified_at,
                        p.phone_e164,
                        p.phone_formatted,
                        p.source as phone_source,
                        p.verified_at as phone_verified_at,
                        
                        -- Website & Social Media
                        w.url as website_url,
                        w.status_code as website_status,
                        w.platform_hint,
                        w.last_crawled_at,
                        w.js_rendered,
                        
                        -- Social Media Counts
                        (SELECT COUNT(*) FROM silver.socials WHERE org_id = o.org_id) as social_media_count,
                        
                        -- Categories
                        (SELECT STRING_AGG(cat.label, '; ' ORDER BY cat.label) 
                         FROM silver.org_categories oc 
                         JOIN silver.categories cat ON cat.category_id = oc.category_id 
                         WHERE oc.org_id = o.org_id) as categories,
                        (SELECT COUNT(*) FROM silver.org_categories WHERE org_id = o.org_id) as category_count,
                        
                        -- Scoring
                        s.fit_score,
                        s.outreach_readiness,
                        s.scoring_notes,
                        s.last_scored_at,
                        
                        -- Enrichment Metadata
                        (SELECT MAX(collected_at) FROM silver.provenance WHERE org_id = o.org_id) as last_enriched_at,
                        (SELECT COUNT(*) FROM silver.api_usage WHERE org_id = o.org_id) as api_calls_made
                        
                    FROM silver.organizations o
                    LEFT JOIN silver.locations l ON l.org_id = o.org_id
                    LEFT JOIN silver.contacts c ON c.org_id = o.org_id
                    -- Prioritize direct emails over generic ones
                    LEFT JOIN LATERAL (
                        SELECT 
                            e.email_id, 
                            e.org_id, 
                            e.contact_id, 
                            e.email, 
                            e.source, 
                            e.verified_at
                        FROM silver.emails e
                        WHERE e.org_id = o.org_id AND e.contact_id = c.contact_id
                        ORDER BY 
                            -- Direct emails first (not generic)
                            CASE WHEN e.email NOT LIKE 'info@%' AND e.email NOT LIKE 'contact@%' AND e.email NOT LIKE 'hello@%' THEN 0 ELSE 1 END,
                            -- Then by verification date (newest first)
                            e.verified_at DESC NULLS LAST
                        LIMIT 1
                    ) e ON true
                    LEFT JOIN silver.phones p ON p.org_id = o.org_id AND p.contact_id = c.contact_id
                    LEFT JOIN silver.websites w ON w.org_id = o.org_id
                    LEFT JOIN silver.scoring s ON s.org_id = o.org_id
                    
                    ORDER BY o.org_id, c.contact_id
                """)
                
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                
                # Create output directory
                output_path = "/Users/jasonmellet/Desktop/AGT_Data_Enrichment/clients/Broadway/outputs/complete_enriched_dataset.csv"
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                
                # Write to CSV
                with open(output_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows(rows)
                
                print(f"✅ Exported {len(rows)} rows to {output_path}")
                
                # Also create a summary report
                summary_path = "/Users/jasonmellet/Desktop/AGT_Data_Enrichment/clients/Broadway/outputs/dataset_summary_report.txt"
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write("COMPLETE ENRICHED DATASET SUMMARY REPORT\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Total Records: {len(rows)}\n\n")
                    
                    # Count unique organizations
                    org_count = len(set(row[0] for row in rows if row[0]))
                    f.write(f"Unique Organizations: {org_count}\n")
                    
                    # Count contacts with emails (email is column 16)
                    email_count = len([row for row in rows if row[16] and str(row[16]).strip()])
                    f.write(f"Contacts with Emails: {email_count}\n")
                    
                    # Count contacts with phones (phone_e164 is column 19)
                    phone_count = len([row for row in rows if row[19] and str(row[19]).strip()])
                    f.write(f"Contacts with Phones: {phone_count}\n")
                    
                    # Count verified businesses (business_status is column 10)
                    open_businesses = len([row for row in rows if row[10] == 'open'])
                    f.write(f"Open Businesses: {open_businesses}\n")
                    
                    # Count high-fit organizations (fit_score is column 31)
                    high_fit = len([row for row in rows if row[31] and str(row[31]).isdigit() and int(row[31]) >= 70])
                    f.write(f"High Fit Organizations (≥70): {high_fit}\n")
                    
                    # Count outreach-ready contacts (outreach_readiness is column 32)
                    outreach_ready = len([row for row in rows if row[32] and str(row[32]).isdigit() and int(row[32]) >= 25])
                    f.write(f"Outreach Ready Contacts (≥25): {outreach_ready}\n")
                    
                    # Calculate averages only for numeric values
                    fit_scores = [int(row[31]) for row in rows if row[31] and str(row[31]).isdigit()]
                    outreach_scores = [int(row[32]) for row in rows if row[32] and str(row[32]).isdigit()]
                    
                    f.write(f"\nAverage Fit Score: {sum(fit_scores) / len(fit_scores):.1f}\n" if fit_scores else "\nAverage Fit Score: N/A\n")
                    f.write(f"Average Outreach Readiness: {sum(outreach_scores) / len(outreach_scores):.1f}\n" if outreach_scores else "\nAverage Outreach Readiness: N/A\n")
                    
                    # Additional insights
                    f.write(f"\nDATASET INSIGHTS:\n")
                    f.write(f"- Organizations with Categories: {len([row for row in rows if row[30] and str(row[30]).isdigit() and int(row[30]) > 0])}\n")
                    f.write(f"- Organizations with Social Media: {len([row for row in rows if row[28] and str(row[28]).isdigit() and int(row[28]) > 0])}\n")
                    f.write(f"- Organizations with Maps Verification: {len([row for row in rows if row[11] or row[12]])}\n")
                    f.write(f"- Total API Calls Made: {sum(int(row[36]) if row[36] and str(row[36]).isdigit() else 0 for row in rows)}\n")
                
                print(f"✅ Created summary report at {summary_path}")
                
                # Show quick stats
                print(f"\n📊 Dataset Summary:")
                print(f"  Total Records: {len(rows)}")
                print(f"  Unique Organizations: {org_count}")
                print(f"  Contacts with Emails: {email_count}")
                print(f"  Contacts with Phones: {phone_count}")
                print(f"  Open Businesses: {open_businesses}")
                print(f"  High Fit (≥70): {high_fit}")
                print(f"  Outreach Ready (≥25): {outreach_ready}")
                
                return True
                
    except Exception as e:
        print(f"❌ Error exporting dataset: {e}")
        return False

if __name__ == "__main__":
    success = export_complete_dataset()
    if success:
        print("🎉 Complete dataset export successful!")
    else:
        print("❌ Dataset export failed!")
