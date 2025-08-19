#!/usr/bin/env python3
"""
Identify contacts missing direct emails and generate a prioritized list for enrichment.
"""

import os
import sys
import csv
import pandas as pd
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

def is_generic_email(email):
    """Check if an email is generic (info@, contact@, etc.)."""
    if not email or not isinstance(email, str):
        return False
    
    generic_prefixes = [
        "info@", "contact@", "hello@", "admin@", "office@", 
        "support@", "sales@", "help@", "mail@", "enquiry@",
        "enquiries@", "general@", "webmaster@", "customerservice@"
    ]
    
    email = email.lower()
    return any(email.startswith(prefix) for prefix in generic_prefixes)

def analyze_emails():
    """Analyze the database for contacts missing direct emails."""
    try:
        # Load data from database
        with get_db_connection() as conn:
            # Get all organizations and their emails
            df = pd.read_sql("""
                SELECT 
                    o.org_id,
                    o.legal_name,
                    o.display_name,
                    o.website_domain,
                    c.contact_id,
                    c.full_name,
                    c.role_title,
                    e.email,
                    l.business_status,
                    w.url as website_url,
                    w.status_code as website_status,
                    s.fit_score,
                    s.outreach_readiness
                FROM silver.organizations o
                LEFT JOIN silver.contacts c ON c.org_id = o.org_id
                LEFT JOIN silver.emails e ON e.org_id = o.org_id AND e.contact_id = c.contact_id
                LEFT JOIN silver.locations l ON l.org_id = o.org_id
                LEFT JOIN silver.websites w ON w.org_id = o.org_id
                LEFT JOIN silver.scoring s ON s.org_id = o.org_id
                ORDER BY o.org_id, c.contact_id
            """, conn)
            
            # Get CSV contact mapping
            csv_map_df = pd.read_sql("""
                SELECT csv_contact_id, org_id, contact_id 
                FROM silver.csv_contact_map
            """, conn)
        
        # Merge with CSV contact IDs
        df = pd.merge(df, csv_map_df, on=['org_id', 'contact_id'], how='left')
        
        # Categorize emails
        df['email_status'] = 'missing'
        df.loc[~df['email'].isna() & (df['email'] != ''), 'email_status'] = 'generic'
        df.loc[~df['email'].isna() & (df['email'] != '') & ~df['email'].apply(is_generic_email), 'email_status'] = 'direct'
        
        # Create summary
        total_orgs = df['org_id'].nunique()
        missing_email_orgs = df[df['email_status'] == 'missing']['org_id'].nunique()
        generic_email_orgs = df[df['email_status'] == 'generic']['org_id'].nunique()
        direct_email_orgs = df[df['email_status'] == 'direct']['org_id'].nunique()
        
        # Prioritize contacts for enrichment
        # Priority 1: Active businesses with generic emails
        priority1 = df[
            (df['email_status'] == 'generic') & 
            (df['business_status'] == 'open') &
            (df['website_status'] == 200)
        ]
        
        # Priority 2: Active businesses with missing emails
        priority2 = df[
            (df['email_status'] == 'missing') & 
            (df['business_status'] == 'open') &
            (df['website_status'] == 200)
        ]
        
        # Priority 3: Businesses with generic emails (any status)
        priority3 = df[
            (df['email_status'] == 'generic') & 
            ~((df['business_status'] == 'open') & (df['website_status'] == 200))
        ]
        
        # Combine priorities
        priorities = pd.concat([priority1, priority2, priority3])
        priorities = priorities.drop_duplicates(subset=['org_id'])
        
        # Create output directory
        output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs"))
        os.makedirs(output_dir, exist_ok=True)
        
        # Write summary report
        summary_path = os.path.join(output_dir, "email_status_report.txt")
        with open(summary_path, 'w') as f:
            f.write("EMAIL STATUS REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total Organizations: {total_orgs}\n")
            f.write(f"Organizations with Direct Emails: {direct_email_orgs} ({direct_email_orgs/total_orgs*100:.1f}%)\n")
            f.write(f"Organizations with Generic Emails: {generic_email_orgs} ({generic_email_orgs/total_orgs*100:.1f}%)\n")
            f.write(f"Organizations with Missing Emails: {missing_email_orgs} ({missing_email_orgs/total_orgs*100:.1f}%)\n\n")
            
            f.write("ENRICHMENT PRIORITIES\n")
            f.write("-" * 50 + "\n")
            f.write(f"Priority 1 - Active businesses with generic emails: {len(priority1)}\n")
            f.write(f"Priority 2 - Active businesses with missing emails: {len(priority2)}\n")
            f.write(f"Priority 3 - Other businesses with generic emails: {len(priority3)}\n")
            f.write(f"Total contacts needing enrichment: {len(priorities)}\n")
        
        # Write prioritized contacts CSV
        priorities_path = os.path.join(output_dir, "email_enrichment_priorities.csv")
        priorities.to_csv(priorities_path, index=False)
        
        print(f"✅ Email status report generated: {summary_path}")
        print(f"✅ Enrichment priorities list generated: {priorities_path}")
        
        # Print summary to console
        print("\n📊 EMAIL STATUS SUMMARY:")
        print(f"  Total Organizations: {total_orgs}")
        print(f"  With Direct Emails: {direct_email_orgs} ({direct_email_orgs/total_orgs*100:.1f}%)")
        print(f"  With Generic Emails: {generic_email_orgs} ({generic_email_orgs/total_orgs*100:.1f}%)")
        print(f"  With Missing Emails: {missing_email_orgs} ({missing_email_orgs/total_orgs*100:.1f}%)")
        print(f"\n  Contacts needing enrichment: {len(priorities)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error analyzing emails: {e}")
        return False

if __name__ == "__main__":
    success = analyze_emails()
    if success:
        print("🎉 Email analysis completed successfully!")
    else:
        print("❌ Email analysis failed!")
