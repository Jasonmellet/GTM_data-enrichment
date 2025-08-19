#!/usr/bin/env python3
"""
Create a proper test CSV with org_ids for the 10 worst organizations.
This will be used to test the enrichment pipeline.
"""

import pandas as pd
import psycopg
import os
import sys
sys.path.append('../..')
from config import get_db_connection

def create_proper_test_csv():
    """Create a proper test CSV with org_ids for the 10 worst organizations."""
    
    print("🔧 Creating Proper Test CSV with org_ids for Pipeline Testing")
    print("=" * 60)
    
    # Get the 10 worst organizations from the database
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get the 10 worst organizations with all their data
                query = """
                SELECT 
                    o.org_id,
                    o.legal_name,
                    o.website_domain,
                    c.full_name,
                    c.role_title,
                    e.email,
                    p.phone_e164,
                    l.city,
                    l.state,
                    string_agg(cat.label, '; ' ORDER BY cat.label) as categories,
                    s.fit_score,
                    s.outreach_readiness
                FROM silver.organizations o
                LEFT JOIN silver.contacts c ON o.org_id = c.org_id
                LEFT JOIN silver.emails e ON c.contact_id = e.contact_id
                LEFT JOIN silver.phones p ON c.contact_id = p.contact_id
                LEFT JOIN silver.locations l ON o.org_id = l.org_id
                LEFT JOIN silver.org_categories oc ON o.org_id = oc.org_id
                LEFT JOIN silver.categories cat ON oc.category_id = cat.category_id
                LEFT JOIN silver.scoring s ON o.org_id = s.org_id
                WHERE o.legal_name IN (
                    'Saint Mark''s Episopal School',
                    'Tech EdVentures Summer Camps',
                    'Challenger Learning Center of Colorado',
                    'New Jersey 4H Camp',
                    'Catalina Island Camps',
                    'Nittany Gymnastics Academy',
                    'Noor Kids',
                    'Kids in Action',
                    'Kidology Enterprises-Educational Enrichment Program',
                    'Dallas Independent School District'
                )
                GROUP BY o.org_id, o.legal_name, o.website_domain, c.full_name, c.role_title, 
                         e.email, p.phone_e164, l.city, l.state, s.fit_score, s.outreach_readiness
                ORDER BY o.legal_name
                """
                
                cur.execute(query)
                results = cur.fetchall()
                
                print(f"📊 Found {len(results)} records for the 10 worst organizations")
                
                # Create DataFrame
                columns = [
                    'org_id', 'legal_name', 'website_domain', 'full_name', 'role_title',
                    'email', 'phone_e164', 'city', 'state', 'categories', 'fit_score', 'outreach_readiness'
                ]
                
                df = pd.DataFrame(results, columns=columns)
                
                # Fill NaN values with empty strings
                df = df.fillna('')
                
                # Save to CSV
                output_file = "clients/Broadway/outputs/test_10_organizations.csv"
                df.to_csv(output_file, index=False)
                
                print(f"💾 Test CSV saved to: {output_file}")
                
                # Show the data
                print(f"\n📋 TEST ORGANIZATIONS DATA:")
                print("-" * 60)
                
                for i, (_, row) in enumerate(df.iterrows(), 1):
                    org_name = row['legal_name']
                    org_id = row['org_id']
                    contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
                    email = row['email'] if row['email'] != '' else 'No email'
                    phone = row['phone_e164'] if row['phone_e164'] != '' else 'No phone'
                    categories = row['categories'] if row['categories'] != '' else 'No categories'
                    fit_score = row['fit_score'] if row['fit_score'] != '' else 'No fit score'
                    
                    print(f"{i:2d}. {org_name} (ID: {org_id})")
                    print(f"    Contact: {contact_name}")
                    print(f"    Email: {email}")
                    print(f"    Phone: {phone}")
                    print(f"    Categories: {categories}")
                    print(f"    Fit Score: {fit_score}")
                    print()
                
                return df
                
    except Exception as e:
        print(f"❌ Database error: {e}")
        return None

def main():
    try:
        df = create_proper_test_csv()
        
        if df is not None:
            print(f"\n🎯 NEXT STEPS:")
            print(f"   1. Test CSV created with proper org_ids")
            print(f"   2. Now we can run the enrichment pipeline")
            print(f"   3. Pipeline should process these 10 organizations")
            print(f"   4. Check if they move from incomplete to complete")
            
            print(f"\n🎉 Success! Test CSV ready for pipeline testing")
        else:
            print(f"\n❌ Failed to create test CSV")
        
    except Exception as e:
        print(f"❌ Error creating test CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
