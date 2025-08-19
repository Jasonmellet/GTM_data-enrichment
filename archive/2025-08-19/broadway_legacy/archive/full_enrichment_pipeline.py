#!/usr/bin/env python3
"""
Complete enrichment pipeline with all available data sources.
This script orchestrates the entire enrichment process:
1. Web crawling (Broadway_site_crawler_module.py)
2. Perplexity direct email lookup
3. Apollo API fallback for emails
4. Yelp API for business verification
5. Scoring update and export

Usage:
  python3 full_enrichment_pipeline.py --csv "/path/to/csv" --ids 1,2,3
  python3 full_enrichment_pipeline.py --csv "/path/to/csv" --all
"""

import os
import sys
import csv
import argparse
import subprocess
from datetime import datetime
from typing import List, Dict, Any

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

# Default CSV path
CSV_DEFAULT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test.expanded.csv"))

def run_full_pipeline(csv_path: str, contact_ids: List[int], use_apollo: bool = True, use_yelp: bool = True) -> bool:
    """
    Run the full enrichment pipeline for the specified contacts.
    
    Args:
        csv_path: Path to the CSV file
        contact_ids: List of Contact IDs to process
        use_apollo: Whether to use Apollo API as fallback
        use_yelp: Whether to use Yelp API for business verification
        
    Returns:
        True if successful, False otherwise
    """
    print(f"🔄 Running full enrichment pipeline for {len(contact_ids)} contacts...")
    
    # First, run the standard pipeline to upsert contacts and run crawler
    subprocess.run([
        sys.executable,
        os.path.join(os.path.dirname(__file__), "full_pipeline.py"),
        "--csv", csv_path,
        "--ids", ",".join(map(str, contact_ids))
    ], check=False)
    
    # Now run targeted email enrichment for contacts with generic/missing emails
    print("\n🔍 Running targeted email enrichment for contacts with generic/missing emails...")
    
    # Generate the email status report
    subprocess.run([
        sys.executable,
        os.path.join(os.path.dirname(__file__), "identify_missing_emails.py")
    ], check=False)
    
    # Get the list of contacts needing enrichment
    enrichment_priorities = []
    try:
        priorities_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), 
            "../outputs/email_enrichment_priorities.csv"
        ))
        
        with open(priorities_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if int(row['csv_contact_id']) in contact_ids:
                    enrichment_priorities.append(row)
    except Exception as e:
        print(f"❌ Error reading enrichment priorities: {e}")
    
    # Process priority 1 contacts (active businesses with generic emails)
    priority1 = [row for row in enrichment_priorities if 
                row['email_status'] == 'generic' and 
                row['business_status'] == 'open' and 
                float(row['website_status'] or 0) == 200]
    
    for row in priority1:
        print(f"\n📋 Processing priority 1 contact: {row['display_name']}")
        subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), "targeted_email_enrichment.py"),
            "--org-id", row['org_id'],
            "--contact-id", row['contact_id']
        ], check=False)
    
    # Process priority 2 contacts (active businesses with missing emails)
    priority2 = [row for row in enrichment_priorities if 
                row['email_status'] == 'missing' and 
                row['business_status'] == 'open' and 
                float(row['website_status'] or 0) == 200]
    
    for row in priority2:
        print(f"\n📋 Processing priority 2 contact: {row['display_name']}")
        subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), "targeted_email_enrichment.py"),
            "--org-id", row['org_id'],
            "--contact-id", row['contact_id']
        ], check=False)
    
    # Use Apollo API as fallback for contacts still missing emails
    if use_apollo:
        print("\n🔍 Using Apollo API as fallback for contacts still missing direct emails...")
        
        # Re-generate the email status report
        subprocess.run([
            sys.executable,
            os.path.join(os.path.dirname(__file__), "identify_missing_emails.py")
        ], check=False)
        
        # Get the updated list of contacts needing enrichment
        try:
            with open(priorities_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                apollo_candidates = []
                for row in reader:
                    if int(row['csv_contact_id']) in contact_ids:
                        apollo_candidates.append(row)
            
            # Process top 5 candidates with Apollo
            for row in apollo_candidates[:5]:
                print(f"\n📋 Apollo enrichment for: {row['display_name']}")
                
                # Get company domain from website URL
                domain = ""
                if row['website_domain']:
                    domain = row['website_domain']
                
                # Parse first and last name
                first_name = ""
                last_name = ""
                if row['full_name']:
                    name_parts = row['full_name'].split()
                    if len(name_parts) > 0:
                        first_name = name_parts[0]
                        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                
                subprocess.run([
                    sys.executable,
                    os.path.join(os.path.dirname(__file__), "apollo_email_lookup.py"),
                    "--org-id", row['org_id'],
                    "--contact-id", row['contact_id'],
                    "--first-name", first_name,
                    "--last-name", last_name,
                    "--company", row['display_name'] or row['legal_name'],
                    "--domain", domain
                ], check=False)
        except Exception as e:
            print(f"❌ Error running Apollo enrichment: {e}")
    
    # Use Yelp API for business verification
    if use_yelp:
        print("\n🔍 Using Yelp API for business verification...")
        
        # Get contacts with missing location data
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            o.org_id,
                            COALESCE(o.display_name, o.legal_name) as company_name,
                            m.csv_contact_id,
                            l.city,
                            l.region
                        FROM silver.organizations o
                        JOIN silver.csv_contact_map m ON m.org_id = o.org_id
                        LEFT JOIN silver.locations l ON l.org_id = o.org_id
                        WHERE 
                            m.csv_contact_id = ANY(%s) AND
                            (l.city IS NULL OR l.city = '' OR l.region IS NULL OR l.region = '')
                        LIMIT 5
                    """, (contact_ids,))
                    
                    yelp_candidates = cur.fetchall()
            
            # Process candidates with Yelp
            for org_id, company_name, csv_id, city, region in yelp_candidates:
                print(f"\n📋 Yelp enrichment for: {company_name}")
                
                location = ""
                if city and region:
                    location = f"{city}, {region}"
                elif city:
                    location = city
                elif region:
                    location = region
                
                subprocess.run([
                    sys.executable,
                    os.path.join(os.path.dirname(__file__), "yelp_business_lookup.py"),
                    "--org-id", str(org_id),
                    "--company", company_name,
                    "--location", location
                ], check=False)
        except Exception as e:
            print(f"❌ Error running Yelp enrichment: {e}")
    
    # Final scoring update and export
    print("\n🔄 Final scoring update and export...")
    subprocess.run([
        sys.executable,
        os.path.join(os.path.dirname(__file__), "update_scoring_v3.py")
    ], check=False)
    
    subprocess.run([
        sys.executable,
        os.path.join(os.path.dirname(__file__), "export_complete_dataset.py")
    ], check=False)
    
    # Generate null report
    export_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/complete_enriched_dataset.csv"))
    null_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/null_report.csv"))
    
    subprocess.run([
        sys.executable,
        os.path.join(os.path.dirname(__file__), "full_pipeline.py"),
        "--null-report-only",
        "--export-path", export_csv,
        "--null-path", null_csv
    ], check=False)
    
    print("\n✅ Full enrichment pipeline completed!")
    print(f"Export: {export_csv}")
    print(f"Null report: {null_csv}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Complete enrichment pipeline with all data sources")
    parser.add_argument("--csv", default=CSV_DEFAULT, help="Path to expanded CSV")
    parser.add_argument("--ids", default="", help="Comma-separated CSV org_ids to process; blank means none")
    parser.add_argument("--all", action="store_true", help="Process all rows in CSV")
    parser.add_argument("--no-apollo", action="store_true", help="Skip Apollo API enrichment")
    parser.add_argument("--no-yelp", action="store_true", help="Skip Yelp API enrichment")
    args = parser.parse_args()
    
    # Select rows
    target_ids = []
    if args.all:
        with open(args.csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            target_ids = [int(row.get("org_id") or 0) for row in reader if (row.get("website_domain") or "").strip()]
    elif args.ids:
        target_ids = [int(s.strip()) for s in args.ids.split(",") if s.strip()]
    
    if not target_ids:
        print("No targets specified; use --ids or --all")
        sys.exit(1)
    
    run_full_pipeline(
        args.csv,
        target_ids,
        use_apollo=not args.no_apollo,
        use_yelp=not args.no_yelp
    )

if __name__ == "__main__":
    main()
