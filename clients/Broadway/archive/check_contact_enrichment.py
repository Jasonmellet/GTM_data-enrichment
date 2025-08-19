#!/usr/bin/env python3
"""
Script to check contact enrichment status in the database.
Provides a comprehensive view of contact data quality and enrichment status.
"""

import os
import sys
import argparse
import csv
import psycopg
from dotenv import load_dotenv
from tabulate import tabulate

# Load environment variables
load_dotenv()

def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    return psycopg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "summer_camps_db"),
        user=os.environ.get("POSTGRES_USER", "summer_camps_user"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
    )

def get_contact_status(org_name=None, contact_id=None, direct_emails_only=False, limit=10):
    """Get the enrichment status of contacts from the database."""
    
    # Base query
    query = """
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
    """
    
    # Add filters
    where_clauses = []
    params = []
    
    if org_name:
        where_clauses.append("o.legal_name ILIKE %s")
        params.append(f"%{org_name}%")
        
    if contact_id:
        where_clauses.append("c.contact_id = %s")
        params.append(contact_id)
        
    if direct_emails_only:
        where_clauses.append("e.is_direct = true")
    
    # Add WHERE clause if there are filters
    if where_clauses:
        query += f" WHERE {' AND '.join(where_clauses)}"
    
    # Add GROUP BY and ORDER BY
    query += """
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
        o.legal_name
    """
    
    # Add LIMIT
    query += f" LIMIT {limit}"
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            results = cur.fetchall()
            
    return columns, results

def calculate_enrichment_summary(results):
    """Calculate summary statistics for the enrichment status."""
    total = len(results)
    if total == 0:
        return {
            "total": 0,
            "direct_emails": 0,
            "any_emails": 0,
            "with_phone": 0,
            "verified_address": 0,
            "verified_phone": 0
        }
    
    direct_emails = sum(1 for row in results if row[7])  # is_direct_email column
    any_emails = sum(1 for row in results if row[6])     # email column
    with_phone = sum(1 for row in results if row[11])    # phone_number column
    verified_address = sum(1 for row in results if row[16])  # maps_verified_address column
    verified_phone = sum(1 for row in results if row[17])    # maps_verified_phone column
    
    return {
        "total": total,
        "direct_emails": direct_emails,
        "direct_emails_pct": direct_emails/total*100 if total > 0 else 0,
        "any_emails": any_emails,
        "any_emails_pct": any_emails/total*100 if total > 0 else 0,
        "with_phone": with_phone,
        "with_phone_pct": with_phone/total*100 if total > 0 else 0,
        "verified_address": verified_address,
        "verified_address_pct": verified_address/total*100 if total > 0 else 0,
        "verified_phone": verified_phone,
        "verified_phone_pct": verified_phone/total*100 if total > 0 else 0
    }

def print_enrichment_summary(summary):
    """Print a summary of the enrichment status."""
    if summary["total"] == 0:
        print("No contacts found matching the criteria.")
        return
    
    print("\n=== ENRICHMENT SUMMARY ===")
    print(f"Total contacts: {summary['total']}")
    print(f"With direct emails: {summary['direct_emails']} ({summary['direct_emails_pct']:.1f}%)")
    print(f"With any email: {summary['any_emails']} ({summary['any_emails_pct']:.1f}%)")
    print(f"With phone numbers: {summary['with_phone']} ({summary['with_phone_pct']:.1f}%)")
    print(f"With verified addresses: {summary['verified_address']} ({summary['verified_address_pct']:.1f}%)")
    print(f"With verified phones: {summary['verified_phone']} ({summary['verified_phone_pct']:.1f}%)")
    print("========================\n")

def write_csv(filename, headers, data, include_summary=True, summary=None):
    """Write data to a CSV file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write summary if requested
        if include_summary and summary:
            writer.writerow(["ENRICHMENT SUMMARY"])
            writer.writerow([f"Total contacts: {summary['total']}"])
            writer.writerow([f"With direct emails: {summary['direct_emails']} ({summary['direct_emails_pct']:.1f}%)"])
            writer.writerow([f"With any email: {summary['any_emails']} ({summary['any_emails_pct']:.1f}%)"])
            writer.writerow([f"With phone numbers: {summary['with_phone']} ({summary['with_phone_pct']:.1f}%)"])
            writer.writerow([f"With verified addresses: {summary['verified_address']} ({summary['verified_address_pct']:.1f}%)"])
            writer.writerow([f"With verified phones: {summary['verified_phone']} ({summary['verified_phone_pct']:.1f}%)"])
            writer.writerow([])  # Empty row for separation
        
        # Write headers
        writer.writerow(headers)
        
        # Write data
        for row in data:
            writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description="Check contact enrichment status in the database")
    parser.add_argument("--org", help="Filter by organization name (partial match)")
    parser.add_argument("--contact-id", type=int, help="Filter by contact ID")
    parser.add_argument("--direct-only", action="store_true", help="Show only contacts with direct emails")
    parser.add_argument("--limit", type=int, default=10, help="Limit the number of results (default: 10)")
    parser.add_argument("--compact", action="store_true", help="Show compact view with fewer columns")
    parser.add_argument("--csv", action="store_true", help="Output as CSV instead of table")
    parser.add_argument("--output", help="Output file path (default: contact_enrichment_report.csv or contact_enrichment_report_compact.csv)")
    args = parser.parse_args()
    
    try:
        columns, results = get_contact_status(
            org_name=args.org,
            contact_id=args.contact_id,
            direct_emails_only=args.direct_only,
            limit=args.limit
        )
        
        # Calculate summary statistics
        summary = calculate_enrichment_summary(results)
        
        # Select columns for compact view
        if args.compact:
            compact_columns = [
                "organization_name", "contact_name", "email", "is_direct_email",
                "phone_number", "city", "state", "business_status", "website_status",
                "fit_score", "outreach_readiness"
            ]
            
            # Get indices of compact columns
            indices = [columns.index(col) for col in compact_columns if col in columns]
            
            # Filter columns and results
            columns = [columns[i] for i in indices]
            results = [[row[i] for i in indices] for row in results]
        
        # Determine output mode and destination
        if args.csv:
            # Determine output file
            if args.output:
                output_file = args.output
            else:
                output_dir = os.path.join("clients", "Broadway", "outputs")
                if args.compact:
                    output_file = os.path.join(output_dir, "contact_enrichment_report_compact.csv")
                else:
                    output_file = os.path.join(output_dir, "contact_enrichment_report.csv")
            
            # Write CSV file
            write_csv(output_file, columns, results, include_summary=True, summary=summary)
            print(f"CSV report saved to: {output_file}")
        else:
            # Print to console
            print_enrichment_summary(summary)
            print(tabulate(results, headers=columns, tablefmt="grid"))
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())