#!/usr/bin/env python3
"""
Targeted enrichment for contacts with missing or generic emails.
This script focuses specifically on finding direct email addresses.
"""

import os
import sys
import argparse
import pandas as pd
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection
from Broadway_site_crawler_module import (
    perplexity_person_email_lookup, 
    perplexity_find_primary_contact,
    UsageStats,
    estimate_costs
)

def enrich_contact_email(org_id, contact_id, first_name, last_name, company_name, website_url, verbose=False):
    """Focus specifically on finding a direct email for a contact."""
    print(f"🔍 Enriching email for {first_name} {last_name} at {company_name}")
    
    stats = UsageStats()
    updates = {}
    
    # If we don't have a name, try to find the primary contact first
    if not (first_name or last_name):
        print("  ⚠️ Missing contact name, searching for primary contact...")
        name_updates = perplexity_find_primary_contact(company_name, website_url, verbose=verbose, stats=stats)
        if name_updates:
            first_name = name_updates.get("First Name", "")
            last_name = name_updates.get("Last Name", "")
            print(f"  ✅ Found contact: {first_name} {last_name}")
            updates.update(name_updates)
    
    # Now look for a direct email
    if first_name or last_name:
        print(f"  🔍 Looking up email for {first_name} {last_name}...")
        px_person = perplexity_person_email_lookup(
            first_name, last_name, company_name, website_url, 
            verbose=verbose, stats=stats
        )
        direct_email = px_person.get("Direct Email")
        if direct_email:
            updates["Email"] = direct_email
            updates["Email Status"] = "found_direct"
            updates["Email Confidence"] = px_person.get("Email Confidence", "high")
            if px_person.get("Source Verified URL"):
                updates["Source Verified URL"] = px_person["Source Verified URL"]
            print(f"  ✅ Found direct email: {direct_email}")
        else:
            print("  ❌ No direct email found")
    else:
        print("  ❌ Could not determine contact name")
    
    # Show cost estimate
    costs = estimate_costs(stats)
    print(f"  💰 Cost estimate: ${costs.get('total_cost_est_usd', 0):.3f}")
    
    # Persist updates to database
    if updates:
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Update contact if we found name info
                    if "First Name" in updates or "Last Name" in updates:
                        first = updates.get("First Name", first_name)
                        last = updates.get("Last Name", last_name)
                        full = (first + (" " if last else "") + last).strip()
                        if full:
                            cur.execute("""
                                UPDATE silver.contacts 
                                SET first_name = %s, last_name = %s, full_name = %s
                                WHERE contact_id = %s
                            """, (first, last, full, contact_id))
                    
                    # Add the email if found
                    if "Email" in updates:
                        email = updates["Email"]
                        cur.execute("""
                            INSERT INTO silver.emails 
                            (org_id, contact_id, email, source, verified_at)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (org_id, contact_id, email) DO UPDATE SET
                                source = EXCLUDED.source,
                                verified_at = EXCLUDED.verified_at,
                                updated_at = now()
                            RETURNING email_id
                        """, (org_id, contact_id, email, 'perplexity_direct', datetime.now()))
                        
                        # Note: contacts table doesn't have an email column - we rely on silver.emails table
                    
                    # Log API usage
                    if stats.perplexity_calls > 0:
                        for i in range(stats.perplexity_calls):
                            cur.execute("""
                                INSERT INTO silver.api_usage 
                                (org_id, api_name, cost_usd, metadata)
                                VALUES (%s, %s, %s, %s)
                                RETURNING id
                            """, (
                                org_id,
                                'perplexity',
                                stats.perplexity_cost_usd / stats.perplexity_calls,
                                '{}'
                            ))
                    
                    # Add provenance record with required method field
                    try:
                        cur.execute("""
                            INSERT INTO silver.provenance 
                            (org_id, source, method, metadata, collected_at)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            org_id, 
                            'targeted_email_enrichment',
                            'perplexity_direct',
                            '{"type": "direct_email_lookup"}',
                            datetime.now()
                        ))
                    except Exception as e:
                        print(f"  ⚠️ Could not add provenance: {e}")
                    
                    conn.commit()
                    print("  ✅ Updates persisted to database")
            return True
        except Exception as e:
            print(f"  ❌ Error persisting updates: {e}")
            return False
    else:
        print("  ⚠️ No updates to persist")
        return False

def run_targeted_enrichment(priority_level=None, limit=None):
    """Run targeted email enrichment for contacts based on priority level."""
    try:
        # Load prioritized contacts
        priorities_path = os.path.abspath(os.path.join(
            os.path.dirname(__file__), 
            "../outputs/email_enrichment_priorities.csv"
        ))
        
        if not os.path.exists(priorities_path):
            print(f"❌ Priorities file not found: {priorities_path}")
            print("   Run identify_missing_emails.py first")
            return False
        
        df = pd.read_csv(priorities_path)
        
        # Filter by priority level if specified
        if priority_level:
            if priority_level == 1:
                df = df[(df['email_status'] == 'generic') & 
                        (df['business_status'] == 'open') &
                        (df['website_status'] == 200)]
            elif priority_level == 2:
                df = df[(df['email_status'] == 'missing') & 
                        (df['business_status'] == 'open') &
                        (df['website_status'] == 200)]
            elif priority_level == 3:
                df = df[(df['email_status'] == 'generic') & 
                        ~((df['business_status'] == 'open') & (df['website_status'] == 200))]
        
        # Apply limit if specified
        if limit and limit > 0:
            df = df.head(limit)
        
        if df.empty:
            print("⚠️ No contacts to process with the specified criteria")
            return False
        
        print(f"🔄 Processing {len(df)} contacts for email enrichment...")
        
        # Process each contact
        success_count = 0
        for _, row in df.iterrows():
            org_id = row['org_id']
            contact_id = row['contact_id']
            first_name = row['full_name'].split()[0] if row['full_name'] else ""
            last_name = " ".join(row['full_name'].split()[1:]) if row['full_name'] and len(row['full_name'].split()) > 1 else ""
            company_name = row['display_name'] or row['legal_name']
            website_url = row['website_url']
            
            print(f"\n📋 Processing org_id={org_id}, contact_id={contact_id}: {company_name}")
            
            success = enrich_contact_email(
                org_id, contact_id, first_name, last_name,
                company_name, website_url, verbose=False
            )
            
            if success:
                success_count += 1
        
        # Update scoring after enrichment
        print("\n🔄 Updating scoring with new email data...")
        os.system("python3 " + os.path.join(os.path.dirname(__file__), "update_scoring_v3.py"))
        
        print(f"\n✅ Email enrichment completed: {success_count}/{len(df)} contacts updated")
        return True
        
    except Exception as e:
        print(f"❌ Error in targeted enrichment: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Targeted email enrichment for contacts")
    parser.add_argument("--priority", type=int, choices=[1, 2, 3], help="Priority level (1-3)")
    parser.add_argument("--limit", type=int, default=10, help="Maximum number of contacts to process")
    parser.add_argument("--org-id", type=int, help="Process a specific organization ID")
    parser.add_argument("--contact-id", type=int, help="Process a specific contact ID")
    args = parser.parse_args()
    
    if args.org_id and args.contact_id:
        # Process a single specific contact
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        o.org_id,
                        c.contact_id,
                        c.first_name,
                        c.last_name,
                        COALESCE(o.display_name, o.legal_name) as company_name,
                        w.url as website_url
                    FROM silver.organizations o
                    JOIN silver.contacts c ON c.org_id = o.org_id
                    LEFT JOIN silver.websites w ON w.org_id = o.org_id
                    WHERE o.org_id = %s AND c.contact_id = %s
                """, (args.org_id, args.contact_id))
                
                row = cur.fetchone()
                if row:
                    org_id, contact_id, first_name, last_name, company_name, website_url = row
                    print(f"🔄 Processing single contact: {company_name}")
                    success = enrich_contact_email(
                        org_id, contact_id, first_name, last_name,
                        company_name, website_url, verbose=True
                    )
                    
                    if success:
                        # Update scoring
                        print("\n🔄 Updating scoring with new email data...")
                        os.system("python3 " + os.path.join(os.path.dirname(__file__), "update_scoring_v3.py"))
                        print("✅ Single contact enrichment completed")
                    else:
                        print("❌ Failed to enrich contact")
                else:
                    print(f"❌ Contact not found: org_id={args.org_id}, contact_id={args.contact_id}")
    else:
        # Process contacts by priority
        run_targeted_enrichment(args.priority, args.limit)

if __name__ == "__main__":
    main()
