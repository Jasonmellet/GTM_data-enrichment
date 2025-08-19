#!/usr/bin/env python3
"""
Apollo API integration for email enrichment fallback.
Use this when other methods fail to find direct emails.
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

# Apollo API key
APOLLO_API_KEY = os.getenv("BROADWAY_APOLLO_API_KEY")

def apollo_lookup_person(first_name: str, last_name: str, company_name: str, domain: str = None) -> Dict[str, Any]:
    """
    Look up a person using Apollo API to find their email.
    
    Args:
        first_name: First name of the person
        last_name: Last name of the person
        company_name: Company name
        domain: Optional company domain
        
    Returns:
        Dictionary with email and other details if found
    """
    if not APOLLO_API_KEY:
        print("❌ Missing BROADWAY_APOLLO_API_KEY")
        return {}
    
    url = "https://api.apollo.io/v1/people/search"
    
    # Build query parameters
    params = {
        "api_key": APOLLO_API_KEY,
        "q_person_name": f"{first_name} {last_name}".strip(),
        "page": 1,
        "per_page": 5
    }
    
    # Add company name or domain if available
    if company_name:
        params["q_organization_domains"] = domain if domain else ""
        params["q_organization_name"] = company_name
    
    try:
        response = requests.post(url, json=params)
        if response.status_code != 200:
            print(f"❌ Apollo API error: {response.status_code} - {response.text}")
            return {}
        
        data = response.json()
        if not data.get("people") or len(data["people"]) == 0:
            print("❌ No results found")
            return {}
        
        # Get the best match
        best_match = data["people"][0]
        
        # Extract email
        email = best_match.get("email", "")
        
        # Extract other useful information
        result = {
            "email": email,
            "first_name": best_match.get("first_name", ""),
            "last_name": best_match.get("last_name", ""),
            "title": best_match.get("title", ""),
            "seniority": best_match.get("seniority", ""),
            "linkedin_url": best_match.get("linkedin_url", ""),
            "confidence": "medium" if email else "low"
        }
        
        # If we have an email, mark it as high confidence
        if email and "@" in email and not email.startswith("info@") and not email.startswith("contact@"):
            result["confidence"] = "high"
        
        return result
    
    except Exception as e:
        print(f"❌ Error calling Apollo API: {e}")
        return {}

def apollo_lookup_organization(company_name: str, domain: str = None) -> Dict[str, Any]:
    """
    Look up an organization using Apollo API.
    
    Args:
        company_name: Company name
        domain: Optional company domain
        
    Returns:
        Dictionary with organization details
    """
    if not APOLLO_API_KEY:
        print("❌ Missing BROADWAY_APOLLO_API_KEY")
        return {}
    
    url = "https://api.apollo.io/v1/organizations/search"
    
    # Build query parameters
    params = {
        "api_key": APOLLO_API_KEY,
        "page": 1,
        "per_page": 1
    }
    
    if domain:
        params["q_organization_domains"] = domain
    else:
        params["q_organization_name"] = company_name
    
    try:
        response = requests.post(url, json=params)
        if response.status_code != 200:
            print(f"❌ Apollo API error: {response.status_code} - {response.text}")
            return {}
        
        data = response.json()
        if not data.get("organizations") or len(data["organizations"]) == 0:
            print("❌ No organization found")
            return {}
        
        org = data["organizations"][0]
        
        return {
            "name": org.get("name", ""),
            "domain": org.get("domain", ""),
            "phone": org.get("phone", ""),
            "industry": org.get("industry", ""),
            "website_url": org.get("website_url", ""),
            "linkedin_url": org.get("linkedin_url", ""),
            "twitter_url": org.get("twitter_url", ""),
            "facebook_url": org.get("facebook_url", ""),
            "address": org.get("address", ""),
            "city": org.get("city", ""),
            "state": org.get("state", ""),
            "country": org.get("country", ""),
            "postal_code": org.get("postal_code", "")
        }
    
    except Exception as e:
        print(f"❌ Error calling Apollo API: {e}")
        return {}

def enrich_contact_with_apollo(org_id: int, contact_id: int, first_name: str, last_name: str, company_name: str, domain: str = None) -> bool:
    """
    Enrich a contact with Apollo API and persist to database.
    
    Args:
        org_id: Organization ID in the database
        contact_id: Contact ID in the database
        first_name: First name of the person
        last_name: Last name of the person
        company_name: Company name
        domain: Optional company domain
        
    Returns:
        True if enrichment was successful, False otherwise
    """
    print(f"🔍 Apollo lookup for {first_name} {last_name} at {company_name}")
    
    # Look up the person
    person_data = apollo_lookup_person(first_name, last_name, company_name, domain)
    
    if not person_data.get("email"):
        print("❌ No email found")
        return False
    
    email = person_data["email"]
    print(f"✅ Found email: {email}")
    
    # Look up the organization for additional data
    org_data = apollo_lookup_organization(company_name, domain)
    
    # Persist to database
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Add the email
                cur.execute("""
                    INSERT INTO silver.emails 
                    (org_id, contact_id, email, source, verified_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (org_id, contact_id, email) DO UPDATE SET
                        source = EXCLUDED.source,
                        verified_at = EXCLUDED.verified_at,
                        updated_at = now()
                    RETURNING email_id
                """, (org_id, contact_id, email, 'apollo', datetime.now()))
                
                email_id = cur.fetchone()[0]
                print(f"  ✅ Email {email} persisted (ID: {email_id})")
                
                # Update contact details if available
                if person_data.get("title"):
                    cur.execute("""
                        UPDATE silver.contacts 
                        SET role_title = COALESCE(%s, role_title)
                        WHERE contact_id = %s
                    """, (person_data["title"], contact_id))
                
                # Update organization details if available
                if org_data:
                    # Update location
                    if any([org_data.get(f) for f in ["address", "city", "state", "country", "postal_code"]]):
                        cur.execute("""
                            UPDATE silver.locations 
                            SET street = COALESCE(%s, street),
                                city = COALESCE(%s, city),
                                region = COALESCE(%s, region),
                                postal_code = COALESCE(%s, postal_code),
                                country = COALESCE(%s, country),
                                updated_at = now()
                            WHERE org_id = %s
                        """, (
                            org_data.get("address"),
                            org_data.get("city"),
                            org_data.get("state"),
                            org_data.get("postal_code"),
                            org_data.get("country"),
                            org_id
                        ))
                    
                    # Add phone if available
                    if org_data.get("phone"):
                        cur.execute("""
                            INSERT INTO silver.phones 
                            (org_id, contact_id, phone_e164, phone_formatted, source, verified_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (org_id, contact_id, phone_e164) DO UPDATE SET
                                phone_formatted = EXCLUDED.phone_formatted,
                                source = EXCLUDED.source,
                                verified_at = EXCLUDED.verified_at,
                                updated_at = now()
                            RETURNING phone_id
                        """, (
                            org_id,
                            contact_id,
                            org_data["phone"],
                            org_data["phone"],
                            'apollo',
                            datetime.now()
                        ))
                    
                    # Add social links
                    for platform, url_key in [
                        ("linkedin", "linkedin_url"),
                        ("facebook", "facebook_url"),
                        ("twitter", "twitter_url")
                    ]:
                        if org_data.get(url_key):
                            cur.execute("""
                                INSERT INTO silver.socials 
                                (org_id, platform, url, verified_at)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (org_id, platform) DO UPDATE SET
                                    url = EXCLUDED.url,
                                    verified_at = EXCLUDED.verified_at,
                                    updated_at = now()
                                RETURNING social_id
                            """, (
                                org_id,
                                platform,
                                org_data[url_key],
                                datetime.now()
                            ))
                
                # Add provenance record
                cur.execute("""
                    INSERT INTO silver.provenance 
                    (org_id, source, method, metadata, collected_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    org_id,
                    'apollo_enrichment',
                    'api_lookup',
                    json.dumps({"email_confidence": person_data.get("confidence", "low")}),
                    datetime.now()
                ))
                
                # Add API usage record
                cur.execute("""
                    INSERT INTO silver.api_usage 
                    (org_id, api_name, cost_usd, metadata)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (
                    org_id,
                    'apollo',
                    0.10,  # Approximate cost per lookup
                    '{}'
                ))
                
                conn.commit()
                print("  ✅ Updates persisted to database")
                return True
                
    except Exception as e:
        print(f"  ❌ Error persisting updates: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Apollo API email enrichment")
    parser.add_argument("--org-id", type=int, required=True, help="Organization ID")
    parser.add_argument("--contact-id", type=int, required=True, help="Contact ID")
    parser.add_argument("--first-name", type=str, help="First name")
    parser.add_argument("--last-name", type=str, help="Last name")
    parser.add_argument("--company", type=str, required=True, help="Company name")
    parser.add_argument("--domain", type=str, help="Company domain")
    args = parser.parse_args()
    
    success = enrich_contact_with_apollo(
        args.org_id,
        args.contact_id,
        args.first_name or "",
        args.last_name or "",
        args.company,
        args.domain
    )
    
    if success:
        print("\n🔄 Updating scoring with new data...")
        os.system("python3 " + os.path.join(os.path.dirname(__file__), "update_scoring_v3.py"))
        print("✅ Apollo enrichment completed")
    else:
        print("❌ Apollo enrichment failed")

if __name__ == "__main__":
    main()
