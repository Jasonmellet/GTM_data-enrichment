#!/usr/bin/env python3
"""
Yelp API integration for business verification and enrichment.
Use this to verify business status and get additional contact details.
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

# Yelp API key
YELP_API_KEY = os.getenv("BROADWAY_YELP_API_KEY")

def yelp_search_business(name: str, location: str = None) -> Dict[str, Any]:
    """
    Search for a business on Yelp.
    
    Args:
        name: Business name
        location: Optional location (city, state)
        
    Returns:
        Dictionary with business details if found
    """
    if not YELP_API_KEY:
        print("❌ Missing BROADWAY_YELP_API_KEY")
        return {}
    
    url = "https://api.yelp.com/v3/businesses/search"
    
    # Build query parameters
    params = {
        "term": name,
        "limit": 5
    }
    
    if location:
        params["location"] = location
    
    headers = {
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"❌ Yelp API error: {response.status_code} - {response.text}")
            return {}
        
        data = response.json()
        if not data.get("businesses") or len(data["businesses"]) == 0:
            print("❌ No businesses found")
            return {}
        
        # Get the best match
        best_match = data["businesses"][0]
        
        # Extract business details
        result = {
            "name": best_match.get("name", ""),
            "phone": best_match.get("phone", ""),
            "display_phone": best_match.get("display_phone", ""),
            "rating": best_match.get("rating", 0),
            "review_count": best_match.get("review_count", 0),
            "url": best_match.get("url", ""),
            "image_url": best_match.get("image_url", ""),
            "is_closed": best_match.get("is_closed", False),
            "categories": [cat.get("title", "") for cat in best_match.get("categories", [])],
            "business_id": best_match.get("id", "")
        }
        
        # Extract location details
        if best_match.get("location"):
            location = best_match["location"]
            result.update({
                "address1": location.get("address1", ""),
                "address2": location.get("address2", ""),
                "address3": location.get("address3", ""),
                "city": location.get("city", ""),
                "state": location.get("state", ""),
                "zip_code": location.get("zip_code", ""),
                "country": location.get("country", ""),
                "display_address": ", ".join(location.get("display_address", []))
            })
        
        # Extract coordinates
        if best_match.get("coordinates"):
            result.update({
                "latitude": best_match["coordinates"].get("latitude", 0),
                "longitude": best_match["coordinates"].get("longitude", 0)
            })
        
        return result
    
    except Exception as e:
        print(f"❌ Error calling Yelp API: {e}")
        return {}

def yelp_get_business_details(business_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a business from Yelp.
    
    Args:
        business_id: Yelp business ID
        
    Returns:
        Dictionary with detailed business information
    """
    if not YELP_API_KEY or not business_id:
        return {}
    
    url = f"https://api.yelp.com/v3/businesses/{business_id}"
    
    headers = {
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"❌ Yelp API error: {response.status_code} - {response.text}")
            return {}
        
        return response.json()
    
    except Exception as e:
        print(f"❌ Error calling Yelp API: {e}")
        return {}

def enrich_business_with_yelp(org_id: int, company_name: str, location: str = None) -> bool:
    """
    Enrich a business with Yelp API data and persist to database.
    
    Args:
        org_id: Organization ID in the database
        company_name: Company name
        location: Optional location (city, state)
        
    Returns:
        True if enrichment was successful, False otherwise
    """
    print(f"🔍 Yelp lookup for {company_name}")
    
    # Search for the business
    business_data = yelp_search_business(company_name, location)
    
    if not business_data:
        print("❌ No business found")
        return False
    
    print(f"✅ Found business: {business_data.get('name')}")
    
    # Get additional details if business ID is available
    if business_data.get("business_id"):
        details = yelp_get_business_details(business_data["business_id"])
        if details:
            business_data.update(details)
    
    # Persist to database
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Update organization name if available
                if business_data.get("name"):
                    cur.execute("""
                        UPDATE silver.organizations 
                        SET display_name = COALESCE(%s, display_name)
                        WHERE org_id = %s
                    """, (business_data["name"], org_id))
                
                # Update location
                address_fields = ["address1", "city", "state", "zip_code", "country"]
                if any(business_data.get(f) for f in address_fields):
                    cur.execute("""
                        UPDATE silver.locations 
                        SET street = COALESCE(%s, street),
                            city = COALESCE(%s, city),
                            region = COALESCE(%s, region),
                            postal_code = COALESCE(%s, postal_code),
                            country = COALESCE(%s, country),
                            business_status = %s,
                            updated_at = now()
                        WHERE org_id = %s
                    """, (
                        business_data.get("address1"),
                        business_data.get("city"),
                        business_data.get("state"),
                        business_data.get("zip_code"),
                        business_data.get("country"),
                        "closed" if business_data.get("is_closed") else "open",
                        org_id
                    ))
                
                # Add phone if available
                if business_data.get("phone"):
                    cur.execute("""
                        INSERT INTO silver.phones 
                        (org_id, contact_id, phone_e164, phone_formatted, source, verified_at)
                        SELECT %s, contact_id, %s, %s, %s, %s
                        FROM silver.contacts
                        WHERE org_id = %s
                        LIMIT 1
                        ON CONFLICT (org_id, contact_id, phone_e164) DO UPDATE SET
                            phone_formatted = EXCLUDED.phone_formatted,
                            source = EXCLUDED.source,
                            verified_at = EXCLUDED.verified_at,
                            updated_at = now()
                    """, (
                        org_id,
                        business_data["phone"],
                        business_data.get("display_phone", business_data["phone"]),
                        'yelp',
                        datetime.now(),
                        org_id
                    ))
                
                # Add categories if available
                if business_data.get("categories"):
                    categories = business_data["categories"]
                    if isinstance(categories, list) and categories:
                        categories_str = "; ".join(categories)
                        
                        # First check if we already have categories
                        cur.execute("""
                            SELECT COUNT(*) FROM silver.org_categories WHERE org_id = %s
                        """, (org_id,))
                        
                        if cur.fetchone()[0] == 0:
                            # Add categories one by one
                            for category in categories:
                                # Create category if it doesn't exist
                                slug = category.lower().replace(" ", "-").replace("/", "-")
                                cur.execute("""
                                    INSERT INTO silver.categories (slug, label)
                                    VALUES (%s, %s)
                                    ON CONFLICT (slug) DO UPDATE SET label = EXCLUDED.label
                                    RETURNING category_id
                                """, (slug, category))
                                
                                category_id = cur.fetchone()[0]
                                
                                # Link category to organization
                                cur.execute("""
                                    INSERT INTO silver.org_categories (org_id, category_id, confidence)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (org_id, category_id) DO UPDATE SET
                                        confidence = EXCLUDED.confidence
                                """, (org_id, category_id, 80))
                
                # Add provenance record
                cur.execute("""
                    INSERT INTO silver.provenance 
                    (org_id, source, method, metadata, collected_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    org_id,
                    'yelp_enrichment',
                    'api_lookup',
                    json.dumps({
                        "business_id": business_data.get("business_id", ""),
                        "rating": business_data.get("rating", 0),
                        "review_count": business_data.get("review_count", 0)
                    }),
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
                    'yelp',
                    0.05,  # Approximate cost per lookup
                    '{}'
                ))
                
                conn.commit()
                print("  ✅ Updates persisted to database")
                return True
                
    except Exception as e:
        print(f"  ❌ Error persisting updates: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Yelp API business enrichment")
    parser.add_argument("--org-id", type=int, required=True, help="Organization ID")
    parser.add_argument("--company", type=str, required=True, help="Company name")
    parser.add_argument("--location", type=str, help="Location (city, state)")
    args = parser.parse_args()
    
    success = enrich_business_with_yelp(
        args.org_id,
        args.company,
        args.location
    )
    
    if success:
        print("\n🔄 Updating scoring with new data...")
        os.system("python3 " + os.path.join(os.path.dirname(__file__), "update_scoring_v3.py"))
        print("✅ Yelp enrichment completed")
    else:
        print("❌ Yelp enrichment failed")

if __name__ == "__main__":
    main()
