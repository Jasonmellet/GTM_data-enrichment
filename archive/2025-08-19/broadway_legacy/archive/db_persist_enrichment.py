#!/usr/bin/env python3
"""
Persist crawler and Maps enrichment data to PostgreSQL database.
Updates websites, socials, emails, phones, locations, and provenance tables.
"""

import os
import sys
import json
import psycopg
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import argparse

# Add the project root to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../..'))

from config import get_db_connection

def persist_website_data(org_id: int, website_data: Dict, cur) -> int:
    """Persist website data to silver.websites table."""
    try:
        cur.execute("""
            INSERT INTO silver.websites 
            (org_id, url, status_code, platform_hint, last_crawled_at, js_rendered, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (org_id) DO UPDATE SET
                url = EXCLUDED.url,
                status_code = EXCLUDED.status_code,
                platform_hint = EXCLUDED.platform_hint,
                last_crawled_at = EXCLUDED.last_crawled_at,
                js_rendered = EXCLUDED.js_rendered,
                metadata = EXCLUDED.metadata,
                updated_at = now()
            RETURNING website_id
        """, (
            org_id,
            website_data.get('url'),
            website_data.get('status_code'),
            website_data.get('platform_hint'),
            datetime.now(),
            website_data.get('js_rendered', False),
            json.dumps(website_data.get('metadata', {}))
        ))
        return cur.fetchone()[0]
    except Exception as e:
        print(f"❌ Error persisting website data for org_id {org_id}: {e}")
        return None

def persist_socials(org_id: int, socials_data: Dict, cur) -> List[int]:
    """Persist social media data to silver.socials table."""
    social_ids = []
    for platform, url in socials_data.items():
        if url and url.strip():
            try:
                cur.execute("""
                    INSERT INTO silver.socials 
                    (org_id, platform, url, verified_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (org_id, platform) DO UPDATE SET
                        url = EXCLUDED.url,
                        verified_at = EXCLUDED.verified_at,
                        updated_at = now()
                    RETURNING social_id
                """, (org_id, platform, url, datetime.now()))
                social_ids.append(cur.fetchone()[0])
            except Exception as e:
                print(f"❌ Error persisting {platform} social for org_id {org_id}: {e}")
    return social_ids

def persist_emails(org_id: int, contact_id: int, emails: List[str], cur) -> List[int]:
    """Persist email data to silver.emails table."""
    email_ids = []
    for email in emails:
        if email and email.strip():
            try:
                cur.execute("""
                    INSERT INTO silver.emails 
                    (org_id, contact_id, email, source, verified_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (org_id, contact_id, email) DO UPDATE SET
                        source = EXCLUDED.source,
                        verified_at = EXCLUDED.verified_at,
                        updated_at = now()
                    RETURNING email_id
                """, (org_id, contact_id, email, 'crawler', datetime.now()))
                email_ids.append(cur.fetchone()[0])
            except Exception as e:
                print(f"❌ Error persisting email {email} for org_id {org_id}: {e}")
    return email_ids

def persist_phones(org_id: int, contact_id: int, phones: List[str], cur) -> List[int]:
    """Persist phone data to silver.phones table."""
    phone_ids = []
    for phone in phones:
        if phone and phone.strip():
            try:
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
                """, (org_id, contact_id, phone, phone, 'crawler', datetime.now()))
                phone_ids.append(cur.fetchone()[0])
            except Exception as e:
                print(f"❌ Error persisting phone {phone} for org_id {org_id}: {e}")
    return phone_ids

def persist_location_updates(org_id: int, location_data: Dict, cur) -> bool:
    """Update location data with business status and Maps verification."""
    try:
        cur.execute("""
            UPDATE silver.locations 
            SET business_status = %s,
                maps_verified_phone = %s,
                maps_verified_address = %s,
                updated_at = now()
            WHERE org_id = %s
        """, (
            location_data.get('business_status'),
            location_data.get('maps_verified_phone'),
            location_data.get('maps_verified_address'),
            org_id
        ))
        return cur.rowcount > 0
    except Exception as e:
        print(f"❌ Error updating location for org_id {org_id}: {e}")
        return False

def persist_provenance(org_id: int, source: str, method: str, metadata: Dict, cur) -> int:
    """Persist provenance data to silver.provenance table."""
    try:
        cur.execute("""
            INSERT INTO silver.provenance 
            (org_id, source, method, metadata, collected_at)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING provenance_id
        """, (
            org_id,
            source,
            method,
            json.dumps(metadata),
            datetime.now()
        ))
        return cur.fetchone()[0]
    except Exception as e:
        print(f"❌ Error persisting provenance for org_id {org_id}: {e}")
        return None

def persist_api_usage(org_id: int, api_calls: List[Dict], cur) -> List[int]:
    """Persist API usage data to silver.api_usage table."""
    usage_ids = []
    for call in api_calls:
        try:
            cur.execute("""
                INSERT INTO silver.api_usage 
                (org_id, api_name, endpoint, cost_usd, tokens_used, response_time_ms, success, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING usage_id
            """, (
                org_id,
                call.get('api_name'),
                call.get('endpoint'),
                call.get('cost_usd', 0.0),
                call.get('tokens_used', 0),
                call.get('response_time_ms', 0),
                call.get('success', True),
                json.dumps(call.get('metadata', {}))
            ))
            usage_ids.append(cur.fetchone()[0])
        except Exception as e:
            print(f"❌ Error persisting API usage for org_id {org_id}: {e}")
    return usage_ids

def get_org_id_from_contact_id(contact_id: int, cur) -> Optional[int]:
    """Get organization ID from contact ID."""
    try:
        cur.execute("""
            SELECT org_id FROM silver.contacts WHERE contact_id = %s
        """, (contact_id,))
        result = cur.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"❌ Error getting org_id for contact_id {contact_id}: {e}")
        return None

def persist_enrichment_for_contact(contact_id: int, enrichment_data: Dict, cur) -> bool:
    """Persist all enrichment data for a single contact."""
    org_id = get_org_id_from_contact_id(contact_id, cur)
    if not org_id:
        print(f"❌ No org_id found for contact_id {contact_id}")
        return False
    
    print(f"📊 Persisting enrichment for contact_id {contact_id} (org_id: {org_id})")
    
    # Persist website data
    if 'website' in enrichment_data:
        website_id = persist_website_data(org_id, enrichment_data['website'], cur)
        if website_id:
            print(f"  ✅ Website persisted (ID: {website_id})")
    
    # Persist socials
    if 'socials' in enrichment_data:
        social_ids = persist_socials(org_id, enrichment_data['socials'], cur)
        if social_ids:
            print(f"  ✅ {len(social_ids)} socials persisted")
    
    # Persist emails
    if 'emails' in enrichment_data:
        email_ids = persist_emails(org_id, contact_id, enrichment_data['emails'], cur)
        if email_ids:
            print(f"  ✅ {len(email_ids)} emails persisted")
    
    # Persist phones
    if 'phones' in enrichment_data:
        phone_ids = persist_phones(org_id, contact_id, enrichment_data['phones'], cur)
        if phone_ids:
            print(f"  ✅ {len(phone_ids)} phones persisted")
    
    # Update location with business status
    if 'location' in enrichment_data:
        location_updated = persist_location_updates(org_id, enrichment_data['location'], cur)
        if location_updated:
            print(f"  ✅ Location updated with business status")
    
    # Persist provenance
    if 'provenance' in enrichment_data:
        provenance_id = persist_provenance(org_id, 'crawler', 'web_scraping', enrichment_data['provenance'], cur)
        if provenance_id:
            print(f"  ✅ Provenance persisted (ID: {provenance_id})")
    
    # Persist API usage
    if 'api_usage' in enrichment_data:
        usage_ids = persist_api_usage(org_id, enrichment_data['api_usage'], cur)
        if usage_ids:
            print(f"  ✅ {len(usage_ids)} API usage records persisted")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Persist enrichment data to database')
    parser.add_argument('--contact-id', type=int, help='Specific contact ID to process')
    parser.add_argument('--csv', type=str, help='Path to CSV file with enrichment data')
    args = parser.parse_args()
    
    if not args.contact_id:
        print("❌ Please provide --contact-id")
        return
    
    # For now, we'll simulate enrichment data since we need to integrate with the crawler
    # In practice, this would come from the crawler output or a JSON file
    
    # Simulated enrichment data structure
    enrichment_data = {
        'website': {
            'url': 'https://example.com',
            'status_code': 200,
            'platform_hint': 'wordpress',
            'js_rendered': True,
            'metadata': {'title': 'Example Camp'}
        },
        'socials': {
            'facebook': 'https://facebook.com/examplecamp',
            'instagram': 'https://instagram.com/examplecamp'
        },
        'emails': ['info@example.com', 'director@example.com'],
        'phones': ['+1-555-0123'],
        'location': {
            'business_status': 'OPERATIONAL',
            'maps_verified_phone': True,
            'maps_verified_address': True
        },
        'provenance': {
            'crawled_at': datetime.now().isoformat(),
            'user_agent': 'Mozilla/5.0',
            'success': True
        },
        'api_usage': [
            {
                'api_name': 'google_maps',
                'endpoint': 'place_details',
                'cost_usd': 0.017,
                'tokens_used': 0,
                'response_time_ms': 150,
                'success': True,
                'metadata': {'place_id': 'ChIJ...'}
            }
        ]
    }
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                success = persist_enrichment_for_contact(args.contact_id, enrichment_data, cur)
                if success:
                    print(f"✅ Successfully persisted enrichment for contact_id {args.contact_id}")
                else:
                    print(f"❌ Failed to persist enrichment for contact_id {args.contact_id}")
    except Exception as e:
        print(f"❌ Database connection error: {e}")

if __name__ == "__main__":
    main()
