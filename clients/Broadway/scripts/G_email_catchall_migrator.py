#!/usr/bin/env python3
"""
Move non-validated contacts to catchall table (MAIN DB).

Logic per contact (main list):
- Generate email predictions using the allowed 10 formats only
- Validate up to 10 predictions (1s delay, accept only status == 'valid')
- If none are valid → insert into summer_camps.catchall_contacts and delete from summer_camps.contacts

Usage:
  python3 scripts/email_catchall_migrator.py --limit 50            # process first 50
  python3 scripts/email_catchall_migrator.py --limit 50 --dry-run  # simulate only
"""

import asyncio
import argparse
import sys
import os
from typing import List, Dict

# Ensure imports of local modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_connection import get_db_connection
from enhanced_email_discovery import EnhancedEmailDiscovery


DDL_CATCHALL = """
CREATE TABLE IF NOT EXISTS summer_camps.catchall_contacts (
    catchall_id SERIAL PRIMARY KEY,
    contact_id INTEGER,
    org_id INTEGER,
    contact_name VARCHAR(255),
    role_title VARCHAR(255),
    company_name VARCHAR(500),
    website_url TEXT,
    attempted_count INTEGER DEFAULT 0,
    attempted_emails TEXT[],
    reason VARCHAR(100) DEFAULT 'no_valid_email',
    moved_at TIMESTAMP DEFAULT NOW()
);
"""


def ensure_catchall_table() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL_CATCHALL)
            conn.commit()


def fetch_main_contacts(limit: int, offset: int) -> List[Dict]:
    """Fetch contacts from main list that need validation.
    Targets contacts with missing email or non-valid status.
    1 contact per org to reduce bloat per run.
    """
    contacts: List[Dict] = []
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (c.org_id)
                    c.contact_id,
                    c.org_id,
                    c.contact_name,
                    c.role_title,
                    COALESCE(c.email_validation_status, '') AS email_status,
                    COALESCE(c.contact_email, '') AS contact_email,
                    o.company_name,
                    o.website_url
                FROM summer_camps.contacts c
                JOIN summer_camps.organizations o ON c.org_id = o.org_id
                WHERE (c.contact_email IS NULL OR c.contact_email = '' OR c.contact_email = 'None')
                   OR (c.email_validation_status IS NULL OR c.email_validation_status <> 'valid')
                ORDER BY c.org_id, c.contact_id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            for row in cur.fetchall():
                contacts.append(
                    {
                        'contact_id': row[0],
                        'org_id': row[1],
                        'contact_name': row[2],
                        'role_title': row[3],
                        'email_status': row[4],
                        'contact_email': row[5],
                        'company_name': row[6],
                        'website_url': row[7],
                    }
                )
    return contacts


async def validate_up_to_10(discovery: EnhancedEmailDiscovery, name: str, website_url: str) -> Dict:
    """Generate allowed-format predictions and validate up to 10, returning result dict."""
    predictions = discovery.generate_pattern_based_predictions(name, website_url)
    predictions = predictions[:10]  # enforce 10 max

    attempted = []
    for email in predictions:
        attempted.append(email)
        try:
            result = await discovery.validate_single_email(email)
            if result.get('status') == 'valid':
                return {'valid_email': email, 'attempted': attempted}
        except Exception:
            # ignore and continue
            pass
        await asyncio.sleep(1)

    return {'valid_email': None, 'attempted': attempted}


def move_to_catchall(contact: Dict, attempted: List[str], dry_run: bool) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if not dry_run:
                # insert to catchall
                cur.execute(
                    """
                    INSERT INTO summer_camps.catchall_contacts
                        (contact_id, org_id, contact_name, role_title, company_name, website_url,
                         attempted_count, attempted_emails, reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'no_valid_email')
                    """,
                    (
                        contact['contact_id'],
                        contact['org_id'],
                        contact['contact_name'],
                        contact.get('role_title') or None,
                        contact['company_name'],
                        contact['website_url'],
                        len(attempted),
                        attempted,
                    ),
                )
                # delete from contacts
                cur.execute(
                    "DELETE FROM summer_camps.contacts WHERE contact_id = %s",
                    (contact['contact_id'],),
                )
                conn.commit()


def save_valid_email(contact_id: int, email: str, score: int = 0, dry_run: bool = False) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            if not dry_run:
                cur.execute(
                    """
                    UPDATE summer_camps.contacts
                    SET contact_email = %s,
                        email_validation_status = 'valid',
                        email_validation_score = %s,
                        email_validation_timestamp = NOW(),
                        email_validation_provider = 'zerobounce',
                        last_enriched_at = NOW()
                    WHERE contact_id = %s
                    """,
                    (email, int(score) if score is not None else 0, contact_id),
                )
                conn.commit()


async def main():
    parser = argparse.ArgumentParser(description='Move non-validated contacts to catchall (main DB)')
    parser.add_argument('--limit', type=int, default=50, help='Max contacts to process (1 per org)')
    parser.add_argument('--offset', type=int, default=0, help='Offset for pagination (in contacts)')
    parser.add_argument('--dry-run', action='store_true', help='Simulate without DB writes')
    args = parser.parse_args()

    ensure_catchall_table()
    contacts = fetch_main_contacts(args.limit, args.offset)
    if not contacts:
        print('No candidates found.')
        return

    print(f'Processing {len(contacts)} contacts (dry-run={args.dry_run})...')

    discovery = EnhancedEmailDiscovery()

    moved = 0
    validated = 0

    for c in contacts:
        print(f"\n👤 {c['contact_name']} at {c['company_name']} ({c['website_url']}) [contact_id={c['contact_id']}]")
        res = await validate_up_to_10(discovery, c['contact_name'], c['website_url'])
        if res['valid_email']:
            print(f"   ✅ VALID: {res['valid_email']}")
            save_valid_email(c['contact_id'], res['valid_email'], score=0, dry_run=args.dry_run)
            validated += 1
        else:
            print(f"   ❌ No valid after {len(res['attempted'])} attempts → moving to catchall")
            move_to_catchall(c, res['attempted'], dry_run=args.dry_run)
            moved += 1

    print(f"\nDone. Validated: {validated}, Moved to catchall: {moved}, Total processed: {len(contacts)}")


if __name__ == '__main__':
    asyncio.run(main())
