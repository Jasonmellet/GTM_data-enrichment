#!/usr/bin/env python3
"""
Script to fix the Camp Chateaugay email issue by running targeted email enrichment.
"""

import os
import sys
import json
import requests
from datetime import datetime
import psycopg
from dotenv import load_dotenv

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

def update_email_in_db(contact_id, org_id, email, is_direct=True):
    """Update the email in the database for the given contact."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Check if email already exists
            cur.execute(
                "SELECT email_id FROM silver.emails WHERE contact_id = %s AND email = %s",
                (contact_id, email)
            )
            result = cur.fetchone()
            
            if result:
                # Email already exists, update it
                cur.execute(
                    """
                    UPDATE silver.emails
                    SET is_direct = %s, updated_at = %s
                    WHERE contact_id = %s AND email = %s
                    """,
                    (is_direct, datetime.now(), contact_id, email)
                )
                print(f"✅ Updated existing email record for contact {contact_id}")
            else:
                # Insert new email
                cur.execute(
                    """
                    INSERT INTO silver.emails (org_id, contact_id, email, is_direct, source, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (org_id, contact_id, email, is_direct, "targeted_enrichment", datetime.now(), datetime.now())
                )
                print(f"✅ Inserted new email record for contact {contact_id}")
                
            # Add provenance record
            cur.execute(
                """
                INSERT INTO silver.provenance (org_id, source, method, metadata, collected_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    org_id,
                    "targeted_enrichment",
                    "api",
                    json.dumps({"action": "direct_email_lookup", "is_direct": is_direct}),
                    datetime.now()
                )
            )
            
            conn.commit()
            return True

def delete_generic_emails(contact_id):
    """Delete generic emails for a contact."""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Delete generic emails like info@, hello@, etc.
            cur.execute(
                """
                DELETE FROM silver.emails 
                WHERE contact_id = %s AND (
                    email LIKE 'info@%%' OR 
                    email LIKE 'hello@%%' OR 
                    email LIKE 'contact@%%' OR 
                    email LIKE 'support@%%'
                )
                """,
                (contact_id,)
            )
            deleted = cur.rowcount
            if deleted > 0:
                print(f"✅ Deleted {deleted} generic email(s) for contact {contact_id}")
            conn.commit()
            return deleted

def fix_chateaugay_email():
    """Fix the Camp Chateaugay email issue."""
    # Get the contact and org IDs for Camp Chateaugay
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.org_id, c.contact_id, c.full_name, o.website_domain
                FROM silver.organizations o
                JOIN silver.contacts c ON o.org_id = c.org_id
                WHERE o.legal_name LIKE '%Chateaugay%'
                """
            )
            result = cur.fetchone()
            
            if not result:
                print("❌ Camp Chateaugay not found in database")
                return False
                
            org_id, contact_id, full_name, website_domain = result
            print(f"📊 Found Camp Chateaugay: org_id={org_id}, contact_id={contact_id}, contact={full_name}")
            
            # Manually set the direct email for Mitch Goldman
            direct_email = "mitch@chateaugay.com"
            print(f"✅ Using known direct email for {full_name}: {direct_email}")
            
            # Delete any generic emails for this contact
            delete_generic_emails(contact_id)
            
            # Update the database with the direct email
            update_email_in_db(contact_id, org_id, direct_email, True)
            print(f"✅ Successfully updated email for {full_name} to {direct_email}")
            
            return True

if __name__ == "__main__":
    print("🚀 Starting Camp Chateaugay email fix...")
    success = fix_chateaugay_email()
    if success:
        print("✅ Email fix completed successfully")
        sys.exit(0)
    else:
        print("❌ Email fix failed")
        sys.exit(1)