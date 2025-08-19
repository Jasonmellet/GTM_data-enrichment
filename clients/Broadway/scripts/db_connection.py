#!/usr/bin/env python3
"""
Simple database connection module for Broadway summer camps project.
Clean, simple database operations.
"""

import os
import psycopg
from typing import Optional, Dict, Any
from datetime import datetime

def get_db_connection():
    """Get a database connection using environment variables."""
    return psycopg.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        dbname=os.getenv('DB_NAME', 'summer_camps_db'),
        user=os.getenv('DB_USER', 'summer_camps_user'),
        password=os.getenv('DB_PASSWORD', 'blank')
    )

def test_connection():
    """Test database connection and return status."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                return True, f"Connected successfully. PostgreSQL version: {version[0]}"
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

def get_table_counts():
    """Get row counts for our tables."""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        'organizations' as table_name, COUNT(*) as count 
                    FROM summer_camps.organizations
                    UNION ALL
                    SELECT 
                        'contacts' as table_name, COUNT(*) as count 
                    FROM summer_camps.contacts
                    ORDER BY table_name;
                """)
                return cur.fetchall()
    except Exception as e:
        return [("error", str(e))]

if __name__ == "__main__":
    print("Testing Broadway database connection...")
    success, message = test_connection()
    print(f"Status: {message}")
    
    if success:
        print("\nTable counts:")
        counts = get_table_counts()
        for table, count in counts:
            print(f"  {table}: {count} rows")
