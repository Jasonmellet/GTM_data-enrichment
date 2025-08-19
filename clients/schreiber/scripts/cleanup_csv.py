#!/usr/bin/env python3
"""
Cleanup script to fix CSV data that was written to wrong rows
"""

import pandas as pd
import os

# CSV file path (use fixed CSV used by pipeline)
CSV_FILE_PATH = "../data/Schreiber Sheet 5_11 test - Sheet1 (2).fixed.csv"

def cleanup_csv():
    """Clean up the CSV by removing incorrectly written research data"""
    
    print("🧹 CSV Cleanup Script")
    print("=" * 50)
    
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        return
    
    # Read the CSV
    print(f"📖 Reading CSV file: {CSV_FILE_PATH}")
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"📊 Found {len(df)} rows of data")
    
    # Research columns to clean
    research_columns = [
        'Company Research Summary', 
        'Contact Research Summary', 
        'Industry Pain Points', 
        'Schreiber Opportunity Match', 
        'Research Quality Score'
    ]
    
    # Email columns to clean
    email_columns = [
        'Email 1 Subject', 'Email 1 Icebreaker', 'Email 1 Body', 'Email 1 CTA',
        'Email 2 Subject', 'Email 2 Icebreaker', 'Email 2 Body', 'Email 2 CTA Text',
        'Email 3 Subject', 'Email 3 Icebreaker', 'Email 3 Body', 'Email 3 CTA Text'
    ]
    
    # Clear ALL email columns across all rows; keep research intact
    cleared = 0
    for idx in range(len(df)):
        for col in email_columns:
            if col in df.columns and df.at[idx, col] != '':
                df.at[idx, col] = ''
                cleared += 1
    print(f"🧹 Cleared {cleared} email fields across all rows (BA–BL)")
    
    # Save the cleaned CSV
    df.to_csv(CSV_FILE_PATH, index=False)
    print(f"✅ Cleaned CSV saved successfully")
    
    # Verify the cleanup: ensure email columns are empty
    print(f"\n🔍 Verification: email columns empty check")
    sample = df[email_columns].head(3).to_string(index=False)
    print(sample)

if __name__ == "__main__":
    cleanup_csv()
