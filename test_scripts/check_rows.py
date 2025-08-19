#!/usr/bin/env python3
"""
Check what companies are actually in rows 6, 7, and 8.
"""

import os
from dotenv import load_dotenv
from google_sheets_handler import GoogleSheetsHandler

def check_rows():
    """Check what companies are in specific rows."""
    print("🔍 Checking Company Names in Rows 6, 7, and 8")
    print("=" * 60)
    
    load_dotenv()
    sheet_url = os.getenv('GOOGLE_SHEET_URL')
    
    if not sheet_url:
        print("❌ Google Sheet URL not found")
        return
    
    sheets_handler = GoogleSheetsHandler()
    sheet_name = "Sheet1"
    
    try:
        df = sheets_handler.read_sheet_data(sheet_url, sheet_name)
        if df is None or df.empty:
            print("❌ Failed to read sheet data")
            return
        
        print(f"📊 Found {len(df)} rows of data")
        
        # Check rows 6, 7, and 8 (indices 5, 6, 7)
        for i in [5, 6, 7]:
            row = df.iloc[i]
            company_name = row.iloc[0] if len(row) > 0 else "No company name"
            first_name = row.iloc[4] if len(row) > 4 else "No first name"
            last_name = row.iloc[3] if len(row) > 3 else "No last name"
            job_title = row.iloc[2] if len(row) > 2 else "No job title"
            
            print(f"Row {i+1} (index {i}): {company_name}")
            print(f"  Contact: {first_name} {last_name} - {job_title}")
            print("-" * 40)
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_rows()
