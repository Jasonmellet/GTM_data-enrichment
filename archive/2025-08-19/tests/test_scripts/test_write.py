#!/usr/bin/env python3
"""
Simple test script to write one NAICS code to row 2 of Google Sheet.
"""

from google_sheets_handler import GoogleSheetsHandler
from naics_enricher import NAICSEnricher

def test_single_write():
    """Test writing one NAICS code to the sheet."""
    print("🧪 Testing Google Sheets Write...")
    
    # Initialize handlers
    sheets_handler = GoogleSheetsHandler()
    enricher = NAICSEnricher()
    
    # Test with one business
    business_name = "8th Avenue Food and Provisions"
    
    print(f"🔍 Testing with: {business_name}")
    
    # Get NAICS code
    result = enricher.enrich_business_data(business_name=business_name)
    
    print(f"📊 Result: {result}")
    
    # Write to sheet
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    print(f"📝 Writing to Google Sheet...")
    
    success = sheets_handler.write_naics_codes(
        sheet_url=sheet_url,
        sheet_name=sheet_name,
        enriched_data=[result],
        naics_column='NAICS_Code'
    )
    
    if success:
        print("✅ Successfully wrote to Google Sheet!")
    else:
        print("❌ Failed to write to Google Sheet")

if __name__ == "__main__":
    test_single_write()
