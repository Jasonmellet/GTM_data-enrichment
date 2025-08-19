#!/usr/bin/env python3
"""
Test script to run enhanced NAICS enrichment with cream cheese analysis on first 5 records.
"""

from google_sheets_handler import GoogleSheetsHandler
from naics_enricher import NAICSEnricher
import pandas as pd
from tqdm import tqdm

def test_first_5():
    """Test the enhanced enrichment on first 5 records."""
    print("🧪 Testing Enhanced NAICS + Cream Cheese Analysis (First 5 Records)")
    print("=" * 70)
    
    # Initialize handlers
    sheets_handler = GoogleSheetsHandler()
    enricher = NAICSEnricher()
    
    # Read data from Google Sheets
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    print(f"📖 Reading data from Google Sheets...")
    df = sheets_handler.read_sheet_data(sheet_url, sheet_name)
    
    print(f"✅ Found {len(df)} rows of data")
    print(f"📋 Columns: {', '.join(df.columns)}")
    
    # Process only first 5 businesses
    print(f"\n🔍 Processing first 5 businesses...")
    enriched_data = []
    
    for idx in range(min(5, len(df))):
        row = df.iloc[idx]
        business_name = str(row['Company Name']) if pd.notna(row['Company Name']) else ""
        
        if not business_name or business_name.strip() == "":
            continue
        
        print(f"\n📊 Processing: {business_name}")
        
        # Enrich with NAICS code and cream cheese analysis
        result = enricher.enrich_business_data(business_name=business_name)
        
        # Add row index for tracking
        result['row_index'] = idx + 2  # +2 because Google Sheets is 1-indexed and we skip header
        enriched_data.append(result)
        
        # Show detailed result
        source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
        likely_emoji = "✅" if result['likely_to_buy'] == 'Yes' else "❌"
        print(f"  NAICS: {result['naics_code'] or 'N/A'} {source_emoji}")
        print(f"  Likely to Buy: {likely_emoji} {result['likely_to_buy']}")
    
    # Summary
    print(f"\n📊 Test Summary:")
    print(f"  Processed: {len(enriched_data)} businesses")
    print(f"  With NAICS: {sum(1 for item in enriched_data if item['naics_code'])}")
    print(f"  Likely customers: {sum(1 for item in enriched_data if item['likely_to_buy'] == 'Yes')}")
    
    # Write to sheet
    print(f"\n📝 Writing to Google Sheet...")
    success = sheets_handler.write_naics_codes(
        sheet_url=sheet_url,
        sheet_name=sheet_name,
        enriched_data=enriched_data,
        naics_column='NAICS_Code'
    )
    
    if success:
        print("✅ Successfully wrote to Google Sheet!")
        print("🔍 Check your sheet - you should see:")
        print("   - NAICS_Code column with codes")
        print("   - Likely to Buy column with Yes/No")
    else:
        print("❌ Failed to write to Google Sheet")

if __name__ == "__main__":
    test_first_5()
