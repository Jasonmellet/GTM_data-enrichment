#!/usr/bin/env python3
"""
Script to check the research data written to columns AV-AZ.
"""

from google_sheets_handler import GoogleSheetsHandler

def check_research_data():
    """Check the research data in columns AV-AZ."""
    print("🔍 Checking Research Data in Spreadsheet")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        # Read the research columns (AV-AZ) for row 2 only
        # AV = index 48, AW = index 49, AX = index 50, AY = index 51, AZ = index 52
        range_name = f"{sheet_name}!AV2:AZ2"  # Only row 2
        
        result = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("❌ No data found in research columns")
            return
        
        print(f"📊 Found {len(values)} rows in research columns")
        
        # Display data for row 2 only
        print(f"\n📊 Research Data for Row 2 (8th Avenue Food and Provisions):")
        print("=" * 80)
        
        if len(values) > 0:
            row_data = values[0]
            print(f"\n🔍 Row 2 Data:")
            
            # Pad row to ensure we have all 5 columns
            padded_row = row_data + [''] * (5 - len(row_data))
            
            if len(padded_row) >= 5:
                print(f"  Column AV (Company Research): {len(padded_row[0])} chars")
                print(f"    Content: {padded_row[0][:200]}...")
                print(f"\n  Column AW (Contact Research): {len(padded_row[1])} chars")
                print(f"    Content: {padded_row[1][:200]}...")
                print(f"\n  Column AX (Pain Points): {len(padded_row[2])} chars")
                print(f"    Content: {padded_row[2][:200]}...")
                print(f"\n  Column AY (Opportunity): {len(padded_row[3])} chars")
                print(f"    Content: {padded_row[3][:200]}...")
                print(f"\n  Column AZ (Quality Score): {padded_row[4]}")
            else:
                print(f"  ❌ Incomplete data: {len(padded_row)} columns found")
        
        print(f"\n✅ Research data check completed for Row 2!")
        
    except Exception as e:
        print(f"❌ Error checking research data: {e}")

if __name__ == "__main__":
    check_research_data()
