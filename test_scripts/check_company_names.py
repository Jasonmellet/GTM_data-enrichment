#!/usr/bin/env python3
"""
Check the actual company names in rows 1-5 to understand the correct row mapping.
"""

from google_sheets_handler import GoogleSheetsHandler

def check_company_names():
    """Check company names in rows 1-5 to understand the correct mapping."""
    print("🔍 Checking Company Names in Rows 1-5")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Check rows 1-5 to see the actual company names
        for row_num in range(1, 6):
            range_name = f"{sheet_name}!A{row_num}:E{row_num}"
            result = sheets_handler.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            if 'values' in result and len(result['values']) > 0:
                row_data = result['values'][0]
                company_name = row_data[0] if len(row_data) > 0 else "N/A"
                website = row_data[1] if len(row_data) > 1 else "N/A"
                job_title = row_data[2] if len(row_data) > 2 else "N/A"
                last_name = row_data[3] if len(row_data) > 3 else "N/A"
                first_name = row_data[4] if len(row_data) > 4 else "N/A"
                
                print(f"Row {row_num}: {company_name}")
                print(f"  Contact: {first_name} {last_name} - {job_title}")
                print(f"  Website: {website}")
                print()
            else:
                print(f"Row {row_num}: No data")
                print()
        
    except Exception as e:
        print(f"❌ Error checking company names: {e}")

if __name__ == "__main__":
    check_company_names()
