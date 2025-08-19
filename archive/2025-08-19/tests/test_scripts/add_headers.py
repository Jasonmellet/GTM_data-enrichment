#!/usr/bin/env python3
"""
Quick script to add column headers to columns AT and AU.
"""

from google_sheets_handler import GoogleSheetsHandler

def add_headers():
    """Add column headers to AT and AU."""
    print("📝 Adding column headers...")
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    # Use the specific columns AT and AU
    naics_col_index = 45  # Column AT
    likely_to_buy_col_index = 46  # Column AU
    
    try:
        # Write NAICS header to column AT, row 1
        naics_header_range = f"{sheet_name}!{sheets_handler._get_column_letter(naics_col_index)}1"
        sheets_handler.service.spreadsheets().values().update(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=naics_header_range,
            valueInputOption='RAW',
            body={'values': [['NAICS Code']]}
        ).execute()
        
        # Write Likely to Buy header to column AU, row 1
        likely_header_range = f"{sheet_name}!{sheets_handler._get_column_letter(likely_to_buy_col_index)}1"
        sheets_handler.service.spreadsheets().values().update(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=likely_header_range,
            valueInputOption='RAW',
            body={'values': [['Likely to Buy']]}
        ).execute()
        
        print(f"✅ Added headers:")
        print(f"   - Column {sheets_handler._get_column_letter(naics_col_index)}: 'NAICS Code'")
        print(f"   - Column {sheets_handler._get_column_letter(likely_to_buy_col_index)}: 'Likely to Buy'")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    add_headers()
