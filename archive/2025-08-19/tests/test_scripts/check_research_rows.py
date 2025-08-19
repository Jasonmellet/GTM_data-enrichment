#!/usr/bin/env python3
"""
Check which rows have research data in columns AV-AZ.
"""

import os
from dotenv import load_dotenv
from google_sheets_handler import GoogleSheetsHandler

# Load environment variables
load_dotenv()

def check_research_rows():
    """Check which rows have research data."""
    print("🔍 Checking Which Rows Have Research Data")
    print("=" * 50)
    
    # Load Google Sheet URL
    sheet_url = os.getenv('GOOGLE_SHEET_URL')
    if not sheet_url:
        print("❌ Google Sheet URL not found")
        return
    
    sheet_name = "Sheet1"
    
    # Initialize Google Sheets handler
    sheets_handler = GoogleSheetsHandler()
    
    try:
        # Check rows 1-20 for research data
        for row in range(1, 21):
            research_range = f"{sheet_name}!AV{row}:AZ{row}"
            research_data = sheets_handler.service.spreadsheets().values().get(
                spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                range=research_range
            ).execute()
            
            if research_data.get('values') and research_data['values'][0]:
                row_data = research_data['values'][0]
                # Check if there's actual content (not just empty cells)
                has_content = any(cell and str(cell).strip() for cell in row_data)
                if has_content:
                    print(f"Row {row}: Has research data")
                    # Show first column content as preview
                    first_col = row_data[0][:100] if row_data[0] else "Empty"
                    print(f"  Preview: {first_col}...")
                else:
                    print(f"Row {row}: Empty research data")
            else:
                print(f"Row {row}: No research data")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_research_rows()
