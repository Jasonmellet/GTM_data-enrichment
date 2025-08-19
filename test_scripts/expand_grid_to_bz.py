#!/usr/bin/env python3
"""
Expand Google Sheet grid to column BZ to accommodate new email content columns.
"""

from google_sheets_handler import GoogleSheetsHandler

def expand_grid_to_bz():
    """Expand the sheet grid to column BZ."""
    print("🔧 Expanding Google Sheet Grid to Column BZ")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Column BZ is index 77 (0-indexed: A=0, B=1, ..., BZ=77)
        # We want to expand to at least column BZ, so we'll go to column CA (index 78) to be safe
        
        print(f"📊 Current grid limits: 52 columns (A-AZ)")
        print(f"🎯 Expanding to: 78+ columns (A-CA+)")
        
        # Expand the grid by updating sheet properties
        request = {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": 0,  # First sheet
                    "gridProperties": {
                        "columnCount": 80  # Expand to 80 columns (A-CB) to be safe
                    }
                },
                "fields": "gridProperties.columnCount"
            }
        }
        
        body = {"requests": [request]}
        
        result = sheets_handler.service.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body=body
        ).execute()
        
        print(f"✅ Grid expanded successfully!")
        print(f"📊 New grid should now support columns A-CB+")
        print(f"🎯 Ready to add email content columns BA-BE")
        
        # Verify the expansion by reading a few cells in the new columns
        print(f"\n🔍 Verifying grid expansion...")
        try:
            # Try to read from column BA (index 53)
            test_range = f"{sheet_name}!BA1:BA1"
            test_result = sheets_handler.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=test_range
            ).execute()
            
            print(f"✅ Column BA is now accessible")
            
        except Exception as e:
            print(f"❌ Column BA still not accessible: {e}")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error expanding grid: {e}")
        return False

if __name__ == "__main__":
    expand_grid_to_bz()
