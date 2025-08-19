#!/usr/bin/env python3
"""
Script to add new research columns to the Google Sheet.
"""

from google_sheets_handler import GoogleSheetsHandler

def add_research_columns():
    """Add new research columns to the spreadsheet."""
    print("📝 Adding New Research Columns to Google Sheet")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    # Column definitions (after AT & AU)
    new_columns = [
        "Company Research Summary",
        "Contact Research Summary", 
        "Industry Pain Points",
        "Schreiber Opportunity Match",
        "Research Quality Score"
    ]
    
    try:
        # Get current sheet info
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Read current headers
        result = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=f"{sheet_name}!A1:ZZ1"
        ).execute()
        
        headers = result.get('values', [[]])[0]
        print(f"📊 Current columns: {len(headers)}")
        print(f"📋 Last column: {sheets_handler._get_column_letter(len(headers) - 1)}")
        
        # Add new columns starting after the last existing column
        start_col = len(headers)
        
        print(f"🔧 Adding {len(new_columns)} new columns...")
        
        # Try to add columns by appending to the header row
        try:
            # Append new headers to the existing header row
            new_headers = [""] * start_col + new_columns
            
            sheets_handler.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='RAW',
                body={'values': [new_headers]}
            ).execute()
            
            print(f"✅ Successfully added {len(new_columns)} new columns!")
            
        except Exception as e:
            print(f"❌ Failed to add columns in batch: {e}")
            print("🔄 Trying individual column addition...")
            
            # Fallback: try adding columns one by one
            for i, column_name in enumerate(new_columns):
                try:
                    # Try to add to the next available column
                    col_letter = sheets_handler._get_column_letter(start_col + i)
                    print(f"📝 Adding column {col_letter}: {column_name}")
                    
                    sheets_handler.service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=f"{sheet_name}!{col_letter}1",
                        valueInputOption='RAW',
                        body={'values': [[column_name]]}
                    ).execute()
                    
                    print(f"✅ Added column {col_letter}")
                    
                except Exception as col_error:
                    print(f"❌ Failed to add column {col_letter}: {col_error}")
                    break
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    add_research_columns()
