#!/usr/bin/env python3
"""
Add headers for email content columns BA-BE.
"""

from google_sheets_handler import GoogleSheetsHandler

def add_email_column_headers():
    """Add headers for email content columns BA-BE."""
    print("📝 Adding Email Content Column Headers")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Email content column headers
        email_headers = [
            "Email Subject Line",
            "Email Greeting", 
            "Email Introduction",
            "Email Body",
            "Email Call to Action"
        ]
        
        # Write headers to row 1, columns BA-BE (indices 53-57)
        print(f"📝 Adding headers to columns BA-BE...")
        
        for i, header in enumerate(email_headers):
            col_index = 53 + i  # Start from BA (index 53)
            col_letter = sheets_handler._get_column_letter(col_index)
            range_name = f"{sheet_name}!{col_letter}1"
            
            print(f"  📝 Column {col_letter}: {header}")
            
            sheets_handler.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body={'values': [[header]]}
            ).execute()
        
        print(f"✅ Email column headers added successfully!")
        print(f"📊 Columns BA-BE now have descriptive headers")
        
        return True
        
    except Exception as e:
        print(f"❌ Error adding headers: {e}")
        return False

if __name__ == "__main__":
    add_email_column_headers()
