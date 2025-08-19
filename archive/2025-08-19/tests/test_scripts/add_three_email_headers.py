#!/usr/bin/env python3
"""
Add headers for three separate emails with icebreaker, body, and CTA sections.
"""

from google_sheets_handler import GoogleSheetsHandler

def add_three_email_headers():
    """Add headers for three separate emails."""
    print("📝 Adding Three Email Column Headers")
    print("=" * 50)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Three email column headers (no greeting needed)
        email_headers = [
            # Email 1
            "Email 1 Subject",
            "Email 1 Icebreaker", 
            "Email 1 Body",
            "Email 1 CTA",
            
            # Email 2
            "Email 2 Subject",
            "Email 2 Icebreaker",
            "Email 2 Body", 
            "Email 2 CTA",
            
            # Email 3
            "Email 3 Subject",
            "Email 3 Icebreaker",
            "Email 3 Body",
            "Email 3 CTA"
        ]
        
        # Write headers to row 1, columns BA-BM (indices 53-64)
        print(f"📝 Adding headers for 3 emails to columns BA-BM...")
        
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
        
        print(f"✅ Three email column headers added successfully!")
        print(f"📊 Columns BA-BM now have proper email structure")
        print(f"🎯 Ready for 3 emails: Subject + Icebreaker + Body + CTA each")
        
        return True
        
    except Exception as e:
        print(f"❌ Error adding headers: {e}")
        return False

if __name__ == "__main__":
    add_three_email_headers()
