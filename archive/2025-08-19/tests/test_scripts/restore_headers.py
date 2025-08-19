#!/usr/bin/env python3
"""
Script to restore original column headers and properly add new research columns.
"""

from google_sheets_handler import GoogleSheetsHandler

def restore_and_add_columns():
    """Restore original headers and properly add new columns."""
    print("🔧 Restoring Original Headers and Adding New Columns")
    print("=" * 60)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    # Original column headers from your sheet
    original_headers = [
        "Company Name", "Website", "Job Title", "Last Name", "First Name", 
        "Direct Phone Number", "Email Address", "Contact #", "Mobile phone", 
        "Person Street", "Person City", "Person State", "Person Zip Code", 
        "Country", "Founded Year", "Company HQ Phone", "Revenue Range (in USD)", 
        "Company Street Address", "Company City", "Company State", 
        "Company Zip Code", "Company Country", "Full Address", "Result", 
        "First Name", "Last Name", "Title", "Person Linkedin Url", "City", 
        "State", "Country", "Email", "Company", "Website", "Industry", 
        "# Employees", "Annual Revenue", "Total Funding", "Company Linkedin Url", 
        "Company Street", "Company City", "Company Postal Code", "Company State", 
        "Company Country", "Company Founded Year", "NAICS Code", "Likely to Buy"
    ]
    
    # New research columns to add
    new_columns = [
        "Company Research Summary",
        "Contact Research Summary", 
        "Industry Pain Points",
        "Schreiber Opportunity Match",
        "Research Quality Score"
    ]
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Step 1: Restore original headers
        print("📝 Step 1: Restoring original column headers...")
        sheets_handler.service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='RAW',
            body={'values': [original_headers]}
        ).execute()
        print(f"✅ Restored {len(original_headers)} original columns")
        
        # Step 2: Add new research columns
        print(f"\n📝 Step 2: Adding {len(new_columns)} new research columns...")
        
        for i, column_name in enumerate(new_columns):
            col_index = len(original_headers) + i
            col_letter = sheets_handler._get_column_letter(col_index)
            
            print(f"📝 Adding column {col_letter}: {column_name}")
            
            sheets_handler.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=f"{sheet_name}!{col_letter}1",
                valueInputOption='RAW',
                body={'values': [[column_name]]}
            ).execute()
        
        print(f"\n✅ Successfully restored headers and added new columns!")
        print(f"📊 Total columns: {len(original_headers) + len(new_columns)}")
        print(f"📋 New columns added:")
        for i, column_name in enumerate(new_columns):
            col_letter = sheets_handler._get_column_letter(len(original_headers) + i)
            print(f"  {col_letter}: {column_name}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    restore_and_add_columns()
