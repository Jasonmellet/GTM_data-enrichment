#!/usr/bin/env python3
"""
Check current email content in the Google Sheet for rows 2, 3, and 4.
"""

from google_sheets_handler import GoogleSheetsHandler

def check_current_emails():
    """Check the current email data in columns BA-BM for rows 2, 3, and 4."""
    print("🔍 Checking Current Email Data in Spreadsheet")
    print("=" * 60)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Check rows 2, 3, and 4, columns BA-BM (indices 53-64)
        print(f"📊 Checking rows 2, 3, 4 in columns BA-BM...")
        
        for row_num in [2, 3, 4]:
            print(f"\n📋 ROW {row_num}:")
            print("-" * 40)
            
            # Read the entire row data to see what's there
            range_name = f"{sheet_name}!A{row_num}:BM{row_num}"
            result = sheets_handler.service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range=range_name
            ).execute()
            
            if 'values' in result and len(result['values']) > 0:
                row_data = result['values'][0]
                
                # Get company name and contact info
                company_name = row_data[0] if len(row_data) > 0 else "N/A"
                contact_name = f"{row_data[4]} {row_data[3]}" if len(row_data) > 4 else "N/A"
                
                print(f"Company: {company_name}")
                print(f"Contact: {contact_name}")
                
                # Check email columns BA-BM (indices 53-64)
                email_columns = ['BA', 'BB', 'BC', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BK', 'BL']
                email_sections = [
                    'Email 1 Subject', 'Email 1 Icebreaker', 'Email 1 Body', 'Email 1 CTA',
                    'Email 2 Subject', 'Email 2 Icebreaker', 'Email 2 Body', 'Email 2 CTA',
                    'Email 3 Subject', 'Email 3 Icebreaker', 'Email 3 Body', 'Email 3 CTA'
                ]
                
                print(f"Email Data:")
                has_email_data = False
                for i, col_letter in enumerate(email_columns):
                    col_index = 53 + i  # BA starts at index 53
                    if col_index < len(row_data):
                        content = row_data[col_index] if row_data[col_index] else "EMPTY"
                        if content and content != "EMPTY":
                            has_email_data = True
                            # Truncate long content for display
                            display_content = content[:80] + "..." if len(content) > 80 else content
                            print(f"  {col_letter} ({email_sections[i]}): {display_content}")
                        else:
                            print(f"  {col_letter} ({email_sections[i]}): EMPTY")
                    else:
                        print(f"  {col_letter} ({email_sections[i]}): COLUMN NOT FOUND")
                
                if not has_email_data:
                    print(f"  ❌ No email data found for this row")
                
                # Check if research data exists (columns AV-AZ, indices 48-52)
                print(f"\nResearch Data Check:")
                research_columns = ['AV', 'AW', 'AX', 'AY', 'AZ']
                research_sections = ['Company Research', 'Contact Research', 'Pain Points', 'Opportunity', 'Quality Score']
                
                for i, col_letter in enumerate(research_columns):
                    col_index = 48 + i  # AV starts at index 48
                    if col_index < len(row_data):
                        content = row_data[col_index] if row_data[col_index] else "EMPTY"
                        has_content = "✅" if content and content != "EMPTY" else "❌"
                        print(f"  {col_letter} ({research_sections[i]}): {has_content}")
                    else:
                        print(f"  {col_letter} ({research_sections[i]}): COLUMN NOT FOUND")
                
            else:
                print(f"❌ No data found for row {row_num}")
        
        print(f"\n🔍 Summary Check Complete")
        print(f"📊 Checked rows 2, 3, 4 in columns A-BM")
        
    except Exception as e:
        print(f"❌ Error checking data: {e}")

if __name__ == "__main__":
    check_current_emails()
