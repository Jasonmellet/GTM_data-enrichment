#!/usr/bin/env python3
"""
Convert plain text URLs in email CTAs to actual clickable hyperlinks.
"""

from google_sheets_handler import GoogleSheetsHandler

def add_hyperlinks():
    """Convert plain text URLs in email CTAs to clickable hyperlinks."""
    print("🔗 Converting Email CTAs to Clickable Hyperlinks")
    print("=" * 60)
    
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        sheet_id = sheets_handler.extract_sheet_id_from_url(sheet_url)
        
        # Process rows 2, 3, and 4
        target_rows = [2, 3, 4]
        
        for sheet_row in target_rows:
            print(f"\n📊 Processing Row {sheet_row}...")
            
            # Email 2 CTA (column BH) - Website visit
            email2_cta_col = "BH"
            email2_cta_range = f"{sheet_name}!{email2_cta_col}{sheet_row}"
            
            # Email 3 CTA (column BL) - Sample request
            email3_cta_col = "BL"
            email3_cta_range = f"{sheet_name}!{email3_cta_col}{sheet_row}"
            
            try:
                # Read current CTA content
                email2_result = sheets_handler.service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=email2_cta_range
                ).execute()
                
                email3_result = sheets_handler.service.spreadsheets().values().get(
                    spreadsheetId=sheet_id,
                    range=email3_cta_range
                ).execute()
                
                # Extract current text
                email2_text = email2_result.get('values', [['']])[0][0] if email2_result.get('values') else ""
                email3_text = email3_result.get('values', [['']])[0][0] if email3_result.get('values') else ""
                
                print(f"  📝 Email 2 CTA (current): {email2_text[:80]}...")
                print(f"  📝 Email 3 CTA (current): {email3_text[:80]}...")
                
                # Create hyperlink formulas with supporting text
                # Email 2: Website visit
                if "schreiberfoodsproducts.com/about/" in email2_text:
                    # Create compelling supporting text for website visit
                    supporting_text = "Ready to learn more? Visit our website to explore our full range of heat-stable cream cheese solutions and see how we can transform your product line."
                    hyperlink_formula = f'=HYPERLINK("https://www.schreiberfoodsproducts.com/about/", "Visit our website")'
                    
                    # Write the supporting text with hyperlink
                    full_cta = f'{supporting_text} {hyperlink_formula}'
                    
                    sheets_handler.service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=email2_cta_range,
                        valueInputOption='USER_ENTERED',  # This allows formulas to work
                        body={'values': [[full_cta]]}
                    ).execute()
                    
                    print(f"    ✅ Email 2 CTA updated with supporting text and hyperlink")
                
                # Email 3: Sample request
                if "schreiberfoodsproducts.com/request-sample/" in email3_text:
                    # Create compelling supporting text for sample request
                    supporting_text = "Experience the difference firsthand! Request a free sample of our heat-stable cream cheese and see how it can revolutionize your formulations."
                    hyperlink_formula = f'=HYPERLINK("https://www.schreiberfoodsproducts.com/request-sample/", "Request a free sample")'
                    
                    # Write the supporting text with hyperlink
                    full_cta = f'{supporting_text} {hyperlink_formula}'
                    
                    sheets_handler.service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=email3_cta_range,
                        valueInputOption='USER_ENTERED',  # This allows formulas to work
                        body={'values': [[full_cta]]}
                    ).execute()
                    
                    print(f"    ✅ Email 3 CTA updated with supporting text and hyperlink")
                
            except Exception as e:
                print(f"    ❌ Error processing row {sheet_row}: {e}")
                continue
        
        print(f"\n🎉 Hyperlink conversion completed!")
        print(f"🔗 Email 2 CTAs now link to: https://www.schreiberfoodsproducts.com/about/")
        print(f"🔗 Email 3 CTAs now link to: https://www.schreiberfoodsproducts.com/request-sample/")
        print(f"📊 Check your Google Sheet - CTAs should now be clickable blue links")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    add_hyperlinks()
