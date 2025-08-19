#!/usr/bin/env python3
"""
Generate three emails for the correct companies: AbiMar Foods (row 3), ACH Food Companies (row 4), and Andros Foods (row 5).
"""

import os
import openai
import pandas as pd
from google_sheets_handler import GoogleSheetsHandler
from dotenv import load_dotenv

def generate_correct_emails():
    """Generate three emails for the correct companies."""
    print("📧 Generating Three Emails for Correct Companies")
    print("=" * 60)
    
    # Load OpenAI API key
    load_dotenv()
    openai_api_key = os.getenv('OPENAI_API_KEY')
    
    if not openai_api_key:
        print("❌ OpenAI API key not found in .env file")
        return
    
    print(f"✅ OpenAI API Key loaded: {openai_api_key[:20]}...")
    
    # Initialize OpenAI client
    client = openai.OpenAI(api_key=openai_api_key)
    
    # Initialize Google Sheets handler
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    try:
        # Read data from Google Sheets
        print("📖 Reading data from Google Sheets...")
        data = sheets_handler.read_sheet_data(sheet_url, sheet_name)
        
        if data is None or data.empty:
            print("❌ No data found in sheet")
            return
        
        # Process the correct rows: 3, 4, 5 (AbiMar, ACH, Andros)
        target_rows = [3, 4, 5]  # These are the actual sheet rows
        
        for sheet_row in target_rows:
            # Convert sheet row to data index (sheet row 3 = data index 2)
            data_idx = sheet_row - 1
            
            if data_idx >= len(data):
                print(f"❌ Data index {data_idx} out of range")
                continue
                
            row = data.iloc[data_idx]
            company_name = row.iloc[0] if len(row) > 0 else "Unknown Company"
            website = row.iloc[1] if len(row) > 1 else "No website"
            job_title = row.iloc[2] if len(row) > 2 else "Unknown Title"
            last_name = row.iloc[3] if len(row) > 3 else "Unknown"
            first_name = row.iloc[4] if len(row) > 4 else "Unknown"
            
            print(f"\n📊 Processing Sheet Row {sheet_row}: {company_name}")
            print(f"  Contact: {first_name} {last_name} - {job_title}")
            print(f"  Website: {website}")
            
            # Load research data from columns AV-AZ (indices 48-52)
            research_data = {}
            research_columns = ['company_research', 'contact_research', 'pain_points', 'opportunity_match', 'quality_score']
            
            for i, col_name in enumerate(research_columns):
                col_index = 48 + i  # AV-AZ
                if col_index < len(row):
                    research_data[col_name] = str(row.iloc[col_index]) if pd.notna(row.iloc[col_index]) else ""
                else:
                    research_data[col_name] = ""
            
            # Check if we have research data
            if not research_data.get('company_research') or research_data['company_research'] == "":
                print(f"    ❌ No research data found for {company_name}")
                continue
            
            print(f"    ✅ Research data loaded: Company ({len(research_data['company_research'])} chars), Contact ({len(research_data['contact_research'])} chars)")
            
            # Generate three emails with OpenAI
            print(f"    🤖 Generating three emails with OpenAI...")
            
            try:
                # Create focused prompt for the specific company
                prompt = f"""Create three separate, highly personalized cold emails for {first_name} {last_name} at {company_name} ({website}) as a {job_title}.

COMPANY CONTEXT: {research_data['company_research']}
CONTACT CONTEXT: {research_data['contact_research']}
PAIN POINTS: {research_data['pain_points']}
OPPORTUNITY: {research_data['opportunity_match']}

Create THREE separate emails that are:
- HUMAN-SOUNDING (not AI-generated)
- SPECIFIC to {company_name} and their business
- REFERENCE their website, products, industry trends
- MENTION current market challenges (2024-2025)
- NO greetings like "Hi [Name]"
- NO personal names in icebreaker - focus on company/role/industry

Each email structure:
1. SUBJECT LINE (max 50 chars, specific to their business, NO "Elevate")
2. ICEBREAKER (1-2 sentences, reference something specific about their company/role, NO greeting, NO names)
3. BODY (2-3 sentences, explain value proposition for THEIR specific needs)
4. CTA (1 sentence, specific action)

Email 1 CTA: "Simply reply to this email"
Email 2 CTA: "Visit our website at [website]"
Email 3 CTA: "Request a free sample"

TOTAL word count for each email: MAX 150 words (icebreaker + body + CTA combined).

Format exactly like this:
EMAIL 1:
SUBJECT: [subject line]
ICEBREAKER: [icebreaker text]
BODY: [body text]
CTA: [CTA text]

EMAIL 2:
SUBJECT: [subject line]
ICEBREAKER: [icebreaker text]
BODY: [body text]
CTA: [CTA text]

EMAIL 3:
SUBJECT: [subject line]
ICEBREAKER: [icebreaker text]
BODY: [body text]
CTA: [CTA text]"""

                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=1000,
                    temperature=0.8
                )
                
                if response.choices and len(response.choices) > 0:
                    content = response.choices[0].message.content.strip()
                    print(f"      ✅ Three emails generated: {len(content)} chars")
                    
                    # Parse the three emails
                    emails = parse_three_emails_fixed(content)
                    
                    if emails:
                        # Write emails to columns BA-BM (indices 53-64) for the correct sheet row
                        print(f"    📝 Writing three emails to sheet (Row {sheet_row}, Columns BA-BM)...")
                        
                        for email_num, email_data in emails.items():
                            for section, text in email_data.items():
                                col_index = get_email_column_index_fixed(email_num, section)
                                col_letter = sheets_handler._get_column_letter(col_index)
                                range_name = f"{sheet_name}!{col_letter}{sheet_row}"
                                
                                print(f"      📝 Column {col_letter} (Email {email_num} {section}): {range_name}")
                                
                                # Clean the content
                                clean_text = text.replace('\n', ' ').replace('\r', ' ').strip() if text else "No content"
                                
                                sheets_handler.service.spreadsheets().values().update(
                                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                                    range=range_name,
                                    valueInputOption='RAW',
                                    body={'values': [[clean_text]]}
                                ).execute()
                        
                        print(f"      ✅ Three emails written to Row {sheet_row}, Columns BA-BM")
                        
                        # Display email summaries
                        print(f"\n📧 Generated Three Emails for {first_name} {last_name} at {company_name}:")
                        print("-" * 60)
                        for email_num, email_data in emails.items():
                            print(f"EMAIL {email_num}:")
                            print(f"  Subject: {email_data.get('subject', 'N/A')}")
                            print(f"  Icebreaker: {email_data.get('icebreaker', 'N/A')[:80]}...")
                            print(f"  Body: {email_data.get('body', 'N/A')[:80]}...")
                            print(f"  CTA: {email_data.get('cta', 'N/A')}")
                            print()
                        
                    else:
                        print(f"      ❌ Failed to parse emails")
                        
                else:
                    print(f"      ❌ No response from OpenAI")
                    
            except Exception as e:
                print(f"      ❌ Error generating emails: {e}")
                continue
        
        print(f"\n🎉 Email generation completed for Rows 3, 4, and 5!")
        print(f"🔍 Check your Google Sheet - Rows 3-5, Columns BA-BM should now contain three cold emails each")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def parse_three_emails_fixed(content):
    """Parse the OpenAI response into three structured emails."""
    emails = {}
    
    try:
        lines = content.split('\n')
        current_email = None
        current_section = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('EMAIL '):
                email_num = line.split(':')[0].split()[1]
                current_email = email_num
                emails[email_num] = {}
                
            elif line.startswith('SUBJECT:'):
                current_section = 'subject'
                emails[current_email]['subject'] = line.split(':', 1)[1].strip()
                
            elif line.startswith('ICEBREAKER:'):
                current_section = 'icebreaker'
                emails[current_email]['icebreaker'] = line.split(':', 1)[1].strip()
                
            elif line.startswith('BODY:'):
                current_section = 'body'
                emails[current_email]['body'] = line.split(':', 1)[1].strip()
                
            elif line.startswith('CTA:'):
                current_section = 'cta'
                emails[current_email]['cta'] = line.split(':', 1)[1].strip()
                
            elif current_section and current_email:
                # Append to current section if it's continuation
                if current_section in emails[current_email]:
                    emails[current_email][current_section] += ' ' + line
                else:
                    emails[current_email][current_section] = line
        
        return emails
        
    except Exception as e:
        print(f"Error parsing emails: {e}")
        return None

def get_email_column_index_fixed(email_num, section):
    """Get the column index for a specific email section."""
    # Email 1: BA-BD (53-56), Email 2: BE-BH (57-60), Email 3: BI-BL (61-64)
    section_map = {'subject': 0, 'icebreaker': 1, 'body': 2, 'cta': 3}
    
    if email_num == '1':
        return 53 + section_map[section]
    elif email_num == '2':
        return 57 + section_map[section]
    elif email_num == '3':
        return 61 + section_map[section]
    else:
        return 53

if __name__ == "__main__":
    generate_correct_emails()
