#!/usr/bin/env python3
"""
Test Email Content Generation using OpenAI - Based on research data from columns AV-AZ.
"""

import os
import openai
import pandas as pd
from dotenv import load_dotenv
from google_sheets_handler import GoogleSheetsHandler

def main():
    """Generate cold email content using OpenAI based on research data."""
    print("🧪 Email Content Generation using OpenAI - Testing rows 2, 3, 4")
    print("=" * 70)
    
    load_dotenv()
    openai_api_key = os.getenv('OPENAI_API_KEY')
    
    if not openai_api_key:
        print("❌ OPENAI_API_KEY not found in .env file")
        return
    
    print(f"✅ OpenAI API Key loaded: {openai_api_key[:20]}...")
    
    # Initialize OpenAI client
    client = openai.OpenAI(api_key=openai_api_key)
    
    # Initialize Google Sheets handler
    sheets_handler = GoogleSheetsHandler()
    sheet_url = "1unIqiZBqP0fSHpF-K8jDSMEluhIZ-crPlsXCNPJCYVg"
    sheet_name = "Sheet1"
    
    # Read data from Google Sheets
    print(f"📖 Reading data from Google Sheets...")
    try:
        df = sheets_handler.read_sheet_data(sheet_url, sheet_name)
        if df is None or df.empty:
            print("❌ Failed to read sheet data")
            return
        
        print(f"📊 Found {len(df)} rows of data")
        
    except Exception as e:
        print(f"❌ Error reading sheet: {e}")
        return
    
    print(f"🔍 Generating email content for rows 2, 3, and 4...")
    
    # Process rows 2, 3, and 4 (indices 1, 2, 3 in 0-indexed)
    for idx in range(1, 4):  # Process rows 2, 3, 4
        row = df.iloc[idx]
        
        company_name = row.iloc[0]  # Company Name
        website = row.iloc[1] if len(row) > 1 else None  # Website
        job_title = row.iloc[2] if len(row) > 2 else None  # Job Title
        last_name = row.iloc[3] if len(row) > 3 else None  # Last Name
        first_name = row.iloc[4] if len(row) > 4 else None  # First Name
        
        # Construct full contact name
        contact_name = f"{first_name} {last_name}" if first_name and last_name else f"{first_name or last_name or 'Unknown'}"
        
        print(f"\n📊 Processing Row {idx + 1}: {company_name}")
        print(f"  Contact: {contact_name} - {job_title}")
        print(f"  Website: {website}")
        
        # Read research data from columns AV-AZ (indices 47-51)
        try:
            # Read research data for this row
            research_range = f"{sheet_name}!AV{idx + 1}:AZ{idx + 1}"
            research_result = sheets_handler.service.spreadsheets().values().get(
                spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                range=research_range
            ).execute()
            
            research_values = research_result.get('values', [[]])[0]
            
            if len(research_values) >= 5:
                company_research = research_values[0] if research_values[0] else "No company research available"
                contact_research = research_values[1] if research_values[1] else "No contact research available"
                pain_points = research_values[2] if research_values[2] else "No pain points available"
                opportunity = research_values[3] if research_values[3] else "No opportunity assessment available"
                quality_score = research_values[4] if research_values[4] else "No quality score"
                
                print(f"    ✅ Research data loaded: Company ({len(company_research)} chars), Contact ({len(contact_research)} chars)")
                
            else:
                print(f"    ❌ Insufficient research data found")
                continue
                
        except Exception as e:
            print(f"    ❌ Error reading research data: {e}")
            continue
        
        # Generate cold email content using OpenAI
        print(f"    🤖 Generating email content with OpenAI...")
        
        email_prompt = f"""Based on the following research data, create a personalized cold email for selling bulk heat-stable cream cheese to {contact_name} at {company_name}.

RESEARCH DATA:
Company: {company_name}
Contact: {contact_name} - {job_title}
Company Research: {company_research}
Contact Research: {contact_research}
Pain Points: {pain_points}
Opportunity: {opportunity}

REQUIREMENTS:
1. SUBJECT LINE: Under 50 characters, personalized, action-oriented
2. GREETING: Personalized salutation using first name
3. INTRODUCTION/HOOK: 2-4 sentences explaining why you're emailing, reference their business
4. BODY: 100-150 words explaining how heat-stable cream cheese solves their pain points
5. CALL TO ACTION: Clear next step (email reply, website visit, or request free sample)

Focus on their specific needs and how heat-stable cream cheese from Schreiber Foods can help their business. Be professional but conversational.

Return the content in this exact format:
SUBJECT: [subject line]
GREETING: [greeting]
INTRODUCTION: [introduction]
BODY: [body]
CTA: [call to action]"""
        
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert sales copywriter specializing in B2B food industry sales. Create compelling, personalized cold emails that convert."},
                    {"role": "user", "content": email_prompt}
                ],
                max_tokens=800,
                temperature=0.7
            )
            
            if response.choices and len(response.choices) > 0:
                email_content = response.choices[0].message.content.strip()
                print(f"      ✅ Email content generated: {len(email_content)} chars")
                
                # Parse the email content into sections
                email_sections = parse_email_content(email_content)
                
                # Write email content to columns BA-BE
                print(f"    📝 Writing email content to sheet (Row {idx + 1}, Columns BA-BE)...")
                try:
                    # Write to columns BA (53), BB (54), BC (55), BD (56), BE (57)
                    for i, (section_name, content) in enumerate(email_sections.items()):
                        col_index = 53 + i  # Start from BA (index 53)
                        col_letter = sheets_handler._get_column_letter(col_index)
                        range_name = f"{sheet_name}!{col_letter}{idx + 1}"
                        
                        print(f"      📝 Column {col_letter} ({section_name}): {range_name}")
                        
                        # Clean the content
                        clean_content = content.replace('\n', ' ').replace('\r', ' ').strip() if content else "No content"
                        
                        sheets_handler.service.spreadsheets().values().update(
                            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                            range=range_name,
                            valueInputOption='RAW',
                            body={'values': [[clean_content]]}
                        ).execute()
                    
                    print(f"      ✅ Email content written to Row {idx + 1}, Columns BA-BE")
                    
                except Exception as e:
                    print(f"      ❌ Failed to write email content: {e}")
                
                # Display generated content
                print(f"\n📧 Generated Email Content for {contact_name}:")
                print("-" * 50)
                for section_name, content in email_sections.items():
                    print(f"{section_name.upper()}: {content[:100]}...")
                
            else:
                print(f"      ❌ No email content generated")
                
        except Exception as e:
            print(f"      ❌ Error generating email content: {e}")
        
        print("-" * 60)
    
    print(f"\n🎉 Email content generation completed for Rows 2, 3, and 4!")
    print("🔍 Check your Google Sheet - Rows 2-4, Columns BA-BE should now contain cold email content")

def parse_email_content(email_content):
    """Parse the email content into sections."""
    sections = {
        'subject': '',
        'greeting': '',
        'introduction': '',
        'body': '',
        'cta': ''
    }
    
    lines = email_content.split('\n')
    current_section = None
    
    for line in lines:
        line = line.strip()
        if line.startswith('SUBJECT:'):
            current_section = 'subject'
            sections['subject'] = line.replace('SUBJECT:', '').strip()
        elif line.startswith('GREETING:'):
            current_section = 'greeting'
            sections['greeting'] = line.replace('GREETING:', '').strip()
        elif line.startswith('INTRODUCTION:'):
            current_section = 'introduction'
            sections['introduction'] = line.replace('INTRODUCTION:', '').strip()
        elif line.startswith('BODY:'):
            current_section = 'body'
            sections['body'] = line.replace('BODY:', '').strip()
        elif line.startswith('CTA:'):
            current_section = 'cta'
            sections['cta'] = line.replace('CTA:', '').strip()
        elif current_section and line:
            # Continue content for current section
            sections[current_section] += ' ' + line
    
    return sections

if __name__ == "__main__":
    main()
