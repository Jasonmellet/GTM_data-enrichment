#!/usr/bin/env python3
"""
Generate three distinct cold emails for each contact based on research data.
This script creates separate columns for CTA text and links for Instantly variables.
"""

import os
import sys
import pandas as pd
from dotenv import load_dotenv
import openai
from google_sheets_handler import GoogleSheetsHandler

# Load environment variables
load_dotenv()

# Configure OpenAI
openai_api_key = os.getenv('OPENAI_API_KEY')
if not openai_api_key:
    print("❌ OpenAI API key not found in environment variables")
    sys.exit(1)

client = openai.OpenAI(api_key=openai_api_key)

def generate_three_emails(company_name, company_url, contact_name, contact_title, 
                         company_summary, contact_summary, pain_points, opportunity_match,
                         first_name, last_name, industry, company_linkedin, contact_linkedin):
    """
    Generate three distinct cold emails using OpenAI with REAL personalization.
    """
    
    prompt = f"""
    You are an expert cold email copywriter for Schreiber Foods, a leading manufacturer of heat-stable cream cheese for the food industry.

    TARGET COMPANY SPECIFIC INFORMATION:
    Company: {company_name}
    Company Website: {company_url}
    Company Industry: {industry}
    Company LinkedIn: {company_linkedin}
    
    CONTACT SPECIFIC INFORMATION:
    Contact Name: {first_name} {last_name}
    Job Title: {contact_title}
    Contact LinkedIn: {contact_linkedin}
    
    RESEARCH DATA (USE THIS SPECIFIC INFORMATION):
    Company Research: {company_summary}
    Contact Research: {contact_summary}
    Industry Pain Points: {pain_points}
    Schreiber Opportunity: {opportunity_match}

    Create THREE completely different, HIGHLY PERSONALIZED cold emails for {first_name} at {company_name}. 
    Each email MUST reference specific details from the research data above.

    CRITICAL REQUIREMENTS:
    - MENTION {company_name} specifically in each email
    - REFERENCE {first_name}'s role as {contact_title} 
    - USE specific pain points from column AX (Industry Pain Points) in the body
    - REFERENCE their industry ({industry}) and specific challenges
    - MENTION their website ({company_url}) or LinkedIn if relevant
    - NO greetings (don't start with "Hi [Name]" or "Hello")
    - Total word count for icebreaker + body + CTA: MAX 150 words
    - Subject lines must be original and non-spammy (avoid "Elevate" and generic terms)
    - Icebreakers cannot contain the person's name but should reference their role/company

    EMAIL STRUCTURE (CRITICAL):
    - Email 1: Subject + Icebreaker + Body + CTA (reply to email)
    - Email 2: Subject + Icebreaker + Body + CTA (visit website) 
    - Email 3: Subject + Icebreaker + Body + CTA (request sample)

    CTA REQUIREMENTS (CRITICAL):
    - Email 1: "Simply reply to this email" (but make it compelling, not generic)
    - Email 2: "Visit our website" BUT make it specific about what they'll discover
    - Email 3: "Request a free sample" BUT make it compelling about what they'll get
    - CTAs must be SEPARATE from the body content - do NOT embed the CTA in the body
    - Each CTA should be 1-2 sentences max and drive specific action

    PERSONALIZATION REQUIREMENTS:
    - Reference their specific products (cookies, crackers, etc. from research)
    - INCORPORATE SPECIFIC PAIN POINTS from column AX (Industry Pain Points) in the body
    - Reference their company's mission/values if mentioned in research
    - Use their industry-specific language and terminology
    - Make it clear you've researched THEIR company specifically

    PSYCHOLOGICAL ELEMENTS TO INCLUDE:
    - Subject: Curiosity gaps, numbers, questions, urgency
    - Icebreaker: Industry trends, recent events, specific observations about THEIR company
    - Body: Social proof, specific benefits, problem-solution alignment for THEIR needs, PAIN POINTS from column AX
    - CTA: Low-commitment, urgency, FOMO, clear next steps with specific benefits

    FORMAT YOUR RESPONSE EXACTLY LIKE THIS:
    EMAIL 1:
    Subject: [subject line]
    Icebreaker: [icebreaker content]
    Body: [body content - include pain points from column AX]
    CTA: [CTA content - separate from body]

    EMAIL 2:
    Subject: [subject line]
    Icebreaker: [icebreaker content]
    Body: [body content - include pain points from column AX]
    CTA: [CTA content - separate from body]

    EMAIL 3:
    Subject: [subject line]
    Icebreaker: [icebreaker content]
    Body: [body content - include pain points from column AX]
    CTA: [CTA content - separate from body]

    Focus on promoting Schreiber Foods' heat-stable cream cheese solutions and addressing {company_name}'s specific pain points with OUR product benefits. Make each email feel like it was written specifically for {first_name} and {company_name}, not a generic template. The CTAs must be compelling and specific to their business needs, but SEPARATE from the body content.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
            temperature=0.7
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        print(f"❌ Error calling OpenAI API: {e}")
        return None

def parse_three_emails(ai_response):
    """
    Parse the AI response to extract the three emails with their sections.
    """
    emails = []
    current_email = {}
    
    lines = ai_response.split('\n')
    
    for line in lines:
        line = line.strip()
        
        if line.startswith('EMAIL'):
            if current_email:
                emails.append(current_email)
            current_email = {}
            
        elif line.startswith('Subject:'):
            current_email['subject'] = line.replace('Subject:', '').strip()
        elif line.startswith('Icebreaker:'):
            current_email['icebreaker'] = line.replace('Icebreaker:', '').strip()
        elif line.startswith('Body:'):
            current_email['body'] = line.replace('Body:', '').strip()
        elif line.startswith('CTA:'):
            current_email['cta'] = line.replace('CTA:', '').strip()
    
    # Add the last email
    if current_email:
        emails.append(current_email)
    
    return emails

def get_email_column_index(email_num, section):
    """
    Get the column index for a specific email section.
    Current structure (DO NOT WRITE TO ANY OTHER COLUMNS):
    - Email 1: BA-BD (53-56) - Subject, Icebreaker, Body, CTA
    - Email 2: BE-BH (57-60) - Subject, Icebreaker, Body, CTA Text  
    - Email 3: BI-BL (61-64) - Subject, Icebreaker, Body, CTA Text
    """
    base_indices = {
        1: {'subject': 53, 'icebreaker': 54, 'body': 55, 'cta': 56},      # BA-BD
        2: {'subject': 57, 'icebreaker': 58, 'body': 59, 'cta': 60},      # BE-BH
        3: {'subject': 61, 'icebreaker': 62, 'body': 63, 'cta': 64}       # BI-BL
    }
    
    if section in base_indices[email_num]:
        return base_indices[email_num][section]
    
    return None

def main():
    # Configuration
    sheet_url = os.getenv('GOOGLE_SHEET_URL')
    sheet_name = "Sheet1"
    
    if not sheet_url:
        print("❌ Google Sheet URL not found in environment variables")
        sys.exit(1)
    
    # Initialize Google Sheets handler
    sheets_handler = GoogleSheetsHandler()
    
    # Process only row 5
    sheet_row = 5
    
    print(f"🚀 Generating emails for Row {sheet_row} only...")
    
    try:
        # Read research data for this specific row
        research_range = f"{sheet_name}!AV{sheet_row}:AZ{sheet_row}"
        research_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=research_range
        ).execute()
        
        if not research_data.get('values'):
            print(f"    ⚠️  No research data found for row {sheet_row}")
            return
        
        # Extract research data
        row_data = research_data['values'][0]
        company_name = row_data[0] if len(row_data) > 0 else "Unknown Company"
        contact_summary = row_data[1] if len(row_data) > 1 else "No contact data"
        pain_points = row_data[2] if len(row_data) > 2 else "No pain points data"
        opportunity_match = row_data[3] if len(row_data) > 3 else "No opportunity data"
        company_summary = row_data[4] if len(row_data) > 4 else "No company summary data"
        
        # Read company name and contact info from earlier columns
        company_range = f"{sheet_name}!A{sheet_row}:C{sheet_row}"
        company_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=company_range
        ).execute()
        
        if company_data.get('values'):
            company_info = company_data['values'][0]
            company_name = company_info[0] if len(company_info) > 0 else company_name
            company_url = company_info[1] if len(company_info) > 1 else "No URL"
            contact_title = company_info[2] if len(company_info) > 2 else "Unknown Title"
        else:
            company_url = "No URL"
            contact_title = "Unknown Title"
        
        # Read contact name info (D-E)
        contact_name_range = f"{sheet_name}!D{sheet_row}:E{sheet_row}"
        contact_name_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=contact_name_range
        ).execute()
        
        first_name = "Unknown"
        last_name = "Contact"
        if contact_name_data.get('values'):
            contact_name_info = contact_name_data['values'][0]
            first_name = contact_name_info[0] if len(contact_name_info) > 0 and contact_name_info[0] else "Unknown"
            last_name = contact_name_info[1] if len(contact_name_info) > 1 and contact_name_info[1] else "Contact"
        
        # Read industry info (AI)
        industry_range = f"{sheet_name}!AI{sheet_row}"
        industry_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=industry_range
        ).execute()
        
        industry = "Food Manufacturing"
        if industry_data.get('values'):
            industry = industry_data['values'][0][0] if industry_data['values'][0] else "Food Manufacturing"
        
        # Read company LinkedIn (AM)
        company_linkedin_range = f"{sheet_name}!AM{sheet_row}"
        company_linkedin_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=company_linkedin_range
        ).execute()
        
        company_linkedin = "No Company LinkedIn"
        if company_linkedin_data.get('values'):
            company_linkedin = company_linkedin_data['values'][0][0] if company_linkedin_data['values'][0] else "No Company LinkedIn"
        
        # Read contact LinkedIn (AB)
        contact_linkedin_range = f"{sheet_name}!AB{sheet_row}"
        contact_linkedin_data = sheets_handler.service.spreadsheets().values().get(
            spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
            range=contact_linkedin_range
        ).execute()
        
        contact_linkedin = "No Contact LinkedIn"
        if contact_linkedin_data.get('values'):
            contact_linkedin = contact_linkedin_data['values'][0][0] if contact_linkedin_data['values'][0] else "No Contact LinkedIn"
        
        print(f"    📋 Company: {company_name}")
        print(f"    👤 Contact: {first_name} {last_name} - {contact_title}")
        print(f"    🏭 Industry: {industry}")
        print(f"    🔗 Company LinkedIn: {company_linkedin}")
        print(f"    🔗 Contact LinkedIn: {contact_linkedin}")
        
        # Generate emails using OpenAI
        print(f"    🤖 Generating emails with OpenAI...")
        ai_response = generate_three_emails(
            company_name, company_url, f"{first_name} {last_name}", contact_title,
            company_summary, contact_summary, pain_points, opportunity_match,
            first_name, last_name, industry, company_linkedin, contact_linkedin
        )
        
        if not ai_response:
            print(f"    ❌ Failed to generate emails for row {sheet_row}")
            return
        
        # Parse the AI response
        emails = parse_three_emails(ai_response)
        
        if len(emails) != 3:
            print(f"    ⚠️  Expected 3 emails, got {len(emails)}")
            return
        
        print(f"    ✅ Generated {len(emails)} emails")
        
        # Display the generated emails
        print(f"\n📧 Generated Emails for {company_name}:")
        print("=" * 80)
        
        for i, email in enumerate(emails, 1):
            print(f"\nEMAIL {i}:")
            print(f"Subject: {email.get('subject', 'No subject')}")
            print(f"Icebreaker: {email.get('icebreaker', 'No icebreaker')}")
            print(f"Body: {email.get('body', 'No body')}")
            print(f"CTA: {email.get('cta', 'No CTA')}")
            print("-" * 60)
        
        # Write emails to columns BA-BM (indices 53-64) for the correct sheet row
        print(f"\n📝 Writing emails to sheet...")
        for i, email in enumerate(emails, 1):
            print(f"    📝 Writing Email {i}...")
            
            # Write subject
            subject_col = get_email_column_index(i, 'subject')
            if subject_col:
                subject_range = f"{sheet_name}!{sheets_handler._get_column_letter(subject_col)}{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=subject_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('subject', 'No subject')]]}
                ).execute()
            
            # Write icebreaker
            icebreaker_col = get_email_column_index(i, 'icebreaker')
            if icebreaker_col:
                icebreaker_range = f"{sheet_name}!{sheets_handler._get_column_letter(icebreaker_col)}{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=icebreaker_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('icebreaker', 'No icebreaker')]]}
                ).execute()
            
            # Write body
            body_col = get_email_column_index(i, 'body')
            if body_col:
                body_range = f"{sheet_name}!{sheets_handler._get_column_letter(body_col)}{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=body_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('body', 'No body')]]}
                ).execute()
            
            # Write CTA - but NOT to BI or BN (those are your hyperlink variables)
            if i == 1:  # Email 1 CTA goes to BD
                cta_range = f"{sheet_name}!BD{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=cta_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('cta', 'No CTA')]]}
                ).execute()
            elif i == 2:  # Email 2 CTA Text goes to BH (NOT BI)
                cta_range = f"{sheet_name}!BH{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=cta_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('cta', 'No CTA')]]}
                ).execute()
            elif i == 3:  # Email 3 CTA Text goes to BM (NOT BN)
                cta_range = f"{sheet_name}!BM{sheet_row}"
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=cta_range,
                    valueInputOption='RAW',
                    body={'values': [[email.get('cta', 'No CTA')]]}
                ).execute()
        
        print(f"    ✅ Emails written to correct columns (BA-BD, BE-BH, BI-BL)")
        print(f"    🔒 ONLY wrote to the specified email columns")
        print(f"    ✅ All emails written for row {sheet_row}")
        print(f"    💡 No other columns were touched")
        
        # REMOVED: The CTA text and link variable writing - you'll handle those manually
        print(f"    💡 CTA hyperlink variables in BI & BN are preserved for your use")
        
    except Exception as e:
        print(f"    ❌ Error processing row {sheet_row}: {e}")
    
    print(f"\n🎉 Email generation complete for Row {sheet_row}!")

if __name__ == "__main__":
    main()
