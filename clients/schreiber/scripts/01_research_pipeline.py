#!/usr/bin/env python3
"""
Research Pipeline using Perplexity Sonar API - Testing rows 2, 3, and 4.
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google_sheets_handler import GoogleSheetsHandler

def main():
    """Main research pipeline using Perplexity Sonar."""
    print("🧪 Research Pipeline using Perplexity Sonar - Row 5")
    print("=" * 70)
    
    load_dotenv()
    perplexity_api_key = os.getenv('PERPLEXITY_API_KEY')
    
    if not perplexity_api_key:
        print("❌ PERPLEXITY_API_KEY not found in .env file")
        return
    
    print(f"✅ Perplexity API Key loaded: {perplexity_api_key[:20]}...")
    
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
    
    print(f"🔍 Processing row 5...")
    
    # Process row 5 (index 4 in 0-indexed)
    idx = 4  # Row 5
    row = df.iloc[idx]
    
    company_name = row.iloc[0]  # Company Name
    website = row.iloc[1] if len(row) > 1 else None  # Website
    job_title = row.iloc[2] if len(row) > 2 else None  # Job Title
    last_name = row.iloc[3] if len(row) > 3 else None  # Last Name
    first_name = row.iloc[4] if len(row) > 4 else None  # First Name
    linkedin_url = row.iloc[5] if len(row) > 5 else None  # LinkedIn URL
    
    # Construct full contact name
    contact_name = f"{first_name} {last_name}" if first_name and last_name else f"{first_name or last_name or 'Unknown'}"
    
    print(f"\n📊 Processing Row {idx + 1}: {company_name}")
    print(f"  Contact: {contact_name} - {job_title}")
    print(f"  Website: {website}")
    print(f"  LinkedIn: {linkedin_url if linkedin_url and linkedin_url != 'nan' else 'Not available'}")
    
    # Initialize research data
    research_data = {
        'company_research': '',
        'contact_research': '',
        'industry_pain_points': '',
        'schreiber_opportunity': '',
        'research_quality': 0
    }
    
    # 1. Company Research using Perplexity Sonar
    print(f"    🏢 Researching company...")
    company_prompt = f"""Research {company_name} ({website}) and provide a concise summary including:
1. Company description and mission
2. Main products and services  
3. Company size and scale
4. Industry focus
5. Key business areas that might use dairy ingredients

Return a clean, actionable summary in 3-4 sentences."""
    
    try:
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {perplexity_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar-pro",
                    "messages": [{"role": "user", "content": company_prompt}],
                    "max_tokens": 400,
                    "temperature": 0.1
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    choice = result['choices'][0]
                    message = choice.get('message', {})
                    
                    if 'content' in message:
                        research_data['company_research'] = message['content'].strip()
                        print(f"      ✅ Company research: {len(research_data['company_research'])} chars")
                    else:
                        research_data['company_research'] = "No research data available"
                        print(f"      ❌ No company research data")
                else:
                    research_data['company_research'] = "API response format error"
            else:
                research_data['company_research'] = f"API error: {response.status_code}"
                
        except Exception as e:
            research_data['company_research'] = f"API call failed: {str(e)}"
        
        # 2. Contact Research using Perplexity Sonar
        print(f"    👤 Researching contact...")
        contact_prompt = f"""Research {contact_name} at {company_name} and provide insights on:
1. Professional background and role
2. Current responsibilities
3. Industry expertise
4. Potential pain points in their role
5. How they might benefit from heat-stable cream cheese solutions

Return a clean, professional summary in 2-3 sentences."""
        
        try:
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {perplexity_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar-pro",
                    "messages": [{"role": "user", "content": contact_prompt}],
                    "max_tokens": 300,
                    "temperature": 0.1
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    choice = result['choices'][0]
                    message = choice.get('message', {})
                    
                    if 'content' in message:
                        research_data['contact_research'] = message['content'].strip()
                        print(f"      ✅ Contact research: {len(research_data['contact_research'])} chars")
                    else:
                        research_data['contact_research'] = "No research data available"
                        print(f"      ❌ No contact research data")
                else:
                    research_data['contact_research'] = "API response format error"
            else:
                research_data['contact_research'] = f"API error: {response.status_code}"
                
        except Exception as e:
            research_data['contact_research'] = f"API call failed: {str(e)}"
        
        # 3. Industry Pain Points using Perplexity Sonar
        print(f"    💡 Researching pain points...")
        pain_points_prompt = f"""Based on {company_name}'s business and industry, identify:
1. Common pain points in food manufacturing
2. Challenges with ingredient stability
3. Supply chain issues
4. Quality control concerns
5. Cost optimization needs

Focus on areas relevant to dairy ingredients and food processing. Return a concise summary."""
        
        try:
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {perplexity_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar-pro",
                    "messages": [{"role": "user", "content": pain_points_prompt}],
                    "max_tokens": 350,
                    "temperature": 0.1
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    choice = result['choices'][0]
                    message = choice.get('message', {})
                    
                    if 'content' in message:
                        research_data['industry_pain_points'] = message['content'].strip()
                        print(f"      ✅ Pain points: {len(research_data['industry_pain_points'])} chars")
                    else:
                        research_data['industry_pain_points'] = "No research data available"
                        print(f"      ❌ No pain points data")
                else:
                    research_data['industry_pain_points'] = "API response format error"
            else:
                research_data['industry_pain_points'] = f"API error: {response.status_code}"
                
        except Exception as e:
            research_data['industry_pain_points'] = f"API call failed: {str(e)}"
        
        # 4. Schreiber Opportunity using Perplexity Sonar
        print(f"    🎯 Assessing opportunity...")
        opportunity_prompt = f"""Assess {company_name}'s potential for heat-stable cream cheese:
1. Likelihood they need it (High/Medium/Low)
2. Potential products they'd use it in
3. Estimated volume needs
4. Key decision factors
5. Best approach strategy

Keep response concise and actionable."""
        
        try:
            response = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {perplexity_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "sonar-pro",
                    "messages": [{"role": "user", "content": opportunity_prompt}],
                    "max_tokens": 300,
                    "temperature": 0.1
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    choice = result['choices'][0]
                    message = choice.get('message', {})
                    
                    if 'content' in message:
                        research_data['schreiber_opportunity'] = message['content'].strip()
                        print(f"      ✅ Opportunity: {len(research_data['schreiber_opportunity'])} chars")
                    else:
                        research_data['schreiber_opportunity'] = "No research data available"
                        print(f"      ❌ No opportunity data")
                else:
                    research_data['schreiber_opportunity'] = "API response format error"
            else:
                research_data['schreiber_opportunity'] = f"API error: {response.status_code}"
                
        except Exception as e:
            research_data['schreiber_opportunity'] = f"API call failed: {str(e)}"
        
        # Calculate research quality score
        research_quality = 0
        if research_data['company_research']: research_quality += 2
        if research_data['contact_research']: research_quality += 2
        if research_data['industry_pain_points']: research_quality += 2
        if research_data['schreiber_opportunity']: research_quality += 2
        if website and website != 'nan': research_quality += 1
        if linkedin_url and linkedin_url != 'nan': research_quality += 1
        
        research_data['research_quality'] = research_quality
        
        print(f"    📊 Research Quality Score: {research_quality}/10")
        
        # Display research results
        print(f"\n📋 Research Results for {company_name}:")
        print(f"  Company Research: {research_data['company_research'][:100]}...")
        print(f"  Contact Research: {research_data['contact_research'][:100]}...")
        print(f"  Pain Points: {research_data['industry_pain_points'][:100]}...")
        print(f"  Opportunity: {research_data['schreiber_opportunity'][:100]}...")
        print(f"  Quality Score: {research_data['research_quality']}/10")
        
        # Write research data to Google Sheets - Columns AV-AZ
        print(f"  📝 Writing research data to sheet (Row {idx + 1}, Columns AV-AZ)...")
        try:
            # Prepare data for writing to columns AV-AZ (indices 48-52)
            research_values = [
                [research_data['company_research'] if research_data['company_research'] else "No data available"],
                [research_data['contact_research'] if research_data['contact_research'] else "No data available"],
                [research_data['industry_pain_points'] if research_data['industry_pain_points'] else "No data available"],
                [research_data['schreiber_opportunity'] if research_data['schreiber_opportunity'] else "No data available"],
                [str(research_data['research_quality'])]  # Convert quality score to string
            ]
            
            # Write to columns AV (48), AW (49), AX (50), AY (51), AZ (52)
            print(f"    🔍 Debug: Writing to columns 48-52 (AV-AZ) - Row {idx + 1}")
            for i, values in enumerate(research_values):
                col_index = 48 + i  # Start from AV (index 48)
                col_letter = sheets_handler._get_column_letter(col_index)
                range_name = f"{sheet_name}!{col_letter}{idx + 1}"  # +1 because sheets are 1-indexed and we want row 2,3,4
                
                print(f"    📝 Column {col_letter} (index {col_index}): {range_name}")
                
                # Ensure the value is properly formatted
                clean_value = values[0] if values[0] else "No data available"
                if isinstance(clean_value, str) and len(clean_value) > 0:
                    clean_value = clean_value.replace('\n', ' ').replace('\r', ' ').strip()
                else:
                    clean_value = "No data available"
                
                print(f"    📝 Writing to column {col_letter} (row {idx + 1}): {clean_value[:50]}...")
                
                sheets_handler.service.spreadsheets().values().update(
                    spreadsheetId=sheets_handler.extract_sheet_id_from_url(sheet_url),
                    range=range_name,
                    valueInputOption='RAW',
                    body={'values': [[clean_value]]}
                ).execute()
            
            print(f"    ✅ Research data written to Row {idx + 1}, Columns AV-AZ")
            
        except Exception as e:
            print(f"    ❌ Failed to write to sheet: {e}")
        
        print("-" * 60)
    
    print(f"\n🎉 Research pipeline completed for Rows 2, 3, and 4!")
    print("🔍 Check your Google Sheet - Rows 2-4, Columns AV-AZ should now contain research data")

if __name__ == "__main__":
    main()
