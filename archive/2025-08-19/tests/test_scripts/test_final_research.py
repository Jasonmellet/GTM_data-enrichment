#!/usr/bin/env python3
"""
Final test script showing what research data would be written to each column.
"""

import os
import requests
from dotenv import load_dotenv

def test_final_research():
    """Test the complete research pipeline and show what would be written to each column."""
    print("🧪 Final Research Test - No Sheet Writing")
    print("=" * 60)
    
    load_dotenv()
    xai_api_key = os.getenv('XAI_API_KEY')
    
    if not xai_api_key:
        print("❌ XAI_API_KEY not found in .env file")
        return
    
    print(f"✅ XAI API Key loaded: {xai_api_key[:20]}...")
    
    # Test with one contact
    company_name = "8th Avenue Food and Provisions"
    company_url = "www.8ave.com"
    contact_name = "Laura Powell"
    job_title = "Food Technologist"
    
    print(f"\n🏢 Company: {company_name}")
    print(f"🌐 Website: {company_url}")
    print(f"👤 Contact: {contact_name} - {job_title}")
    
    # Research data storage
    research_data = {
        'company_research': '',
        'contact_research': '',
        'industry_pain_points': '',
        'schreiber_opportunity': '',
        'research_quality': 0
    }
    
    # 1. Company Research
    print(f"\n🔍 1. Company Research")
    company_prompt = f"""Research {company_name} ({company_url}) and provide a comprehensive summary including:
1. Company description and mission
2. Main products and services
3. Company size and scale
4. Industry focus
5. Key business areas that might use dairy ingredients

Return a concise but informative summary."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",
                "messages": [{"role": "user", "content": company_prompt}],
                "max_tokens": 600,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                
                # Use content if available, otherwise extract from reasoning
                if 'content' in message and message['content']:
                    research_data['company_research'] = message['content'].strip()
                    print(f"✅ Using content field: {len(research_data['company_research'])} chars")
                elif 'reasoning_content' in message:
                    reasoning = message['reasoning_content']
                    # Take the last 500 chars as the final answer
                    research_data['company_research'] = reasoning[-500:].strip()
                    print(f"✅ Using reasoning field (last 500 chars): {len(research_data['company_research'])} chars")
                else:
                    research_data['company_research'] = "No research data available"
                    print(f"❌ No research data available")
        else:
            research_data['company_research'] = f"API error: {response.status_code}"
            
    except Exception as e:
        research_data['company_research'] = f"API call failed: {str(e)}"
    
    # 2. Contact Research
    print(f"\n👤 2. Contact Research")
    contact_prompt = f"""Research {contact_name} at {company_name} and provide insights on:
1. Professional background and role
2. Current responsibilities
3. Industry expertise
4. Potential pain points in their role
5. How they might benefit from heat-stable cream cheese solutions

Return a professional summary."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",
                "messages": [{"role": "user", "content": contact_prompt}],
                "max_tokens": 500,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                
                if 'content' in message and message['content']:
                    research_data['contact_research'] = message['content'].strip()
                    print(f"✅ Using content field: {len(research_data['contact_research'])} chars")
                elif 'reasoning_content' in message:
                    reasoning = message['reasoning_content']
                    research_data['contact_research'] = reasoning[-400:].strip()
                    print(f"✅ Using reasoning field (last 400 chars): {len(research_data['contact_research'])} chars")
                else:
                    research_data['contact_research'] = "No research data available"
                    print(f"❌ No research data available")
        else:
            research_data['contact_research'] = f"API error: {response.status_code}"
            
    except Exception as e:
        research_data['contact_research'] = f"API call failed: {str(e)}"
    
    # 3. Industry Pain Points
    print(f"\n💡 3. Industry Pain Points")
    pain_points_prompt = f"""Based on {company_name}'s business and industry, identify:
1. Common pain points in food manufacturing
2. Challenges with ingredient stability
3. Supply chain issues
4. Quality control concerns
5. Cost optimization needs

Focus on areas relevant to dairy ingredients and food processing."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",
                "messages": [{"role": "user", "content": pain_points_prompt}],
                "max_tokens": 400,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                
                if 'content' in message and message['content']:
                    research_data['industry_pain_points'] = message['content'].strip()
                    print(f"✅ Using content field: {len(research_data['industry_pain_points'])} chars")
                elif 'reasoning_content' in message:
                    reasoning = message['reasoning_content']
                    research_data['industry_pain_points'] = reasoning[-350:].strip()
                    print(f"✅ Using reasoning field (last 350 chars): {len(research_data['industry_pain_points'])} chars")
                else:
                    research_data['industry_pain_points'] = "No research data available"
                    print(f"❌ No research data available")
        else:
            research_data['industry_pain_points'] = f"API error: {response.status_code}"
            
    except Exception as e:
        research_data['industry_pain_points'] = f"API call failed: {str(e)}"
    
    # 4. Schreiber Opportunity
    print(f"\n🎯 4. Schreiber Opportunity Match")
    opportunity_prompt = f"""Assess {company_name}'s potential for heat-stable cream cheese:
1. Likelihood they need it (High/Medium/Low)
2. Potential products they'd use it in
3. Estimated volume needs
4. Key decision factors
5. Best approach strategy

Keep response concise and actionable."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",
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
                
                if 'content' in message and message['content']:
                    research_data['schreiber_opportunity'] = message['content'].strip()
                    print(f"✅ Using content field: {len(research_data['schreiber_opportunity'])} chars")
                elif 'reasoning_content' in message:
                    reasoning = message['reasoning_content']
                    research_data['schreiber_opportunity'] = reasoning[-250:].strip()
                    print(f"✅ Using reasoning field (last 250 chars): {len(research_data['schreiber_opportunity'])} chars")
                else:
                    research_data['schreiber_opportunity'] = "No research data available"
                    print(f"❌ No research data available")
        else:
            research_data['schreiber_opportunity'] = f"API error: {response.status_code}"
            
    except Exception as e:
        research_data['schreiber_opportunity'] = f"API call failed: {str(e)}"
    
    # 5. Research Quality Score
    research_quality = 0
    if research_data['company_research']: research_quality += 2
    if research_data['contact_research']: research_quality += 2
    if research_data['industry_pain_points']: research_quality += 2
    if research_data['schreiber_opportunity']: research_quality += 2
    if company_url and company_url != 'nan': research_quality += 1
    research_data['research_quality'] = research_quality
    
    print(f"\n📊 5. Research Quality Score: {research_quality}/10")
    
    # Show what would be written to each column
    print(f"\n📋 RESEARCH DATA SUMMARY:")
    print("=" * 60)
    print(f"Column AV (Company Research): {len(research_data['company_research'])} chars")
    print(f"  Preview: {research_data['company_research'][:100]}...")
    print(f"\nColumn AW (Contact Research): {len(research_data['contact_research'])} chars")
    print(f"  Preview: {research_data['contact_research'][:100]}...")
    print(f"\nColumn AX (Pain Points): {len(research_data['industry_pain_points'])} chars")
    print(f"  Preview: {research_data['industry_pain_points'][:100]}...")
    print(f"\nColumn AY (Opportunity): {len(research_data['schreiber_opportunity'])} chars")
    print(f"  Preview: {research_data['schreiber_opportunity'][:100]}...")
    print(f"\nColumn AZ (Quality Score): {research_data['research_quality']}")
    
    print(f"\n🎯 Ready to write to columns AV-AZ (not AT-AU)!")

if __name__ == "__main__":
    test_final_research()
