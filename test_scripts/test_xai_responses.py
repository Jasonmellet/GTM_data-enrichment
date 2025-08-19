#!/usr/bin/env python3
"""
Test script to see xAI API responses before writing to sheet.
"""

import os
import requests
from dotenv import load_dotenv

def test_xai_responses():
    """Test xAI API responses for the research pipeline."""
    print("🧪 Testing xAI API Responses for Research Pipeline")
    print("=" * 60)
    
    load_dotenv()
    xai_api_key = os.getenv('XAI_API_KEY')
    
    if not xai_api_key:
        print("❌ XAI_API_KEY not found in .env file")
        return
    
    print(f"✅ XAI API Key loaded: {xai_api_key[:20]}...")
    
    # Test data from your first contact
    company_name = "ACH Food Companies"
    company_url = "www.achfood.com"
    contact_name = "Thompson Phyllis"
    contact_linkedin = "http://www.linkedin.com/in/phyllis-thompson-629066aa"
    
    print(f"\n🏢 Company: {company_name}")
    print(f"🌐 Website: {company_url}")
    print(f"👤 Contact: {contact_name}")
    print(f"🔗 LinkedIn: {contact_linkedin}")
    
    # Test 1: Company Research
    print(f"\n🔍 Test 1: Company Research")
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
                "max_tokens": 500,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("✅ Company research successful!")
            print(f"📊 Response length: {len(content)} characters")
            print(f"📝 Response preview: {content[:200]}...")
            print(f"📋 Full response:")
            print("-" * 50)
            print(content)
            print("-" * 50)
        else:
            print(f"❌ Company research failed: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Company research error: {e}")
    
    # Test 2: Contact Research
    print(f"\n👤 Test 2: Contact Research")
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
                "max_tokens": 400,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("✅ Contact research successful!")
            print(f"📊 Response length: {len(content)} characters")
            print(f"📝 Response preview: {content[:200]}...")
            print(f"📋 Full response:")
            print("-" * 50)
            print(content)
            print("-" * 50)
        else:
            print(f"❌ Contact research failed: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Contact research error: {e}")
    
    # Test 3: Industry Pain Points
    print(f"\n💡 Test 3: Industry Pain Points")
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
                "max_tokens": 300,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("✅ Pain points analysis successful!")
            print(f"📊 Response length: {len(content)} characters")
            print(f"📝 Response preview: {content[:200]}...")
            print(f"📋 Full response:")
            print("-" * 50)
            print(content)
            print("-" * 50)
        else:
            print(f"❌ Pain points analysis failed: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Pain points analysis error: {e}")
    
    print(f"\n🎯 Test Summary Complete!")
    print("Now we can see exactly what xAI is returning before writing to the sheet.")

if __name__ == "__main__":
    test_xai_responses()
