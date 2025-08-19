#!/usr/bin/env python3
"""
Test Perplexity Sonar API for clean research responses.
"""

import os
import requests
import json
from dotenv import load_dotenv

def test_perplexity_sonar():
    """Test Perplexity Sonar API for research data."""
    print("🧪 Testing Perplexity Sonar API - NO FILE WRITING")
    print("=" * 60)
    
    load_dotenv()
    perplexity_api_key = os.getenv('PERPLEXITY_API_KEY')
    
    if not perplexity_api_key:
        print("❌ PERPLEXITY_API_KEY not found in .env file")
        return
    
    print(f"✅ Perplexity API Key loaded: {perplexity_api_key[:20]}...")
    
    # Test data from your first contact
    company_name = "8th Avenue Food and Provisions"
    company_url = "www.8ave.com"
    first_name = "Laura"
    last_name = "Powell"
    contact_linkedin = "Not available"
    company_linkedin = "Not available"
    
    print(f"\n🏢 Company: {company_name}")
    print(f"🌐 Website: {company_url}")
    print(f"👤 Contact: {first_name} {last_name}")
    print(f"🔗 Contact LinkedIn: {contact_linkedin}")
    print(f"🏢 Company LinkedIn: {company_linkedin}")
    
    # Test 1: Company Research
    print(f"\n🔍 Test 1: Company Research")
    company_prompt = f"""Research {company_name} ({company_url}) and provide a concise summary including:
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
                "max_tokens": 300,
                "temperature": 0.1
            }
        )
        
        print(f"📊 HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n📋 FULL API RESPONSE JSON:")
            print(json.dumps(result, indent=2))
            
            print(f"\n🔍 RESPONSE ANALYSIS:")
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                print(f"  - Choice keys: {list(choice.keys())}")
                
                if 'message' in choice:
                    message = choice['message']
                    print(f"  - Message keys: {list(message.keys())}")
                    
                    if 'content' in message:
                        content = message['content']
                        print(f"  - Content: '{content}'")
                        print(f"  - Content length: {len(content)}")
                        print(f"  - Is this clean research? {'✅ YES' if len(content) > 50 and 'First' not in content else '❌ NO'}")
                    else:
                        print(f"  - No content field found")
                        
        else:
            print(f"❌ API call failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Test 2: Contact Research
    print(f"\n👤 Test 2: Contact Research")
    contact_prompt = f"""Research {first_name} {last_name} at {company_name} and provide insights on:
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
                "max_tokens": 250,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                
                if 'content' in message:
                    content = message['content']
                    print(f"✅ Contact research response: {len(content)} chars")
                    print(f"  Content: {content[:200]}...")
                    print(f"  Is this clean research? {'✅ YES' if len(content) > 30 and 'First' not in content else '❌ NO'}")
                else:
                    print(f"❌ No content field found")
            else:
                print(f"❌ No choices in response")
        else:
            print(f"❌ API call failed: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print(f"\n🎯 Perplexity Sonar test completed - NO FILE WRITING!")

if __name__ == "__main__":
    test_perplexity_sonar()
