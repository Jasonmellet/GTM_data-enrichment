#!/usr/bin/env python3
"""
Test script to verify content extraction from xAI responses.
"""

import os
import requests
from dotenv import load_dotenv

def test_content_extraction():
    """Test extracting clean content from xAI responses."""
    print("🧪 Testing Content Extraction from xAI")
    print("=" * 50)
    
    load_dotenv()
    xai_api_key = os.getenv('XAI_API_KEY')
    
    if not xai_api_key:
        print("❌ XAI_API_KEY not found in .env file")
        return
    
    print(f"✅ XAI API Key loaded: {xai_api_key[:20]}...")
    
    # Test with one contact
    company_name = "8th Avenue Food and Provisions"
    contact_name = "Laura Powell"
    
    print(f"\n🏢 Company: {company_name}")
    print(f"👤 Contact: {contact_name}")
    
    # Test different research types
    research_tests = [
        {
            "name": "Company Research",
            "prompt": f"Research {company_name} and provide a summary of their business, products, and dairy ingredient usage."
        },
        {
            "name": "Contact Research", 
            "prompt": f"Research {contact_name} at {company_name} and provide insights on their role and potential needs."
        },
        {
            "name": "Pain Points",
            "prompt": f"Identify key pain points for {company_name} in food manufacturing and dairy processing."
        }
    ]
    
    for test in research_tests:
        print(f"\n🔍 {test['name']}")
        print("-" * 30)
        
        try:
            response = requests.post(
                "https://api.x.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {xai_api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "grok-3-mini",
                    "messages": [{"role": "user", "content": test['prompt']}],
                    "max_tokens": 600,
                    "temperature": 0.1
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'choices' in result and len(result['choices']) > 0:
                    choice = result['choices'][0]
                    message = choice.get('message', {})
                    
                    # Extract clean content
                    if 'content' in message and message['content']:
                        clean_content = message['content'].strip()
                        print(f"✅ Clean Content ({len(clean_content)} chars):")
                        print(f"  {clean_content[:200]}...")
                    else:
                        print(f"❌ No content field found")
                        
                    # Show reasoning length for comparison
                    if 'reasoning_content' in message:
                        reasoning_length = len(message['reasoning_content'])
                        print(f"📊 Reasoning content: {reasoning_length} chars")
                        
                else:
                    print(f"❌ No choices in response")
            else:
                print(f"❌ API call failed: HTTP {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
    
    print(f"\n🎯 Test Summary:")
    print("Now we know to use the 'content' field for clean answers!")

if __name__ == "__main__":
    test_content_extraction()
