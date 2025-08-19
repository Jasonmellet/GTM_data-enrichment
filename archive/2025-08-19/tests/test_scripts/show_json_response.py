#!/usr/bin/env python3
"""
Show ONLY the xAI API JSON response - NO FILE WRITING.
"""

import os
import requests
import json
from dotenv import load_dotenv

def show_json_response():
    """Show the raw xAI API response - NO WRITING TO SHEET."""
    print("🔍 SHOWING XAI API JSON RESPONSE - NO FILE WRITING")
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
    
    print(f"\n🏢 Company: {company_name}")
    print(f"🌐 Website: {company_url}")
    
    # Simple test prompt
    test_prompt = f"Tell me about {company_name} in 2 sentences."
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",
                "messages": [{"role": "user", "content": test_prompt}],
                "max_tokens": 100,
                "temperature": 0.1
            }
        )
        
        print(f"\n📊 HTTP Status: {response.status_code}")
        print(f"📊 Response Headers: {dict(response.headers)}")
        
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
                    
                    if 'reasoning_content' in message:
                        reasoning = message['reasoning_content']
                        print(f"  - Reasoning length: {len(reasoning)}")
                        print(f"  - Reasoning preview: {reasoning[:200]}...")
                        
        else:
            print(f"❌ API call failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print(f"\n🎯 This is the raw API response - NO FILE WRITING!")

if __name__ == "__main__":
    show_json_response()
