#!/usr/bin/env python3
"""
Test script to analyze one xAI response in detail without writing to sheet.
"""

import os
import requests
import json
from dotenv import load_dotenv

def test_single_xai_response():
    """Test one xAI API call and analyze the response structure."""
    print("🧪 Testing Single xAI Response - No Sheet Writing")
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
                "max_tokens": 800,
                "temperature": 0.1
            }
        )
        
        print(f"📊 HTTP Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"\n📋 Full API Response JSON:")
            print(json.dumps(result, indent=2))
            
            print(f"\n🔍 Response Analysis:")
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                print(f"  - Choice keys: {list(choice.keys())}")
                
                if 'message' in choice:
                    message = choice['message']
                    print(f"  - Message keys: {list(message.keys())}")
                    
                    if 'reasoning_content' in message:
                        reasoning = message['reasoning_content']
                        print(f"  - Reasoning content length: {len(reasoning)}")
                        print(f"  - Reasoning content preview: {reasoning[:200]}...")
                        
                        # Look for the actual response
                        if "Structure my response:" in reasoning:
                            parts = reasoning.split("Structure my response:")
                            if len(parts) > 1:
                                final_answer = parts[1].strip()
                                print(f"\n✅ Final Answer Found:")
                                print(f"  Length: {len(final_answer)} characters")
                                print(f"  Content: {final_answer[:300]}...")
                            else:
                                print(f"\n⚠️ No 'Structure my response:' found in reasoning")
                        else:
                            print(f"\n⚠️ No 'Structure my response:' found in reasoning")
                            print(f"  Taking last 500 chars: {reasoning[-500:][:300]}...")
                    
                    if 'content' in message:
                        content = message['content']
                        print(f"  - Content length: {len(content)}")
                        print(f"  - Content: '{content}'")
                        
            else:
                print(f"  - No choices in response")
                
        else:
            print(f"❌ API call failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\n🎯 Test Summary:")
    print("Now we can see exactly what xAI returns and how to extract the clean answer.")

if __name__ == "__main__":
    test_single_xai_response()
