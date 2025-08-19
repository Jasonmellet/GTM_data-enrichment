#!/usr/bin/env python3
"""
Debug script to see the exact xAI API response structure.
"""

import os
import requests
import json
from dotenv import load_dotenv

def debug_xai_response():
    """Debug the exact xAI API response structure."""
    print("🔍 Debugging xAI API Response Structure")
    print("=" * 50)
    
    load_dotenv()
    xai_api_key = os.getenv('XAI_API_KEY')
    
    if not xai_api_key:
        print("❌ XAI_API_KEY not found in .env file")
        return
    
    print(f"✅ XAI API Key loaded: {xai_api_key[:20]}...")
    
    # Simple test prompt
    test_prompt = "Hello, can you tell me about food manufacturing companies?"
    
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
        
        print(f"📊 HTTP Status: {response.status_code}")
        print(f"📊 Response Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n📋 Full API Response JSON:")
            print(json.dumps(result, indent=2))
            
            print(f"\n🔍 Response Analysis:")
            print(f"  - Response type: {type(result)}")
            print(f"  - Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
            
            if 'choices' in result:
                choices = result['choices']
                print(f"  - Choices count: {len(choices)}")
                
                for i, choice in enumerate(choices):
                    print(f"  - Choice {i} type: {type(choice)}")
                    print(f"  - Choice {i} keys: {list(choice.keys()) if isinstance(choice, dict) else 'Not a dict'}")
                    
                    if 'message' in choice:
                        message = choice['message']
                        print(f"  - Message type: {type(message)}")
                        print(f"  - Message keys: {list(message.keys()) if isinstance(message, dict) else 'Not a dict'}")
                        
                        if 'content' in message:
                            content = message['content']
                            print(f"  - Content type: {type(content)}")
                            print(f"  - Content length: {len(content) if content else 0}")
                            print(f"  - Content value: '{content}'")
                        else:
                            print(f"  - No 'content' key in message")
                    else:
                        print(f"  - No 'message' key in choice")
            else:
                print(f"  - No 'choices' key in response")
                
        else:
            print(f"❌ API call failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_xai_response()
