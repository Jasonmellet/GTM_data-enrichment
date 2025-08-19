#!/usr/bin/env python3
"""
Test script for xAI API - testing website crawling and LinkedIn data extraction.
"""

import os
import requests
from dotenv import load_dotenv

def test_xai_api():
    """Test xAI API with website crawling and LinkedIn data extraction."""
    print("🧪 Testing xAI API - Website Crawling & LinkedIn Data Extraction")
    print("=" * 70)
    
    load_dotenv()
    xai_api_key = os.getenv('XAI_API_KEY')
    
    if not xai_api_key:
        print("❌ XAI_API_KEY not found in .env file")
        return
    
    print(f"✅ XAI API Key loaded: {xai_api_key[:20]}...")
    
    # Test 0: List available models
    print(f"\n🔍 Test 0: List Available Models")
    try:
        response = requests.get(
            "https://api.x.ai/v1/models",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            }
        )
        
        if response.status_code == 200:
            models = response.json()
            print("✅ Models retrieved successfully!")
            print(f"📊 Available models: {models}")
        else:
            print(f"❌ Failed to get models: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Model listing error: {e}")
    
    # Test 1: Website Crawling (ACH Food Companies)
    print(f"\n🌐 Test 1: Website Crawling")
    print("Target: www.achfood.com")
    
    website_prompt = """Please crawl the website www.achfood.com and extract the following information:
1. Company description and mission
2. Main products and services
3. Company size (employees, locations)
4. Recent news or updates
5. Key executives or leadership
6. Industry focus and specialties

Return the information in a structured format."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",  # Use the available model
                "messages": [
                    {"role": "user", "content": website_prompt}
                ],
                "max_tokens": 1000,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("✅ Website crawl successful!")
            print(f"📊 Full Response:")
            print("-" * 50)
            print(content)
            print("-" * 50)
        else:
            print(f"❌ Website crawl failed: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ Website crawl error: {e}")
    
    # Test 2: LinkedIn Data Extraction
    print(f"\n🔗 Test 2: LinkedIn Data Extraction")
    print("Target: Phyllis Thompson - Food Scientist at ACH Food Companies")
    
    linkedin_prompt = """Please research and extract information about Phyllis Thompson, Food Scientist at ACH Food Companies.
Focus on:
1. Professional background and experience
2. Current role and responsibilities
3. Industry expertise and specializations
4. Recent professional activities
5. Potential pain points or challenges in her role
6. How ACH Food Companies might benefit from heat-stable cream cheese solutions

Use available public information and professional insights."""
    
    try:
        response = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {xai_api_key}",
                "Content-Type": "application/json"
            },
            json={
                "model": "grok-3-mini",  # Use the available model
                "messages": [
                    {"role": "user", "content": linkedin_prompt}
                ],
                "max_tokens": 1000,
                "temperature": 0.1
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            print("✅ LinkedIn research successful!")
            print(f"📊 Full Response:")
            print("-" * 50)
            print(content)
            print("-" * 50)
        else:
            print(f"❌ LinkedIn research failed: HTTP {response.status_code}")
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"❌ LinkedIn research error: {e}")
    
    print(f"\n🎯 Test Summary:")
    print("xAI API tested for:")
    print("  - Website crawling capabilities")
    print("  - LinkedIn data extraction")
    print("  - Professional insights generation")

if __name__ == "__main__":
    test_xai_api()
