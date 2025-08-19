#!/usr/bin/env python3
"""
Test script to ping each AI API and confirm connections.
Prioritizes Gemini Flash as primary due to cost-effectiveness.
"""

import os
from dotenv import load_dotenv

def test_openai():
    """Test OpenAI API connection."""
    try:
        import openai
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key or api_key == 'your_openai_api_key_here':
            return "❌ No valid API key"
        
        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # Using cheaper model
            messages=[{"role": "user", "content": "Hello"}],
            max_tokens=5
        )
        return f"✅ Connected - Model: gpt-4o-mini"
    except Exception as e:
        return f"❌ Error: {str(e)[:100]}"

def test_anthropic():
    """Test Anthropic Claude API connection."""
    try:
        import anthropic
        api_key = os.getenv('ANTHROPIC_API_KEY')
        if not api_key or api_key == 'your_anthropic_api_key_here':
            return "❌ No valid API key"
        
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-3-haiku-20240307",  # Using cheaper model
            max_tokens=5,
            messages=[{"role": "user", "content": "Hello"}]
        )
        return f"✅ Connected - Model: claude-3-haiku-20240307"
    except Exception as e:
        return f"❌ Error: {str(e)[:100]}"

def test_gemini():
    """Test Google Gemini API connection."""
    try:
        import google.generativeai as genai
        api_key = os.getenv('GOOGLE_AI_API_KEY')
        if not api_key or api_key == 'your_google_ai_api_key_here':
            return "❌ No valid API key"
        
        genai.configure(api_key=api_key)
        
        # Test Gemini Flash (cheapest)
        try:
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = model.generate_content("Hello")
            return f"✅ Connected - Model: gemini-1.5-flash (PRIMARY - Cost Effective)"
        except:
            # Fallback to regular Gemini
            model = genai.GenerativeModel('gemini-1.5-pro')
            response = model.generate_content("Hello")
            return f"✅ Connected - Model: gemini-1.5-pro"
            
    except Exception as e:
        return f"❌ Error: {str(e)[:100]}"

def test_perplexity():
    """Test Perplexity API connection."""
    try:
        api_key = os.getenv('PERPLEXITY_API_KEY')
        if not api_key or api_key == 'your_perplexity_api_key_here':
            return "❌ No valid API key"
        
        # Perplexity API test
        import requests
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": "sonar",
            "messages": [{"role": "user", "content": "Hello"}],
            "max_tokens": 5
        }
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers=headers,
            json=data
        )
        if response.status_code == 200:
            return f"✅ Connected - Model: sonar"
        else:
            return f"❌ HTTP {response.status_code}: {response.text[:100]}"
            
    except Exception as e:
        return f"❌ Error: {str(e)[:100]}"

def main():
    """Test all AI APIs and show results."""
    print("🤖 AI API Connection Test")
    print("=" * 50)
    
    load_dotenv()
    
    print("🔑 Testing API Keys...")
    print()
    
    # Test each API
    results = {
        "Google Gemini (Primary)": test_gemini(),
        "OpenAI": test_openai(),
        "Anthropic Claude": test_anthropic(),
        "Perplexity": test_perplexity()
    }
    
    # Display results
    for name, result in results.items():
        print(f"{name}: {result}")
    
    print()
    print("📊 Summary:")
    working_apis = [name for name, result in results.items() if "✅" in result]
    if working_apis:
        print(f"✅ Working APIs: {len(working_apis)}")
        for api in working_apis:
            print(f"   - {api}")
    else:
        print("❌ No working APIs found")
    
    print()
    print("💡 Next Steps:")
    print("1. Fix any API keys that show errors")
    print("2. Gemini Flash will be used as primary (cost-effective)")
    print("3. Other working APIs will be used as fallbacks")

if __name__ == "__main__":
    main()
