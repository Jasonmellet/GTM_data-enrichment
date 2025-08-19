#!/usr/bin/env python3
"""
Test script to verify your NAICS enrichment tool setup.
Run this before using the main tool to check all connections.
"""

import os
import sys
from dotenv import load_dotenv

def test_environment():
    """Test environment variables and API keys."""
    print("🔍 Testing Environment Setup...")
    
    load_dotenv()
    
    # Check required files
    required_files = ['credentials.json']
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        print("   Please download credentials.json from Google Cloud Console")
        return False
    else:
        print("✅ All required files found")
    
    # Check API keys
    api_keys = {
        'CENSUS_API_KEY': 'Government API (Free)',
        'OPENAI_API_KEY': 'OpenAI GPT-4 (Optional)',
        'ANTHROPIC_API_KEY': 'Anthropic Claude (Optional)',
        'GOOGLE_AI_API_KEY': 'Google Gemini (Optional)'
    }
    
    found_keys = []
    for key, description in api_keys.items():
        if os.getenv(key):
            found_keys.append(f"✅ {description}")
        else:
            if key == 'CENSUS_API_KEY':
                print(f"⚠️  {description}: Not set (recommended for free access)")
            else:
                print(f"ℹ️  {description}: Not set (optional)")
    
    if found_keys:
        print(f"✅ Found API keys: {len(found_keys)}")
        for key in found_keys:
            print(f"   {key}")
    
    return True

def test_imports():
    """Test if all required packages can be imported."""
    print("\n📦 Testing Package Imports...")
    
    try:
        import google.auth
        import googleapiclient
        print("✅ Google API packages")
    except ImportError as e:
        print(f"❌ Google API packages: {e}")
        return False
    
    try:
        import pandas
        print("✅ Pandas")
    except ImportError as e:
        print(f"❌ Pandas: {e}")
        return False
    
    try:
        import requests
        print("✅ Requests")
    except ImportError as e:
        print(f"❌ Requests: {e}")
        return False
    
    # Test AI packages (optional)
    ai_packages = {
        'openai': 'OpenAI',
        'anthropic': 'Anthropic Claude',
        'google.generativeai': 'Google Gemini'
    }
    
    for package, name in ai_packages.items():
        try:
            __import__(package)
            print(f"✅ {name}")
        except ImportError:
            print(f"ℹ️  {name}: Not installed (optional)")
    
    return True

def test_google_auth():
    """Test Google authentication setup."""
    print("\n🔐 Testing Google Authentication...")
    
    try:
        from google_sheets_handler import GoogleSheetsHandler
        print("✅ Google Sheets handler imported successfully")
        
        # Don't actually authenticate here, just test the import
        print("ℹ️  Google authentication will happen on first run")
        return True
        
    except Exception as e:
        print(f"❌ Google authentication test failed: {e}")
        return False

def test_naics_enricher():
    """Test NAICS enricher setup."""
    print("\n🔍 Testing NAICS Enricher...")
    
    try:
        from naics_enricher import NAICSEnricher
        print("✅ NAICS enricher imported successfully")
        
        # Test basic functionality
        enricher = NAICSEnricher()
        print("✅ NAICS enricher initialized")
        
        return True
        
    except Exception as e:
        print(f"❌ NAICS enricher test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 NAICS Enrichment Tool - Setup Test")
    print("=" * 50)
    
    tests = [
        test_environment,
        test_imports,
        test_google_auth,
        test_naics_enricher
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your setup is ready.")
        print("\n🚀 Next steps:")
        print("1. Make sure you have a Google Sheet with business data")
        print("2. Run: python main.py 'YOUR_SHEET_URL' --dry-run")
        print("3. Check the README.md for detailed usage instructions")
    else:
        print("⚠️  Some tests failed. Please fix the issues above.")
        print("\n📖 Check the setup_guide.md for help")
        
        if passed < 2:
            print("\n🔧 Common fixes:")
            print("- Run: pip install -r requirements.txt")
            print("- Download credentials.json from Google Cloud Console")
            print("- Create .env file with your API keys")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
