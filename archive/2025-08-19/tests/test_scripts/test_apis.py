#!/usr/bin/env python3
"""
Quick test to verify your API keys are working.
"""

from naics_enricher import NAICSEnricher
from config import CENSUS_API_KEY, ANTHROPIC_API_KEY

def test_apis():
    """Test the APIs with your keys."""
    print("🧪 Testing Your API Keys")
    print("=" * 40)
    
    # Check if keys are loaded
    print(f"🔑 Census API Key: {'✅ Loaded' if CENSUS_API_KEY else '❌ Missing'}")
    print(f"🔑 Anthropic API Key: {'✅ Loaded' if ANTHROPIC_API_KEY else '❌ Missing'}")
    
    if not CENSUS_API_KEY or not ANTHROPIC_API_KEY:
        print("\n❌ Some API keys are missing. Check your .env file.")
        return
    
    # Test NAICS enricher
    print("\n🔍 Testing NAICS Enricher...")
    enricher = NAICSEnricher()
    
    # Test with a few sample businesses
    test_businesses = [
        "Acme Software Solutions",
        "Joe's Family Restaurant", 
        "Downtown Medical Clinic"
    ]
    
    print("\n📊 Testing business classification:")
    for business in test_businesses:
        try:
            result = enricher.enrich_business_data(business_name=business)
            
            source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
            print(f"  {business:<30} → {result['naics_code'] or 'N/A'} {source_emoji}")
            
        except Exception as e:
            print(f"  {business:<30} → ❌ Error: {e}")
    
    print("\n✅ API test completed!")

if __name__ == "__main__":
    test_apis()
