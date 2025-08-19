#!/usr/bin/env python3
"""
Test the AI fallback functionality.
"""

from naics_enricher import NAICSEnricher

def test_ai_fallback():
    """Test AI services for NAICS classification."""
    print("🤖 Testing AI Fallback for NAICS Classification")
    print("=" * 50)
    
    enricher = NAICSEnricher()
    
    # Test businesses that might not be in Census data
    test_businesses = [
        "TechStartup Inc",
        "Green Gardens Landscaping", 
        "Main Street Bakery",
        "Digital Marketing Pro",
        "City Auto Repair"
    ]
    
    print("📊 Testing AI-powered classification:")
    for business in test_businesses:
        try:
            result = enricher.enrich_business_data(business_name=business)
            
            source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
            print(f"  {business:<25} → {result['naics_code'] or 'N/A'} {source_emoji}")
            
            if result['source']:
                print(f"      Source: {result['source']}")
            
        except Exception as e:
            print(f"  {business:<25} → ❌ Error: {e}")
    
    print("\n✅ AI fallback test completed!")

if __name__ == "__main__":
    test_ai_fallback()
