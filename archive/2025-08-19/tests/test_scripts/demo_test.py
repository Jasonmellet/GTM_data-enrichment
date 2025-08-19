#!/usr/bin/env python3
"""
Demo of NAICS enrichment working with sample business data.
This shows what the tool will do with your Google Sheet.
"""

from naics_enricher import NAICSEnricher

def demo_naics_enrichment():
    """Demonstrate NAICS enrichment with sample data."""
    print("🚀 NAICS Enrichment Tool - Live Demo")
    print("=" * 50)
    
    enricher = NAICSEnricher()
    
    # Sample business data (like what you'd have in your Google Sheet)
    sample_businesses = [
        {
            'name': 'Acme Software Solutions',
            'type': 'Technology',
            'description': 'Custom software development company specializing in web apps'
        },
        {
            'name': "Joe's Family Restaurant",
            'type': 'Restaurant',
            'description': 'Family-owned Italian restaurant with full service dining'
        },
        {
            'name': 'Downtown Medical Clinic',
            'type': 'Healthcare',
            'description': 'Primary care medical practice with walk-in appointments'
        },
        {
            'name': 'Green Gardens Landscaping',
            'type': 'Landscaping',
            'description': 'Professional landscaping and garden maintenance services'
        },
        {
            'name': 'Main Street Bakery',
            'type': 'Food Service',
            'description': 'Artisan bakery specializing in breads and pastries'
        }
    ]
    
    print("📊 Processing sample businesses...")
    print()
    
    enriched_results = []
    
    for i, business in enumerate(sample_businesses, 1):
        print(f"🔍 {i}. {business['name']}")
        print(f"   Type: {business['type']}")
        print(f"   Description: {business['description']}")
        
        # Enrich with NAICS code
        result = enricher.enrich_business_data(
            business_name=business['name'],
            business_type=business['type'],
            business_description=business['description']
        )
        
        # Show result
        source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
        print(f"   NAICS Code: {result['naics_code'] or 'N/A'} {source_emoji}")
        print(f"   Source: {result['source']}")
        print()
        
        enriched_results.append(result)
    
    # Summary
    print("📈 Enrichment Summary:")
    print("=" * 30)
    
    total = len(enriched_results)
    successful = sum(1 for r in enriched_results if r['naics_code'])
    
    print(f"Total businesses: {total}")
    print(f"Successfully enriched: {successful}")
    print(f"Success rate: {(successful/total)*100:.1f}%")
    
    # Source breakdown
    sources = {}
    for item in enriched_results:
        if item['source']:
            sources[item['source']] = sources.get(item['source'], 0) + 1
    
    if sources:
        print(f"\nSources used:")
        for source, count in sources.items():
            emoji = "🏛️" if "Census" in source else "🤖" if "AI" in source else "📚"
            print(f"  {emoji} {source}: {count}")
    
    print(f"\n🎉 Demo completed! This is exactly what will happen with your Google Sheet.")
    print(f"Next step: Get your credentials.json and Google Sheet URL ready!")

if __name__ == "__main__":
    demo_naics_enrichment()
