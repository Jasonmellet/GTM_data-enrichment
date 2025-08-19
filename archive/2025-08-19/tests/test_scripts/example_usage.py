#!/usr/bin/env python3
"""
Example usage of the NAICS enrichment tool.
This shows how to use the tool programmatically in your own code.
"""

from naics_enricher import NAICSEnricher
from google_sheets_handler import GoogleSheetsHandler

def example_individual_businesses():
    """Example: Enrich individual business data."""
    print("🔍 Example: Enriching Individual Businesses")
    print("-" * 50)
    
    enricher = NAICSEnricher()
    
    # Example business data
    businesses = [
        {
            'name': 'Acme Software Solutions',
            'type': 'Technology',
            'description': 'Custom software development company'
        },
        {
            'name': "Joe's Family Restaurant",
            'type': 'Restaurant',
            'description': 'Family-owned Italian restaurant'
        },
        {
            'name': 'Downtown Medical Clinic',
            'type': 'Healthcare',
            'description': 'Primary care medical practice'
        }
    ]
    
    for business in businesses:
        result = enricher.enrich_business_data(
            business_name=business['name'],
            business_type=business['type'],
            business_description=business['description']
        )
        
        source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
        print(f"{business['name']:<30} → {result['naics_code'] or 'N/A'} {source_emoji}")

def example_google_sheets_integration():
    """Example: Read from Google Sheets, enrich, and write back."""
    print("\n📊 Example: Google Sheets Integration")
    print("-" * 50)
    
    try:
        # Initialize handlers
        sheets_handler = GoogleSheetsHandler()
        enricher = NAICSEnricher()
        
        # Example sheet URL (replace with your actual URL)
        sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
        
        print("📖 Reading from Google Sheets...")
        # df = sheets_handler.read_sheet_data(sheet_url)
        # print(f"Found {len(df)} rows of data")
        
        print("ℹ️  (Uncomment the code above and add your actual sheet URL to test)")
        
    except Exception as e:
        print(f"⚠️  Google Sheets example skipped: {e}")
        print("   Make sure you have credentials.json and proper setup")

def example_batch_processing():
    """Example: Process a list of businesses in batch."""
    print("\n🔄 Example: Batch Processing")
    print("-" * 50)
    
    enricher = NAICSEnricher()
    
    # Simulate reading from a CSV or database
    business_list = [
        'TechStart Inc',
        'Green Gardens Landscaping',
        'Main Street Bakery',
        'Digital Marketing Pro',
        'City Auto Repair'
    ]
    
    print("Processing businesses in batch...")
    enriched_results = []
    
    for business_name in business_list:
        result = enricher.enrich_business_data(business_name=business_name)
        enriched_results.append(result)
        
        # Show progress
        source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
        print(f"  {business_name:<25} → {result['naics_code'] or 'N/A'} {source_emoji}")
    
    # Summary
    successful = sum(1 for r in enriched_results if r['naics_code'])
    print(f"\n📊 Batch processing complete: {successful}/{len(enriched_results)} successful")

def example_custom_mapping():
    """Example: Add custom business type mappings."""
    print("\n🗺️  Example: Custom Business Type Mapping")
    print("-" * 50)
    
    enricher = NAICSEnricher()
    
    # You can extend the common mapping in config.py
    # Or create your own mapping logic
    custom_mapping = {
        'startup': '541715',  # Research and Development in the Physical, Engineering, and Life Sciences
        'consulting': '541610',  # Management Consulting Services
        'ecommerce': '454110',  # Electronic Shopping
        'saas': '511210',  # Software Publishers
        'fintech': '522320'  # Financial Transactions Processing, Reserve, and Clearinghouse Activities
    }
    
    # Example usage
    test_businesses = ['TechStartup Inc', 'ConsultPro LLC', 'EcomStore']
    
    for business in test_businesses:
        # You could enhance the enricher to use custom mappings
        result = enricher.enrich_business_data(business_name=business)
        print(f"{business:<20} → {result['naics_code'] or 'N/A'}")

def main():
    """Run all examples."""
    print("🚀 NAICS Enrichment Tool - Usage Examples")
    print("=" * 60)
    
    examples = [
        example_individual_businesses,
        example_google_sheets_integration,
        example_batch_processing,
        example_custom_mapping
    ]
    
    for example in examples:
        try:
            example()
            print()  # Add spacing between examples
        except Exception as e:
            print(f"❌ Example failed: {e}")
            print()
    
    print("🎉 Examples completed!")
    print("\n💡 Tips:")
    print("- Use --dry-run when testing with real Google Sheets")
    print("- Start with a small dataset to test your setup")
    print("- Check the README.md for more advanced usage")

if __name__ == "__main__":
    main()
