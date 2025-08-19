#!/usr/bin/env python3
"""
NAICS Code Enrichment Tool for Google Sheets
Automatically adds NAICS codes to business data using government APIs and AI services.
"""

import argparse
import sys
from typing import List, Dict
import pandas as pd
from tqdm import tqdm

from naics_enricher import NAICSEnricher
from google_sheets_handler import GoogleSheetsHandler
from config import COMMON_NAICS_MAPPING

def main():
    parser = argparse.ArgumentParser(
        description='Enrich Google Sheets business data with NAICS codes'
    )
    parser.add_argument(
        'sheet_url',
        help='Google Sheets URL containing business data'
    )
    parser.add_argument(
        '--sheet-name',
        help='Specific sheet name (defaults to first sheet)'
    )
    parser.add_argument(
        '--business-name-col',
        default='Business Name',
        help='Column name containing business names (default: "Business Name")'
    )
    parser.add_argument(
        '--business-type-col',
        help='Column name containing business types'
    )
    parser.add_argument(
        '--description-col',
        help='Column name containing business descriptions'
    )
    parser.add_argument(
        '--naics-col',
        default='NAICS_Code',
        help='Column name for NAICS codes (default: "NAICS_Code")'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Process data without writing to Google Sheets'
    )
    parser.add_argument(
        '--output-csv',
        help='Save enriched data to CSV file'
    )
    
    args = parser.parse_args()
    
    try:
        print("🚀 Starting NAICS Code Enrichment Process...")
        
        # Initialize handlers
        print("📊 Connecting to Google Sheets...")
        sheets_handler = GoogleSheetsHandler()
        
        print("🔍 Initializing NAICS Enricher...")
        enricher = NAICSEnricher()
        
        # Read data from Google Sheets
        print(f"📖 Reading data from Google Sheets...")
        df = sheets_handler.read_sheet_data(args.sheet_url, args.sheet_name)
        
        # Get the actual sheet name that was used
        if not args.sheet_name:
            # Get sheet name from the handler
            sheet_names = sheets_handler.get_sheet_names(args.sheet_url)
            args.sheet_name = sheet_names[0] if sheet_names else "Sheet1"
            print(f"📋 Using sheet: {args.sheet_name}")
        
        print(f"✅ Found {len(df)} rows of data")
        print(f"📋 Columns: {', '.join(df.columns)}")
        
        # Validate required columns
        if args.business_name_col not in df.columns:
            print(f"❌ Error: Column '{args.business_name_col}' not found in sheet")
            print(f"Available columns: {', '.join(df.columns)}")
            sys.exit(1)
        
        # Process each business
        print("\n🔍 Processing businesses and finding NAICS codes...")
        enriched_data = []
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing businesses"):
            business_name = str(row[args.business_name_col]) if pd.notna(row[args.business_name_col]) else ""
            
            if not business_name or business_name.strip() == "":
                continue
            
            business_type = None
            if args.business_type_col and args.business_type_col in df.columns:
                business_type = str(row[args.business_type_col]) if pd.notna(row[args.business_type_col]) else None
            
            business_description = None
            if args.description_col and args.description_col in df.columns:
                business_description = str(row[args.description_col]) if pd.notna(row[args.description_col]) else None
            
            # Enrich with NAICS code
            result = enricher.enrich_business_data(
                business_name=business_name,
                business_type=business_type,
                business_description=business_description
            )
            
            # Add row index for tracking
            result['row_index'] = idx + 2  # +2 because Google Sheets is 1-indexed and we skip header
            enriched_data.append(result)
            
            # Show progress for first few entries
            if idx < 5:
                source_emoji = "🏛️" if result['source'] == 'Census Bureau API' else "🤖" if result['source'] == 'AI Service' else "📚"
                likely_emoji = "✅" if result['likely_to_buy'] == 'Yes' else "❌"
                print(f"  {business_name[:30]:<30} → {result['naics_code'] or 'N/A'} {source_emoji} | {likely_emoji} {result['likely_to_buy']}")
        
        # Summary statistics
        print(f"\n📊 Enrichment Summary:")
        total_processed = len(enriched_data)
        with_naics = sum(1 for item in enriched_data if item['naics_code'])
        without_naics = total_processed - with_naics
        
        print(f"  Total businesses processed: {total_processed}")
        print(f"  Successfully enriched: {with_naics}")
        print(f"  No NAICS found: {without_naics}")
        
        # Likely to Buy summary
        likely_customers = sum(1 for item in enriched_data if item['likely_to_buy'] == 'Yes')
        unlikely_customers = sum(1 for item in enriched_data if item['likely_to_buy'] == 'No')
        print(f"  🧀 Likely to buy cream cheese: {likely_customers}")
        print(f"  ❌ Unlikely to buy cream cheese: {unlikely_customers}")
        
        # Source breakdown
        sources = {}
        for item in enriched_data:
            if item['source']:
                sources[item['source']] = sources.get(item['source'], 0) + 1
        
        if sources:
            print(f"  Sources:")
            for source, count in sources.items():
                emoji = "🏛️" if "Census" in source else "🤖" if "AI" in source else "📚"
                print(f"    {emoji} {source}: {count}")
        
        # Save to CSV if requested
        if args.output_csv:
            print(f"\n💾 Saving enriched data to {args.output_csv}...")
            enriched_df = pd.DataFrame(enriched_data)
            enriched_df.to_csv(args.output_csv, index=False)
            print(f"✅ Data saved to {args.output_csv}")
        
        # Write back to Google Sheets if not dry run
        if not args.dry_run:
            print(f"\n📝 Writing NAICS codes back to Google Sheets...")
            success = sheets_handler.write_naics_codes(
                sheet_url=args.sheet_url,
                sheet_name=args.sheet_name,
                enriched_data=enriched_data,
                naics_column=args.naics_col
            )
            
            if success:
                print("✅ Successfully updated Google Sheet with NAICS codes!")
            else:
                print("❌ Failed to update Google Sheet")
        else:
            print("\n🔍 Dry run completed - no changes made to Google Sheets")
        
        print("\n🎉 NAICS enrichment process completed!")
        
        # Display AI service usage statistics
        enricher.print_usage_stats()
        
    except KeyboardInterrupt:
        print("\n⏹️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
