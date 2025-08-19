#!/usr/bin/env python3
"""
Pick the 10 worst-off organizations from the incomplete list.
These are the organizations missing the most data.
"""

import pandas as pd
import os

def pick_worst_10():
    """Pick the 10 organizations with the most missing data."""
    
    print("🔍 Analyzing Incomplete Organizations to Pick the Worst 10")
    print("=" * 60)
    
    # Load the incomplete organizations list
    input_file = "clients/Broadway/outputs/incomplete_organizations.csv"
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    df = pd.read_csv(input_file)
    print(f"📊 Total incomplete organizations: {len(df)}")
    
    # Calculate a "missing data score" for each organization
    # Higher score = more missing data = worse off
    df['missing_score'] = (
        df['missing_email'].astype(int) + 
        df['missing_categories'].astype(int) + 
        df['missing_fit_score'].astype(int) + 
        df['missing_phone'].astype(int)
    )
    
    # Sort by missing score (highest first = worst off)
    df_sorted = df.sort_values('missing_score', ascending=False)
    
    # Pick the top 10 worst-off organizations
    worst_10 = df_sorted.head(10).copy()
    
    print(f"\n🏆 TOP 10 WORST-OFF ORGANIZATIONS:")
    print("-" * 60)
    
    for i, (_, row) in enumerate(worst_10.iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        website = row['website_domain'] if row['website_domain'] != '' else 'N/A'
        missing_score = row['missing_score']
        
        # Show what's missing
        missing_items = []
        if row['missing_email']:
            missing_items.append("Email")
        if row['missing_categories']:
            missing_items.append("Categories")
        if row['missing_fit_score']:
            missing_items.append("Fit Score")
        if row['missing_phone']:
            missing_items.append("Phone")
        
        print(f"{i:2d}. {org_name}")
        print(f"    Contact: {contact_name}")
        print(f"    Website: {website}")
        print(f"    Missing Score: {missing_score}/4")
        print(f"    Missing: {', '.join(missing_items)}")
        print()
    
    # Save the worst 10 to a separate CSV
    output_file = "clients/Broadway/outputs/worst_10_organizations.csv"
    worst_10.to_csv(output_file, index=False)
    
    print(f"💾 Worst 10 organizations saved to: {output_file}")
    
    # Show summary
    print(f"📊 SUMMARY:")
    print(f"   Total Missing Data Points: {worst_10['missing_score'].sum()}")
    print(f"   Average Missing per Organization: {worst_10['missing_score'].mean():.1f}/4")
    print(f"   Organizations with 4/4 missing: {(worst_10['missing_score'] == 4).sum()}")
    print(f"   Organizations with 3/4 missing: {(worst_10['missing_score'] == 3).sum()}")
    
    return worst_10

def main():
    try:
        worst_10 = pick_worst_10()
        
        print(f"\n🎯 NEXT STEPS:")
        print(f"   1. Review the worst 10 organizations above")
        print(f"   2. Run them through the full enrichment pipeline")
        print(f"   3. Verify they move from incomplete to complete")
        print(f"   4. If successful, scale up to more organizations")
        
        print(f"\n🎉 Success! Worst 10 organizations identified and saved")
        
    except Exception as e:
        print(f"❌ Error picking worst 10: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
