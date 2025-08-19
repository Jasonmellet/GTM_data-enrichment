#!/usr/bin/env python3
"""
Create a CSV with all incomplete organizations and what they're missing.
"""

import pandas as pd
import os

def create_incomplete_list():
    """Create a CSV with all incomplete organizations."""
    
    print("🔍 Creating Incomplete Organizations List for Broadway")
    print("=" * 60)
    
    # Load the summer camp focus list
    input_file = "clients/Broadway/outputs/summer_camp_focus_list.csv"
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    df = pd.read_csv(input_file)
    print(f"📊 Total summer camp organizations: {len(df)}")
    
    # Define what makes an organization "complete"
    complete_mask = (
        (df['email'].notna()) & 
        (df['email'] != '') & 
        (df['email'] != 'nan') &
        (df['categories'].notna()) & 
        (df['categories'] != '') & 
        (df['categories'] != 'nan') &
        (df['fit_score'].notna()) & 
        (df['fit_score'] != '') & 
        (df['fit_score'] != 'nan') &
        (df['phone_e164'].notna()) & 
        (df['phone_e164'] != '') & 
        (df['phone_e164'] != 'nan')
    )
    
    df_complete = df[complete_mask].copy()
    df_incomplete = df[~complete_mask].copy()
    
    print(f"✅ Complete organizations: {len(df_complete)}")
    print(f"❌ Incomplete organizations: {len(df_incomplete)}")
    
    # Add columns to show what's missing
    df_incomplete['missing_email'] = df_incomplete['email'].isna() | (df_incomplete['email'] == '') | (df_incomplete['email'] == 'nan')
    df_incomplete['missing_categories'] = df_incomplete['categories'].isna() | (df_incomplete['categories'] == '') | (df_incomplete['categories'] == 'nan')
    df_incomplete['missing_fit_score'] = df_incomplete['fit_score'].isna() | (df_incomplete['fit_score'] == '') | (df_incomplete['fit_score'] == 'nan')
    df_incomplete['missing_phone'] = df_incomplete['phone_e164'].isna() | (df_incomplete['phone_e164'] == '') | (df_incomplete['phone_e164'] == 'nan')
    
    # Create a summary of what's missing
    df_incomplete['missing_summary'] = df_incomplete.apply(
        lambda row: ', '.join([
            'Email' if row['missing_email'] else '',
            'Categories' if row['missing_categories'] else '',
            'Fit Score' if row['missing_fit_score'] else '',
            'Phone' if row['missing_phone'] else ''
        ]).strip(', '), axis=1
    )
    
    # Select key columns for the incomplete list
    columns_to_show = [
        'legal_name', 'website_domain', 'full_name', 'role_title',
        'email', 'phone_e164', 'city', 'state',
        'categories', 'fit_score', 'outreach_readiness',
        'missing_email', 'missing_categories', 'missing_fit_score', 'missing_phone',
        'missing_summary'
    ]
    
    # Filter to only show columns that exist
    existing_columns = [col for col in columns_to_show if col in df_incomplete.columns]
    df_incomplete_clean = df_incomplete[existing_columns].copy()
    
    # Sort by organization name
    df_incomplete_clean = df_incomplete_clean.sort_values('legal_name')
    
    # Save the incomplete list
    output_file = "clients/Broadway/outputs/incomplete_organizations.csv"
    df_incomplete_clean.to_csv(output_file, index=False)
    
    print(f"💾 Incomplete organizations list saved to: {output_file}")
    
    # Show summary of what's missing
    print(f"\n📊 WHAT'S MISSING:")
    print(f"   Missing Email: {df_incomplete['missing_email'].sum()} organizations")
    print(f"   Missing Categories: {df_incomplete['missing_categories'].sum()} organizations")
    print(f"   Missing Fit Score: {df_incomplete['missing_fit_score'].sum()} organizations")
    print(f"   Missing Phone: {df_incomplete['missing_phone'].sum()} organizations")
    
    # Show first 20 incomplete organizations
    print(f"\n🔍 FIRST 20 INCOMPLETE ORGANIZATIONS:")
    print("-" * 60)
    
    for i, (_, row) in enumerate(df_incomplete_clean.head(20).iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        missing = row['missing_summary'] if 'missing_summary' in row else 'Unknown'
        
        print(f"{i:2d}. {org_name}")
        print(f"    Contact: {contact_name}")
        print(f"    Missing: {missing}")
        print()
    
    if len(df_incomplete_clean) > 20:
        print(f"... and {len(df_incomplete_clean) - 20} more incomplete organizations")
    
    return df_incomplete_clean

def main():
    try:
        incomplete_df = create_incomplete_list()
        
        print(f"\n🎉 Success! Incomplete organizations list created with {len(incomplete_df)} records")
        print(f"📁 File saved to: clients/Broadway/outputs/incomplete_organizations.csv")
        
    except Exception as e:
        print(f"❌ Error creating incomplete list: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
