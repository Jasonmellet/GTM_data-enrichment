#!/usr/bin/env python3
"""
Create a focused list of summer camp organizations that have been enriched.
"""

import pandas as pd
import os

def create_summer_camp_focus_list(input_csv, output_csv):
    """Create a focused list of summer camp organizations."""
    
    print("🔄 Loading essential contact list...")
    df = pd.read_csv(input_csv)
    
    print(f"📊 Total contacts: {len(df)} records")
    
    # Filter for organizations that are likely summer camps
    # Look for organizations with summer camp categories or summer camp keywords in name
    summer_camp_keywords = [
        'camp', 'summer', 'youth', 'kids', 'children', 'academy', 'school',
        'ranch', 'center', 'program', 'enrichment', 'learning'
    ]
    
    # Filter by categories containing summer camp terms
    df_filtered = df[
        df['categories'].str.contains('camp', case=False, na=False) |
        df['legal_name'].str.contains('|'.join(summer_camp_keywords), case=False, na=False)
    ].copy()
    
    print(f"✅ Summer camp focused: {len(df_filtered)} records")
    
    # Remove any remaining duplicates based on business identity
    df_filtered['dedup_key'] = df_filtered['legal_name'].fillna('') + '|' + \
                               df_filtered['website_domain'].fillna('') + '|' + \
                               df_filtered['full_name'].fillna('') + '|' + \
                               df_filtered['email'].fillna('')
    
    df_filtered = df_filtered.drop_duplicates(subset=['dedup_key'], keep='first')
    df_filtered = df_filtered.drop('dedup_key', axis=1)
    
    print(f"✅ After deduplication: {len(df_filtered)} records")
    
    # Sort by fit score (highest first) and then by organization name
    try:
        df_filtered['fit_score_numeric'] = pd.to_numeric(df_filtered['fit_score'], errors='coerce')
        df_filtered = df_filtered.sort_values(['fit_score_numeric', 'legal_name'], ascending=[False, True])
        df_filtered = df_filtered.drop('fit_score_numeric', axis=1)
    except:
        df_filtered = df_filtered.sort_values('legal_name')
    
    # Save the focused list
    df_filtered.to_csv(output_csv, index=False)
    print(f"💾 Summer camp focus list saved to: {output_csv}")
    
    # Generate summary
    total_contacts = len(df_filtered)
    contacts_with_emails = (df_filtered['email'] != '').sum()
    contacts_with_phones = (df_filtered['phone_e164'] != '').sum()
    
    try:
        fit_scores = pd.to_numeric(df_filtered['fit_score'], errors='coerce')
        outreach_scores = pd.to_numeric(df_filtered['outreach_readiness'], errors='coerce')
        high_fit_contacts = (fit_scores >= 70).sum()
        outreach_ready = (outreach_scores >= 25).sum()
    except:
        high_fit_contacts = 'N/A'
        outreach_ready = 'N/A'
    
    print(f"\n🏕️  SUMMER CAMP FOCUS LIST SUMMARY:")
    print(f"   Total Contacts: {total_contacts}")
    print(f"   With Emails: {contacts_with_emails}")
    print(f"   With Phones: {contacts_with_phones}")
    print(f"   High Fit (≥70): {high_fit_contacts}")
    print(f"   Outreach Ready (≥25): {outreach_ready}")
    
    # Show all organizations in the focused list
    print(f"\n🏕️  SUMMER CAMP ORGANIZATIONS:")
    for i, (_, row) in enumerate(df_filtered.iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        email = row['email'] if row['email'] != '' else 'No email'
        fit_score = row['fit_score'] if row['fit_score'] != '' else 'N/A'
        categories = row['categories'][:60] + '...' if len(str(row['categories'])) > 60 else row['categories']
        
        print(f"   {i:2d}. {org_name}")
        print(f"       Contact: {contact_name} | Email: {email} | Fit: {fit_score}")
        print(f"       Categories: {categories}")
        print()
    
    return df_filtered

def main():
    input_file = "clients/Broadway/outputs/essential_contact_list.csv"
    output_file = "clients/Broadway/outputs/summer_camp_focus_list.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    print("🚀 Creating Summer Camp Focus List for Broadway")
    print("=" * 50)
    
    try:
        df_focused = create_summer_camp_focus_list(input_file, output_file)
        
        print(f"\n🎉 Success! Summer camp focus list created with {len(df_focused)} records")
        print(f"📁 File saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error creating summer camp focus list: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
