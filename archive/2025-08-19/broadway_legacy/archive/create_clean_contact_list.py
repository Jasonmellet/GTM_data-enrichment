#!/usr/bin/env python3
"""
Create a clean, deduplicated contact list for Broadway client.
Focuses on business name, URL, contact name, and email for deduplication.
"""

import pandas as pd
import os
from datetime import datetime

def create_clean_contact_list(input_csv, output_csv):
    """Create a clean, deduplicated contact list."""
    
    print("🔄 Loading complete dataset...")
    df = pd.read_csv(input_csv)
    
    print(f"📊 Original dataset: {len(df)} records")
    
    # Create a deduplication key based on core business identity
    df['dedup_key'] = df['legal_name'].fillna('') + '|' + \
                      df['website_domain'].fillna('') + '|' + \
                      df['full_name'].fillna('') + '|' + \
                      df['email'].fillna('')
    
    # Remove exact duplicates based on dedup key
    df_clean = df.drop_duplicates(subset=['dedup_key'], keep='first')
    
    print(f"✅ After deduplication: {len(df_clean)} records")
    print(f"🗑️  Removed {len(df) - len(df_clean)} duplicate records")
    
    # Remove the dedup key column
    df_clean = df_clean.drop('dedup_key', axis=1)
    
    # Sort by organization name and contact name for easy reading
    df_clean = df_clean.sort_values(['legal_name', 'full_name'])
    
    # Save the clean list
    df_clean.to_csv(output_csv, index=False)
    print(f"💾 Clean contact list saved to: {output_csv}")
    
    # Generate summary
    unique_orgs = df_clean['legal_name'].nunique()
    unique_contacts = df_clean['full_name'].nunique()
    contacts_with_emails = df_clean['email'].notna().sum()
    contacts_with_phones = df_clean['phone_e164'].notna().sum()
    
    print(f"\n📋 CLEAN CONTACT LIST SUMMARY:")
    print(f"   Unique Organizations: {unique_orgs}")
    print(f"   Unique Contacts: {unique_contacts}")
    print(f"   Contacts with Emails: {contacts_with_emails}")
    print(f"   Contacts with Phones: {contacts_with_phones}")
    
    # Show first few organizations
    print(f"\n🏢 Sample Organizations:")
    sample_orgs = df_clean['legal_name'].unique()[:10]
    for i, org in enumerate(sample_orgs, 1):
        print(f"   {i}. {org}")
    
    if len(sample_orgs) < unique_orgs:
        print(f"   ... and {unique_orgs - len(sample_orgs)} more")
    
    return df_clean

def main():
    input_file = "clients/Broadway/outputs/complete_enriched_dataset.csv"
    output_file = "clients/Broadway/outputs/clean_contact_list.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    print("🚀 Creating Clean Contact List for Broadway")
    print("=" * 50)
    
    try:
        df_clean = create_clean_contact_list(input_file, output_file)
        
        print(f"\n🎉 Success! Clean contact list created with {len(df_clean)} records")
        print(f"📁 File saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error creating clean contact list: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
