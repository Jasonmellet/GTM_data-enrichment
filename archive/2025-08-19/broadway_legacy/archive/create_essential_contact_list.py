#!/usr/bin/env python3
"""
Create an essential contact list for Broadway client with only the most important columns.
"""

import pandas as pd
import os

def create_essential_contact_list(input_csv, output_csv):
    """Create an essential contact list with key columns only."""
    
    print("🔄 Loading clean contact list...")
    df = pd.read_csv(input_csv)
    
    print(f"📊 Clean dataset: {len(df)} records")
    
    # Select only the essential columns for outreach
    essential_columns = [
        'legal_name',           # Business name
        'website_domain',        # Business URL
        'full_name',            # Contact name
        'role_title',           # Job title
        'email',                # Email address
        'phone_e164',           # Phone number
        'city',                 # City
        'state',                # State
        'categories',           # Business categories
        'fit_score',            # Fit score
        'outreach_readiness'    # Outreach readiness score
    ]
    
    # Create essential list with only these columns
    df_essential = df[essential_columns].copy()
    
    # Clean up the data
    df_essential = df_essential.fillna('')
    
    # Sort by fit score (highest first) and then by organization name
    try:
        df_essential['fit_score_numeric'] = pd.to_numeric(df_essential['fit_score'], errors='coerce')
        df_essential = df_essential.sort_values(['fit_score_numeric', 'legal_name'], ascending=[False, True])
        df_essential = df_essential.drop('fit_score_numeric', axis=1)
    except:
        # If sorting fails, just sort by organization name
        df_essential = df_essential.sort_values('legal_name')
    
    # Save the essential list
    df_essential.to_csv(output_csv, index=False)
    print(f"💾 Essential contact list saved to: {output_csv}")
    
    # Generate summary
    total_contacts = len(df_essential)
    contacts_with_emails = (df_essential['email'] != '').sum()
    contacts_with_phones = (df_essential['phone_e164'] != '').sum()
    
    # Handle numeric columns safely
    try:
        fit_scores = pd.to_numeric(df_essential['fit_score'], errors='coerce')
        outreach_scores = pd.to_numeric(df_essential['outreach_readiness'], errors='coerce')
        high_fit_contacts = (fit_scores >= 70).sum()
        outreach_ready = (outreach_scores >= 25).sum()
    except:
        high_fit_contacts = 'N/A'
        outreach_ready = 'N/A'
    
    print(f"\n📋 ESSENTIAL CONTACT LIST SUMMARY:")
    print(f"   Total Contacts: {total_contacts}")
    print(f"   With Emails: {contacts_with_emails}")
    print(f"   With Phones: {contacts_with_phones}")
    print(f"   High Fit (≥70): {high_fit_contacts}")
    print(f"   Outreach Ready (≥25): {outreach_ready}")
    
    # Show top 10 by fit score
    print(f"\n🏆 TOP 10 CONTACTS BY FIT SCORE:")
    top_10 = df_essential.head(10)
    for i, (_, row) in enumerate(top_10.iterrows(), 1):
        fit_score = row['fit_score'] if row['fit_score'] != '' else 'N/A'
        org_name = row['legal_name'][:40] + '...' if len(row['legal_name']) > 40 else row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        email = row['email'] if row['email'] != '' else 'No email'
        print(f"   {i}. {org_name}")
        print(f"      Contact: {contact_name} | Email: {email} | Fit: {fit_score}")
        print()
    
    return df_essential

def main():
    input_file = "clients/Broadway/outputs/clean_contact_list.csv"
    output_file = "clients/Broadway/outputs/essential_contact_list.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    print("🚀 Creating Essential Contact List for Broadway")
    print("=" * 50)
    
    try:
        df_essential = create_essential_contact_list(input_file, output_file)
        
        print(f"\n🎉 Success! Essential contact list created with {len(df_essential)} records")
        print(f"📁 File saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error creating essential contact list: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
