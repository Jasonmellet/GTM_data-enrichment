#!/usr/bin/env python3
"""
Create a list of only the organizations that have been TRULY fully enriched with NO missing data.
"""

import pandas as pd
import os

def create_truly_complete_list(input_csv, output_csv):
    """Create a list of only truly fully enriched organizations."""
    
    print("🔄 Loading summer camp focus list...")
    df = pd.read_csv(input_csv)
    
    print(f"📊 Total summer camp contacts: {len(df)} records")
    
    # Filter for organizations that have been TRULY fully enriched
    # They should have: email, categories, fit_score, and phone - ALL non-empty and non-nan
    df_complete = df[
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
    ].copy()
    
    print(f"✅ TRULY fully enriched organizations: {len(df_complete)} records")
    
    # Sort by fit score (highest first) and then by organization name
    try:
        df_complete['fit_score_numeric'] = pd.to_numeric(df_complete['fit_score'], errors='coerce')
        df_complete = df_complete.sort_values(['fit_score_numeric', 'legal_name'], ascending=[False, True])
        df_complete = df_complete.drop('fit_score_numeric', axis=1)
    except:
        df_complete = df_complete.sort_values('legal_name')
    
    # Save the truly complete list
    df_complete.to_csv(output_csv, index=False)
    print(f"💾 Truly complete enriched list saved to: {output_csv}")
    
    # Generate summary
    total_contacts = len(df_complete)
    contacts_with_emails = (df_complete['email'] != '').sum()
    contacts_with_phones = (df_complete['phone_e164'] != '').sum()
    
    try:
        fit_scores = pd.to_numeric(df_complete['fit_score'], errors='coerce')
        outreach_scores = pd.to_numeric(df_complete['outreach_readiness'], errors='coerce')
        high_fit_contacts = (fit_scores >= 70).sum()
        outreach_ready = (outreach_scores >= 25).sum()
        avg_fit_score = fit_scores.mean()
    except:
        high_fit_contacts = 'N/A'
        outreach_ready = 'N/A'
        avg_fit_score = 'N/A'
    
    print(f"\n🎯 TRULY COMPLETE ENRICHED ORGANIZATIONS SUMMARY:")
    print(f"   Total Contacts: {total_contacts}")
    print(f"   With Emails: {contacts_with_emails}")
    print(f"   With Phones: {contacts_with_phones}")
    print(f"   High Fit (≥70): {high_fit_contacts}")
    print(f"   Outreach Ready (≥25): {outreach_ready}")
    print(f"   Average Fit Score: {avg_fit_score}")
    
    # Show all truly complete organizations
    print(f"\n🎯 TRULY FULLY ENRICHED SUMMER CAMP ORGANIZATIONS:")
    for i, (_, row) in enumerate(df_complete.iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        email = row['email'] if row['email'] != '' else 'No email'
        fit_score = row['fit_score'] if row['fit_score'] != '' else 'N/A'
        phone = row['phone_e164'] if row['phone_e164'] != '' else 'No phone'
        categories = row['categories'][:80] + '...' if len(str(row['categories'])) > 80 else row['categories']
        
        print(f"   {i:2d}. {org_name}")
        print(f"       Contact: {contact_name} | Email: {email}")
        print(f"       Phone: {phone} | Fit Score: {fit_score}")
        print(f"       Categories: {categories}")
        print()
    
    return df_complete

def main():
    input_file = "clients/Broadway/outputs/summer_camp_focus_list.csv"
    output_file = "clients/Broadway/outputs/truly_complete_enriched_organizations.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    print("🚀 Creating TRULY Complete Enriched Organizations List for Broadway")
    print("=" * 65)
    
    try:
        df_complete = create_truly_complete_list(input_file, output_file)
        
        print(f"\n🎉 Success! Truly complete enriched list created with {len(df_complete)} records")
        print(f"📁 File saved to: {output_file}")
        
        if len(df_complete) == 0:
            print("\n⚠️  WARNING: No organizations have been TRULY fully enriched!")
            print("   This confirms the enrichment pipeline needs to be run again.")
            print("   Many organizations are missing emails, categories, or fit scores.")
        
    except Exception as e:
        print(f"❌ Error creating truly complete enriched list: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
