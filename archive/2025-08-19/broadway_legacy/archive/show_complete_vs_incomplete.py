#!/usr/bin/env python3
"""
Show the 6 organizations that are 100% complete vs the 24 that are missing data.
"""

import pandas as pd
import os

def analyze_completeness():
    """Analyze which organizations are complete vs incomplete."""
    
    print("🔍 Analyzing Organization Completeness for Broadway")
    print("=" * 60)
    
    # Load the summer camp focus list
    input_file = "clients/Broadway/outputs/summer_camp_focus_list.csv"
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return
    
    df = pd.read_csv(input_file)
    print(f"📊 Total summer camp organizations: {len(df)}")
    
    # Define what makes an organization "complete"
    # They should have: email, categories, fit_score, and phone - ALL non-empty and non-nan
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
    
    print(f"\n✅ COMPLETE ORGANIZATIONS: {len(df_complete)}")
    print(f"❌ INCOMPLETE ORGANIZATIONS: {len(df_incomplete)}")
    
    # Show the 6 complete organizations
    print(f"\n🏆 100% COMPLETE ORGANIZATIONS ({len(df_complete)}):")
    print("-" * 50)
    
    for i, (_, row) in enumerate(df_complete.iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        email = row['email']
        phone = row['phone_e164']
        fit_score = row['fit_score']
        categories = row['categories'][:60] + '...' if len(str(row['categories'])) > 60 else row['categories']
        
        print(f"{i:2d}. {org_name}")
        print(f"    Contact: {contact_name}")
        print(f"    Email: {email}")
        print(f"    Phone: {phone}")
        print(f"    Fit Score: {fit_score}")
        print(f"    Categories: {categories}")
        print()
    
    # Show the incomplete organizations and what they're missing
    print(f"\n⚠️  INCOMPLETE ORGANIZATIONS ({len(df_incomplete)}):")
    print("-" * 50)
    
    # Group by what's missing
    missing_email = df_incomplete[df_incomplete['email'].isna() | (df_incomplete['email'] == '') | (df_incomplete['email'] == 'nan')]
    missing_categories = df_incomplete[df_incomplete['categories'].isna() | (df_incomplete['categories'] == '') | (df_incomplete['categories'] == 'nan')]
    missing_fit_score = df_incomplete[df_incomplete['fit_score'].isna() | (df_incomplete['fit_score'] == '') | (df_incomplete['fit_score'] == 'nan')]
    missing_phone = df_incomplete[df_incomplete['phone_e164'].isna() | (df_incomplete['phone_e164'] == '') | (df_incomplete['phone_e164'] == 'nan')]
    
    print(f"📧 Missing Email: {len(missing_email)} organizations")
    print(f"🏷️  Missing Categories: {len(missing_categories)} organizations")
    print(f"📊 Missing Fit Score: {len(missing_fit_score)} organizations")
    print(f"📞 Missing Phone: {len(missing_phone)} organizations")
    
    # Show first 10 incomplete organizations with their missing data
    print(f"\n🔍 SAMPLE INCOMPLETE ORGANIZATIONS (first 10):")
    print("-" * 50)
    
    for i, (_, row) in enumerate(df_incomplete.head(10).iterrows(), 1):
        org_name = row['legal_name']
        contact_name = row['full_name'] if row['full_name'] != '' else 'N/A'
        
        # Check what's missing
        missing = []
        if row['email'] == '' or row['email'] == 'nan' or pd.isna(row['email']):
            missing.append("Email")
        if row['categories'] == '' or row['categories'] == 'nan' or pd.isna(row['categories']):
            missing.append("Categories")
        if row['fit_score'] == '' or row['fit_score'] == 'nan' or pd.isna(row['fit_score']):
            missing.append("Fit Score")
        if row['phone_e164'] == '' or row['phone_e164'] == 'nan' or pd.isna(row['phone_e164']):
            missing.append("Phone")
        
        print(f"{i:2d}. {org_name}")
        print(f"    Contact: {contact_name}")
        print(f"    Missing: {', '.join(missing)}")
        print()
    
    if len(df_incomplete) > 10:
        print(f"... and {len(df_incomplete) - 10} more incomplete organizations")
    
    # Summary
    print(f"\n📋 SUMMARY:")
    print(f"   Total Organizations: {len(df)}")
    print(f"   Complete: {len(df_complete)} ({len(df_complete)/len(df)*100:.1f}%)")
    print(f"   Incomplete: {len(df_incomplete)} ({len(df_incomplete)/len(df)*100:.1f}%)")
    
    if len(df_complete) < 30:
        print(f"\n🎯 TARGET: You want 30 complete organizations")
        print(f"   Need to complete: {30 - len(df_complete)} more organizations")
        print(f"   Current completion rate: {len(df_complete)/30*100:.1f}%")
    
    return df_complete, df_incomplete

def main():
    try:
        complete, incomplete = analyze_completeness()
        
        print(f"\n🎉 Analysis complete!")
        print(f"📁 Complete organizations saved to: clients/Broadway/outputs/truly_complete_enriched_organizations.csv")
        
    except Exception as e:
        print(f"❌ Error analyzing completeness: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
