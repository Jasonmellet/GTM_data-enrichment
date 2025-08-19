#!/usr/bin/env python3
"""
Data cleanup module for Broadway summer camps project.
Handles CSV cleaning, column mapping, and duplicate detection.
"""

import pandas as pd
import os
import sys
from typing import Dict, List, Tuple, Optional
import re
from urllib.parse import urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleanup:
    """Clean and validate CSV data for Broadway summer camps."""
    
    def __init__(self):
        # Define our standard column mappings
        self.column_mappings = {
            # Core business data
            'company_name': ['company_name', 'company name', 'business name', 'camp name', 'name', 'organization'],
            'website_url': ['website_url', 'website', 'url', 'website url', 'domain', 'web'],
            'company_phone': ['company_phone', 'phone', 'business phone', 'company phone', 'main phone'],
            'full_address': ['full_address', 'full address', 'address', 'street address', 'location'],
            'city': ['city'],
            'state': ['state', 'state/province', 'province', 'region'],
            'zip_code': ['zip_code', 'zip', 'postal code', 'postal', 'zipcode'],
            'country': ['country', 'country/region', 'nation'],
            
            # Contact data
            'contact_name': ['contact_name', 'contact name', 'name', 'full name', 'first name', 'last name'],
            'contact_email': ['contact_email', 'email', 'contact email', 'primary email', 'email address'],
            'contact_title': ['contact_title', 'title', 'job title', 'role', 'position', 'designation'],
            'contact_phone': ['contact_phone', 'contact phone', 'mobile', 'direct phone', 'cell'],
            
            # Bonus data (keep if available)
            'lat': ['lat', 'latitude'],
            'lon': ['lon', 'longitude', 'lng'],
            'rating': ['rating', 'score'],
            'review_count': ['review_count', 'reviews', 'review count'],
            'description': ['description', 'desc', 'about'],
            'camp_type': ['camp_type', 'camp type', 'type'],
            'place_id': ['place_id', 'place id'],
            'age_range': ['age_range', 'age range', 'ages'],
            'session_length': ['session_length', 'session length', 'duration'],
            'specialties': ['specialties', 'specialty', 'focus']
        }
        
        # Columns to always remove (old enrichment artifacts)
        self.columns_to_remove = [
            'gemini_enriched', 'enrichment_confidence', 'enrichment_quality_score',
            'enrichment_features', 'enrichment_key_strengths', 'enrichment_updated_at',
            'data_source', 'search_term', 'is_camp', 'camp_classification',
            'created_at', 'updated_at', 'categories'
        ]
    
    def detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """Detect and map columns from the CSV to our standard names."""
        detected_mappings = {}
        unmapped_columns = []
        
        logger.info("Detecting column mappings...")
        
        for col in df.columns:
            col_lower = col.lower().strip()
            mapped = False
            
            # Check if this column maps to one of our standards
            for standard_name, possible_names in self.column_mappings.items():
                if col_lower in possible_names or col in possible_names:
                    detected_mappings[col] = standard_name
                    mapped = True
                    logger.info(f"Mapped '{col}' → '{standard_name}'")
                    break
            
            if not mapped:
                unmapped_columns.append(col)
                logger.warning(f"Unmapped column: '{col}'")
        
        return detected_mappings, unmapped_columns
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize the data."""
        logger.info("Cleaning data...")
        
        # Create a copy to work with
        df_clean = df.copy()
        
        # Remove unwanted columns
        for col in self.columns_to_remove:
            if col in df_clean.columns:
                df_clean = df_clean.drop(columns=[col])
                logger.info(f"Removed column: {col}")
        
        # Clean company names
        if 'company_name' in df_clean.columns:
            df_clean['company_name'] = df_clean['company_name'].astype(str).str.strip()
            df_clean['company_name'] = df_clean['company_name'].replace('nan', '').replace('None', '')
        
        # Clean and validate URLs
        if 'website_url' in df_clean.columns:
            df_clean['website_url'] = df_clean['website_url'].astype(str).str.strip()
            df_clean['website_url'] = df_clean['website_url'].apply(self.clean_url)
        
        # Clean phone numbers
        phone_columns = ['company_phone', 'contact_phone']
        for col in phone_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].apply(self.clean_phone)
        
        # Clean addresses
        address_columns = ['full_address', 'city', 'state', 'zip_code']
        for col in address_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace('nan', '').replace('None', '')
        
        # Clean emails
        if 'contact_email' in df_clean.columns:
            df_clean['contact_email'] = df_clean['contact_email'].astype(str).str.strip()
            df_clean['contact_email'] = df_clean['contact_email'].apply(self.clean_email)
        
        return df_clean
    
    def clean_url(self, url: str) -> str:
        """Clean and validate URL."""
        if pd.isna(url) or url in ['nan', 'None', '']:
            return ''
        
        url = str(url).strip()
        
        # Add http:// if no protocol
        if url and not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Basic URL validation
        try:
            result = urlparse(url)
            if result.netloc:
                return url
        except:
            pass
        
        return ''
    
    def clean_phone(self, phone: str) -> str:
        """Clean and normalize phone numbers."""
        if pd.isna(phone) or phone in ['nan', 'None', '']:
            return ''
        
        phone = str(phone).strip()
        
        # Remove common non-numeric characters
        phone = re.sub(r'[^\d\+\(\)\-\s]', '', phone)
        
        # Ensure it's not empty after cleaning
        if phone.strip():
            return phone.strip()
        
        return ''
    
    def clean_email(self, email: str) -> str:
        """Clean and validate email addresses."""
        if pd.isna(email) or email in ['nan', 'None', '']:
            return ''
        
        email = str(email).strip().lower()
        
        # Basic email validation
        if '@' in email and '.' in email.split('@')[1]:
            return email
        
        return ''
    
    def detect_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[int]]:
        """Detect and handle duplicates."""
        logger.info("Checking for duplicates...")
        
        # Create duplicate detection key
        duplicate_key_columns = []
        for col in ['company_name', 'website_url', 'contact_name', 'contact_email']:
            if col in df.columns:
                duplicate_key_columns.append(col)
        
        if not duplicate_key_columns:
            logger.warning("No columns available for duplicate detection")
            return df, []
        
        # Create a duplicate key
        df['duplicate_key'] = df[duplicate_key_columns].fillna('').astype(str).agg('|'.join, axis=1)
        
        # Find duplicates
        duplicates = df[df.duplicated(subset=['duplicate_key'], keep=False)]
        duplicate_indices = duplicates.index.tolist()
        
        if duplicate_indices:
            logger.warning(f"Found {len(duplicate_indices)} potential duplicate rows")
            # Keep first occurrence, mark others for removal
            df = df.drop_duplicates(subset=['duplicate_key'], keep='first')
        
        # Remove the duplicate key column
        df = df.drop(columns=['duplicate_key'])
        
        return df, duplicate_indices
    
    def add_required_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add any missing required columns with default values."""
        logger.info("Adding missing required columns...")
        
        required_columns = {
            'contact_name': '',
            'contact_email': '',
            'contact_title': '',
            'country': 'USA'
        }
        
        for col, default_value in required_columns.items():
            if col not in df.columns:
                df[col] = default_value
                logger.info(f"Added missing column: {col} with default value: {default_value}")
        
        return df
    
    def generate_cleanup_report(self, original_df: pd.DataFrame, cleaned_df: pd.DataFrame, 
                               column_mappings: Dict[str, str], unmapped_columns: List[str],
                               duplicate_indices: List[int]) -> str:
        """Generate a cleanup report."""
        report = []
        report.append("=" * 60)
        report.append("BROADWAY DATA CLEANUP REPORT")
        report.append("=" * 60)
        report.append(f"Original rows: {len(original_df)}")
        report.append(f"Cleaned rows: {len(original_df)}")
        report.append(f"Duplicates removed: {len(duplicate_indices)}")
        report.append("")
        
        report.append("COLUMN MAPPINGS:")
        for original_col, standard_name in column_mappings.items():
            report.append(f"  {original_col} → {standard_name}")
        
        if unmapped_columns:
            report.append("")
            report.append("UNMAPPED COLUMNS (kept as-is):")
            for col in unmapped_columns:
                report.append(f"  {col}")
        
        if duplicate_indices:
            report.append("")
            report.append("DUPLICATE ROWS REMOVED:")
            for idx in duplicate_indices:
                report.append(f"  Row {idx + 1}")
        
        report.append("")
        report.append("CLEANUP COMPLETE - Ready for database loading!")
        
        return "\n".join(report)
    
    def process_csv(self, input_path: str, output_path: str) -> bool:
        """Main method to process the CSV file."""
        try:
            logger.info(f"Processing CSV: {input_path}")
            
            # Read the CSV
            df = pd.read_csv(input_path)
            logger.info(f"Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
            
            # Detect column mappings
            column_mappings, unmapped_columns = self.detect_columns(df)
            
            # Clean the data
            df_clean = self.clean_data(df)
            
            # Detect and remove duplicates
            df_clean, duplicate_indices = self.detect_duplicates(df_clean)
            
            # Add missing required columns
            df_clean = self.add_required_columns(df_clean)
            
            # Save cleaned data
            df_clean.to_csv(output_path, index=False)
            logger.info(f"Saved cleaned data to: {output_path}")
            
            # Generate and display report
            report = self.generate_cleanup_report(df, df_clean, column_mappings, 
                                               unmapped_columns, duplicate_indices)
            print(report)
            
            # Save report to file
            report_path = output_path.replace('.csv', '_cleanup_report.txt')
            with open(report_path, 'w') as f:
                f.write(report)
            logger.info(f"Saved cleanup report to: {report_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            return False

def main():
    """Main function for command line usage."""
    if len(sys.argv) != 3:
        print("Usage: python data_cleanup.py <input_csv> <output_csv>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    if not os.path.exists(input_path):
        print(f"Error: Input file not found: {input_path}")
        sys.exit(1)
    
    cleanup = DataCleanup()
    success = cleanup.process_csv(input_path, output_path)
    
    if success:
        print("\n✅ Data cleanup completed successfully!")
    else:
        print("\n❌ Data cleanup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
