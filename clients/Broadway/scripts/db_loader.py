#!/usr/bin/env python3
"""
Database loader for Broadway summer camps project.
Loads cleaned CSV data into the new clean database structure.
"""

import pandas as pd
import os
import sys
from typing import Dict, List, Tuple, Optional
import logging
from db_connection import get_db_connection

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Load cleaned CSV data into the Broadway database."""
    
    def __init__(self):
        # Define the expected columns after cleanup
        self.expected_columns = [
            'company_name', 'website_url', 'company_phone', 'full_address', 
            'city', 'state', 'zip_code', 'country', 'contact_name', 
            'contact_email', 'contact_title', 'contact_phone',
            'lat', 'lon', 'rating', 'review_count', 'description', 
            'camp_type', 'place_id', 'age_range', 'session_length', 'specialties'
        ]
    
    def validate_csv_structure(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate that the CSV has the expected structure."""
        missing_columns = []
        available_columns = list(df.columns)
        
        # Check for required columns
        required_columns = ['company_name']
        for col in required_columns:
            if col not in available_columns:
                missing_columns.append(f"Required: {col}")
        
        # Check for at least one contact identifier
        contact_columns = ['contact_name', 'contact_email', 'contact_phone']
        if not any(col in available_columns for col in contact_columns):
            missing_columns.append("At least one contact field required")
        
        return len(missing_columns) == 0, missing_columns
    
    def check_existing_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """Check for existing data in the database to avoid duplicates."""
        logger.info("Checking for existing data in database...")
        
        new_records = []
        existing_records = []
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    for index, row in df.iterrows():
                        # Create a lookup key for this record
                        lookup_key = self.create_lookup_key(row)
                        
                        # Check if organization exists
                        existing_org = self.find_existing_organization(cur, lookup_key)
                        
                        if existing_org:
                            existing_records.append({
                                'row_index': index,
                                'company_name': row.get('company_name', ''),
                                'website_url': row.get('website_url', ''),
                                'existing_org_id': existing_org['org_id'],
                                'reason': 'Organization already exists in database'
                            })
                        else:
                            new_records.append({
                                'row_index': index,
                                'row_data': row
                            })
        
        except Exception as e:
            logger.error(f"Error checking existing data: {e}")
            # If we can't check, assume all records are new
            new_records = [{'row_index': i, 'row_data': row} for i, row in df.iterrows()]
        
        logger.info(f"Found {len(existing_records)} existing records, {len(new_records)} new records")
        return df.iloc[[r['row_index'] for r in new_records]], existing_records
    
    def create_lookup_key(self, row: pd.Series) -> Dict[str, str]:
        """Create a lookup key for duplicate detection."""
        return {
            'company_name': str(row.get('company_name', '')).strip().lower(),
            'website_url': str(row.get('website_url', '')).strip().lower(),
            'contact_name': str(row.get('contact_name', '')).strip().lower(),
            'contact_email': str(row.get('contact_email', '')).strip().lower()
        }
    
    def find_existing_organization(self, cur, lookup_key: Dict[str, str]) -> Optional[Dict]:
        """Find existing organization in database."""
        # Build query based on available lookup data
        conditions = []
        params = []
        
        if lookup_key['company_name']:
            conditions.append("LOWER(company_name) = %s")
            params.append(lookup_key['company_name'])
        
        if lookup_key['website_url']:
            conditions.append("LOWER(website_url) = %s")
            params.append(lookup_key['website_url'])
        
        if not conditions:
            return None
        
        query = f"""
            SELECT org_id, company_name, website_url 
            FROM summer_camps.organizations 
            WHERE {' OR '.join(conditions)}
            LIMIT 1
        """
        
        cur.execute(query, params)
        result = cur.fetchone()
        
        if result:
            return {
                'org_id': result[0],
                'company_name': result[1],
                'website_url': result[2]
            }
        
        return None
    
    def load_organizations(self, df: pd.DataFrame) -> List[int]:
        """Load organizations into the database."""
        logger.info("Loading organizations...")
        
        org_ids = []
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    for index, row in df.iterrows():
                        # Prepare organization data
                        org_data = {
                            'company_name': row.get('company_name', ''),
                            'website_url': row.get('website_url', ''),
                            'company_phone': row.get('contact_phone', ''),  # Fixed: CSV has 'contact_phone'
                            'street': row.get('full_address ', ''),  # Fixed: CSV has 'full_address ' (with space)
                            'city': row.get('city', ''),
                            'zip': row.get('zip_code', ''),
                            'state': row.get('state', ''),
                            'country': row.get('country', 'USA'),
                            'categories': row.get('camp_type', ''),
                            'fallback_email': '',  # Will be populated later
                            'notes': f"Loaded from CSV row {index + 1}"
                        }
                        
                        # Clean empty strings to None
                        org_data = {k: v if pd.notna(v) and v != '' else None for k, v in org_data.items()}
                        
                        # Insert organization
                        cur.execute("""
                            INSERT INTO summer_camps.organizations 
                            (company_name, website_url, company_phone, street, city, zip, state, country, categories, fallback_email, notes)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING org_id;
                        """, (
                            org_data['company_name'], org_data['website_url'], org_data['company_phone'],
                            org_data['street'], org_data['city'], org_data['zip'], org_data['state'],
                            org_data['country'], org_data['categories'], org_data['fallback_email'], org_data['notes']
                        ))
                        
                        org_id = cur.fetchone()[0]
                        org_ids.append(org_id)
                        
                        logger.info(f"Inserted organization {org_id}: {org_data['company_name']}")
                    
                    conn.commit()
                    logger.info(f"Successfully loaded {len(org_ids)} organizations")
                    
        except Exception as e:
            logger.error(f"Error loading organizations: {e}")
            raise
        
        return org_ids
    
    def load_contacts(self, df: pd.DataFrame, org_ids: List[int]) -> int:
        """Load contacts into the database."""
        logger.info("Loading contacts...")
        
        contact_count = 0
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    for index, (row, org_id) in enumerate(zip(df.iterrows(), org_ids)):
                        row = row[1]  # Get the actual row data
                        
                        # Prepare contact data
                        contact_data = {
                            'org_id': org_id,
                            'contact_name': row.get('contact_name', ''),
                            'contact_email': row.get('contact_email', ''),
                            'role_title': row.get('contact_title', ''),
                            'is_primary_contact': True,
                            'email_quality': self.determine_email_quality(row.get('contact_email', '')),
                            'notes': f"Primary contact from CSV row {index + 1}"
                        }
                        
                        # Clean empty strings to None
                        contact_data = {k: v if pd.notna(v) and v != '' else None for k, v in contact_data.items()}
                        
                        # Insert contact
                        cur.execute("""
                            INSERT INTO summer_camps.contacts 
                            (org_id, contact_name, contact_email, role_title, is_primary_contact, email_quality, notes)
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """, (
                            contact_data['org_id'], contact_data['contact_name'], contact_data['contact_email'],
                            contact_data['role_title'], contact_data['is_primary_contact'], 
                            contact_data['email_quality'], contact_data['notes']
                        ))
                        
                        contact_count += 1
                        logger.info(f"Inserted contact for organization {org_id}")
                    
                    conn.commit()
                    logger.info(f"Successfully loaded {contact_count} contacts")
                    
                    # Clean up null contacts (they'll be replaced by enrichment modules)
                    cur.execute("""
                        DELETE FROM summer_camps.contacts 
                        WHERE contact_name IS NULL OR contact_name = '' OR contact_name = 'None'
                    """)
                    null_contacts_deleted = cur.rowcount
                    conn.commit()
                    
                    if null_contacts_deleted > 0:
                        logger.info(f"Cleaned up {null_contacts_deleted} null contacts - enrichment modules will create real ones")
                    
        except Exception as e:
            logger.error(f"Error loading contacts: {e}")
            raise
        
        return contact_count
    
    def determine_email_quality(self, email: str) -> str:
        """Determine the quality of an email address."""
        if not email or pd.isna(email):
            return 'missing'
        
        email = str(email).lower()
        
        if email.startswith(('info@', 'contact@', 'hello@', 'admin@')):
            return 'generic'
        elif '@' in email and '.' in email.split('@')[1]:
            return 'direct'
        else:
            return 'invalid'
    
    def generate_loading_report(self, original_count: int, loaded_count: int, 
                               existing_records: List[Dict], org_ids: List[int], 
                               contact_count: int) -> str:
        """Generate a loading report."""
        report = []
        report.append("=" * 60)
        report.append("BROADWAY DATABASE LOADING REPORT")
        report.append("=" * 60)
        report.append(f"Original CSV rows: {original_count}")
        report.append(f"New organizations loaded: {len(org_ids)}")
        report.append(f"New contacts loaded: {contact_count}")
        report.append(f"Existing records skipped: {len(existing_records)}")
        report.append("")
        
        if existing_records:
            report.append("EXISTING RECORDS SKIPPED:")
            for record in existing_records:
                report.append(f"  Row {record['row_index'] + 1}: {record['company_name']} (Org ID: {record['existing_org_id']})")
        
        report.append("")
        report.append("NEW RECORDS LOADED:")
        for i, org_id in enumerate(org_ids):
            report.append(f"  Organization {org_id} (contact will be created by enrichment)")
        
        report.append("")
        report.append("NOTE: Null contacts are cleaned up - enrichment modules will create real contacts")
        report.append("")
        report.append("LOADING COMPLETE - Ready for enrichment pipeline!")
        
        return "\n".join(report)
    
    def load_csv_to_db(self, csv_path: str) -> bool:
        """Main method to load CSV data into the database."""
        try:
            logger.info(f"Loading CSV to database: {csv_path}")
            
            # Read the CSV
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded CSV with {len(df)} rows")
            
            # Validate structure
            is_valid, missing_columns = self.validate_csv_structure(df)
            if not is_valid:
                logger.error(f"CSV validation failed. Missing columns: {missing_columns}")
                return False
            
            # Check for existing data
            df_new, existing_records = self.check_existing_data(df)
            
            if len(df_new) == 0:
                logger.info("No new records to load")
                return True
            
            # Load organizations
            org_ids = self.load_organizations(df_new)
            
            # Load contacts
            contact_count = self.load_contacts(df_new, org_ids)
            
            # Generate and display report
            report = self.generate_loading_report(len(df), len(df_new), existing_records, org_ids, contact_count)
            print(report)
            
            # Save report to file
            report_path = csv_path.replace('.csv', '_loading_report.txt')
            with open(report_path, 'w') as f:
                f.write(report)
            logger.info(f"Saved loading report to: {report_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading CSV to database: {e}")
            return False

def main():
    """Main function for command line usage."""
    if len(sys.argv) != 2:
        print("Usage: python db_loader.py <cleaned_csv>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    loader = DatabaseLoader()
    success = loader.load_csv_to_db(csv_path)
    
    if success:
        print("\n✅ Database loading completed successfully!")
    else:
        print("\n❌ Database loading failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
