#!/usr/bin/env python3
"""
Apollo Company-Focused Contact Discovery

This script searches Apollo for contacts by company name to discover
leadership contacts for all companies in the database.

Usage:
    python3 scripts/apollo_company_search.py --all-companies
    python3 scripts/apollo_company_search.py --company-ids 1,2,3
    python3 scripts/apollo_company_search.py --workers 10
"""

import asyncio
import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import aiohttp

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_connection import get_db_connection

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ApolloCompanySearcher:
    """Search Apollo for company contacts."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_APOLLO_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_APOLLO_API_KEY")
        
        self.base_url = "https://api.apollo.io/v1"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def test_connection(self) -> bool:
        """Test Apollo API connection."""
        try:
            headers = {'X-Api-Key': self.api_key}
            params = {
                'api_key': self.api_key,
                'q_organization_domains': 'google.com',
                'page': 1,
                'per_page': 1
            }
            
            async with self.session.get(f"{self.base_url}/people/search", headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Apollo API connection successful!")
                    logger.info(f"   API key: {self.api_key[:8]}...")
                    return True
                else:
                    logger.error(f"❌ Apollo API connection failed: {response.status}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Apollo API connection error: {e}")
            return False
    
    async def search_company_contacts(self, company_name: str, website_url: str = None) -> List[Dict]:
        """Search for contacts at a specific company."""
        try:
            headers = {'X-Api-Key': self.api_key}
            
            # Build search parameters
            params = {
                'api_key': self.api_key,
                'page': 1,
                'per_page': 25,  # Get more contacts per company
                'q_organization_name': company_name
            }
            
            # Add website domain if available
            if website_url:
                from urllib.parse import urlparse
                try:
                    parsed = urlparse(website_url)
                    domain = parsed.netloc.lower()
                    if domain.startswith('www.'):
                        domain = domain[4:]
                    params['q_organization_domains'] = domain
                except:
                    pass
            
            # Search for people
            async with self.session.get(f"{self.base_url}/people/search", headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if 'people' in data and data['people']:
                        contacts = []
                        for person in data['people']:
                            contact = {
                                'apollo_id': person.get('id'),
                                'name': person.get('name', ''),
                                'first_name': person.get('first_name', ''),
                                'last_name': person.get('last_name', ''),
                                'title': person.get('title', ''),
                                'email': person.get('email', ''),
                                'email_status': person.get('email_status', ''),
                                'linkedin_url': person.get('linkedin_url', ''),
                                'seniority': person.get('seniority', ''),
                                'departments': person.get('departments', []),
                                'functions': person.get('functions', []),
                                'organization_name': person.get('organization', {}).get('name', company_name),
                                'website_url': person.get('organization', {}).get('website_url', website_url)
                            }
                            contacts.append(contact)
                        
                        logger.info(f"Found {len(contacts)} contacts for {company_name}")
                        return contacts
                    else:
                        logger.info(f"No contacts found for {company_name}")
                        return []
                        
                else:
                    logger.warning(f"Search failed for {company_name}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error searching {company_name}: {e}")
            return []
    
    async def get_companies_without_contacts(self) -> List[Tuple[int, str, str]]:
        """Get companies that don't have any contacts."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT o.org_id, o.company_name, o.website_url
                    FROM summer_camps.organizations o
                    LEFT JOIN summer_camps.contacts c ON o.org_id = c.org_id
                    WHERE c.contact_id IS NULL
                    ORDER BY o.org_id
                """)
                return cur.fetchall()
    
    async def save_contacts_to_database(self, org_id: int, contacts: List[Dict]) -> int:
        """Save discovered contacts to database."""
        if not contacts:
            return 0
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    saved_count = 0
                    
                    for contact in contacts:
                        # Check if contact already exists
                        cur.execute("""
                            SELECT contact_id FROM summer_camps.contacts 
                            WHERE org_id = %s AND contact_name = %s
                        """, (org_id, contact['name']))
                        
                        if cur.fetchone():
                            continue  # Skip if already exists
                        
                        # Insert new contact
                        cur.execute("""
                            INSERT INTO summer_camps.contacts 
                            (org_id, contact_name, contact_email, role_title, email_quality, 
                             last_enriched_at, notes, created_at)
                            VALUES (%s, %s, %s, %s, %s, NOW(), %s, NOW())
                        """, (
                            org_id,
                            contact['name'],
                            contact['email'] if contact['email'] != 'email_not_unlocked@domain.com' else None,
                            contact['title'],
                            'direct' if contact['email'] and contact['email'] != 'email_not_unlocked@domain.com' else 'unknown',
                            f"Discovered via Apollo company search | Seniority: {contact['seniority']} | Departments: {', '.join(contact['departments'])}"
                        ))
                        
                        saved_count += 1
                    
                    conn.commit()
                    logger.info(f"Saved {saved_count} new contacts for organization {org_id}")
                    return saved_count
                    
        except Exception as e:
            logger.error(f"Error saving contacts for org {org_id}: {e}")
            return 0
    
    async def process_companies(self, company_ids: Optional[List[int]] = None, max_workers: int = 5) -> Dict:
        """Process companies to discover contacts."""
        if company_ids:
            # Get specific companies
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    placeholders = ','.join(['%s'] * len(company_ids))
                    cur.execute(f"""
                        SELECT org_id, company_name, website_url
                        FROM summer_camps.organizations 
                        WHERE org_id IN ({placeholders})
                        ORDER BY org_id
                    """, company_ids)
                    companies = cur.fetchall()
        else:
            # Get all companies without contacts
            companies = await self.get_companies_without_contacts()
        
        logger.info(f"Processing {len(companies)} companies with {max_workers} workers")
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_workers)
        
        async def process_company(company):
            async with semaphore:
                org_id, company_name, website_url = company
                
                logger.info(f"Searching contacts for: {company_name}")
                
                # Search for contacts
                contacts = await self.search_company_contacts(company_name, website_url)
                
                # Save to database
                saved_count = await self.save_contacts_to_database(org_id, contacts)
                
                # Small delay to respect rate limits
                await asyncio.sleep(1)
                
                return {
                    'org_id': org_id,
                    'company_name': company_name,
                    'contacts_found': len(contacts),
                    'contacts_saved': saved_count
                }
        
        # Process companies concurrently
        tasks = [process_company(company) for company in companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        valid_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Company processing error: {result}")
            else:
                valid_results.append(result)
        
        return {
            'total_companies': len(companies),
            'processed': len(valid_results),
            'total_contacts_found': sum(r['contacts_found'] for r in valid_results),
            'total_contacts_saved': sum(r['contacts_saved'] for r in valid_results),
            'results': valid_results
        }
    
    def generate_report(self, results: Dict) -> str:
        """Generate a comprehensive report."""
        report = []
        report.append("=" * 60)
        report.append("APOLLO COMPANY CONTACT DISCOVERY REPORT")
        report.append("=" * 60)
        report.append(f"Total companies processed: {results['total_companies']}")
        report.append(f"Successfully processed: {results['processed']}")
        report.append(f"Total contacts discovered: {results['total_contacts_found']}")
        report.append(f"Total contacts saved: {results['total_contacts_saved']}")
        report.append(f"Average contacts per company: {results['total_contacts_found']/results['total_companies']:.1f}")
        report.append("")
        
        # Top companies by contact count
        top_companies = sorted(results['results'], key=lambda x: x['contacts_found'], reverse=True)[:10]
        report.append("TOP 10 COMPANIES BY CONTACTS DISCOVERED:")
        for i, company in enumerate(top_companies, 1):
            report.append(f"{i:2d}. {company['company_name'][:40]:<40} | {company['contacts_found']:2d} contacts")
        
        return "\n".join(report)

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Apollo Company Contact Discovery')
    parser.add_argument('--company-ids', type=str, help='Comma-separated list of company IDs to process')
    parser.add_argument('--all-companies', action='store_true', help='Process all companies without contacts')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers (default: 5)')
    parser.add_argument('--output', type=str, default='apollo_company_discovery_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    if not args.company_ids and not args.all_companies:
        print("Error: Must specify either --company-ids or --all-companies")
        sys.exit(1)
    
    try:
        async with ApolloCompanySearcher() as searcher:
            # Test connection
            if not await searcher.test_connection():
                print("❌ Apollo API connection failed")
                sys.exit(1)
            
            # Parse company IDs if specified
            company_ids = None
            if args.company_ids:
                company_ids = [int(x.strip()) for x in args.company_ids.split(',')]
                print(f"🔍 Processing specific companies: {company_ids}")
            else:
                print("🔍 Processing all companies without contacts...")
            
            # Process companies
            results = await searcher.process_companies(company_ids, args.workers)
            
            # Generate and display report
            report = searcher.generate_report(results)
            print("\n" + report)
            
            # Save results to CSV
            import pandas as pd
            df = pd.DataFrame(results['results'])
            df.to_csv(f"outputs/{args.output}", index=False)
            print(f"\n📊 Results saved to outputs/{args.output}")
            
            # Save report
            with open('outputs/apollo_company_discovery_report.txt', 'w') as f:
                f.write(report)
            print(f"📋 Report saved to outputs/apollo_company_discovery_report.txt")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
