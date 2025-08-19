#!/usr/bin/env python3
"""
Apollo Leadership Role Search - Targeted Contact Discovery

This script searches Apollo for specific leadership roles (director, owner, etc.)
combined with company names for more targeted, API-efficient results.

Usage:
    python3 scripts/apollo_leadership_search.py --company-ids 22,23,24
    python3 scripts/apollo_leadership_search.py --test-roles
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

class ApolloLeadershipSearcher:
    """Search Apollo for specific leadership roles at companies."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_APOLLO_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_APOLLO_API_KEY")
        
        self.base_url = "https://api.apollo.io/v1"
        self.session = None
        
        # Leadership role keywords to search for - SENIOR POSITIONS ONLY
        self.leadership_roles = [
            "executive director",
            "camp director", 
            "program director",
            "managing director",
            "president",
            "ceo",
            "founder",
            "owner"
        ]
    
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
    
    async def search_leadership_by_role(self, company_name: str, role: str, website_url: str = None) -> List[Dict]:
        """Search for contacts with a specific leadership role at a company."""
        try:
            headers = {'X-Api-Key': self.api_key}
            
            # Build search parameters - combine role and company
            params = {
                'api_key': self.api_key,
                'page': 1,
                'per_page': 10,  # Limit results for efficiency
                'q_organization_name': company_name,
                'q_titles': role  # Search for specific title
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
            
            logger.info(f"Searching for '{role}' at {company_name}")
            
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
                                'website_url': person.get('organization', {}).get('website_url', website_url),
                                'search_role': role
                            }
                            contacts.append(contact)
                        
                        logger.info(f"Found {len(contacts)} '{role}' contacts for {company_name}")
                        return contacts
                    else:
                        logger.info(f"No '{role}' contacts found for {company_name}")
                        return []
                        
                else:
                    logger.warning(f"Search failed for {role} at {company_name}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error searching for {role} at {company_name}: {e}")
            return []
    
    async def search_company_leadership(self, company_name: str, website_url: str = None) -> Dict:
        """Search for leadership contacts using multiple role keywords."""
        all_contacts = []
        role_results = {}
        
        logger.info(f"🔍 Searching leadership roles for: {company_name}")
        
        for role in self.leadership_roles:
            contacts = await self.search_leadership_by_role(company_name, role, website_url)
            if contacts:
                role_results[role] = len(contacts)
                all_contacts.extend(contacts)
            
            # Small delay between role searches
            await asyncio.sleep(0.5)
        
        # Remove duplicates based on apollo_id
        unique_contacts = []
        seen_ids = set()
        for contact in all_contacts:
            if contact['apollo_id'] not in seen_ids:
                unique_contacts.append(contact)
                seen_ids.add(contact['apollo_id'])
        
        logger.info(f"Total unique leadership contacts found: {len(unique_contacts)}")
        
        return {
            'company_name': company_name,
            'total_contacts': len(unique_contacts),
            'role_breakdown': role_results,
            'contacts': unique_contacts
        }
    
    async def test_leadership_search(self) -> Dict:
        """Test leadership search with sample companies."""
        test_companies = [
            ("10Four Youth Center", "https://www.10four.io/"),
            ("18 Reasons", "https://www.18reasons.org/"),
            ("92Y Camps", None)
        ]
        
        logger.info("🧪 Testing leadership role search with sample companies...")
        
        results = []
        for company_name, website_url in test_companies:
            result = await self.search_company_leadership(company_name, website_url)
            results.append(result)
            
            # Display results
            print(f"\n📊 RESULTS FOR: {company_name}")
            print(f"   Total contacts: {result['total_contacts']}")
            print(f"   Role breakdown:")
            for role, count in result['role_breakdown'].items():
                print(f"     {role}: {count}")
            
            if result['contacts']:
                print(f"   Sample contacts:")
                for i, contact in enumerate(result['contacts'][:3], 1):
                    print(f"     {i}. {contact['name']} - {contact['title']}")
                    if contact['email'] and contact['email'] != 'email_not_unlocked@domain.com':
                        print(f"        Email: {contact['email']}")
        
        return {
            'test_companies': len(test_companies),
            'total_contacts_found': sum(r['total_contacts'] for r in results),
            'results': results
        }
    
    async def test_mohawk_camp(self) -> Dict:
        """Test senior leadership search specifically with Mohawk Day Camp."""
        company_name = "Mohawk. Day .Camp"
        website_url = "https://www.campmohawk.com/"
        
        logger.info(f"🧪 Testing SENIOR leadership search with: {company_name}")
        
        result = await self.search_company_leadership(company_name, website_url)
        
        # Display detailed results
        print(f"\n📊 SENIOR LEADERSHIP RESULTS FOR: {company_name}")
        print(f"   Website: {website_url}")
        print(f"   Total senior contacts: {result['total_contacts']}")
        print(f"   Role breakdown:")
        for role, count in result['role_breakdown'].items():
            print(f"     {role}: {count}")
        
        if result['contacts']:
            print(f"\n   Senior Leadership Contacts:")
            for i, contact in enumerate(result['contacts'], 1):
                print(f"     {i}. {contact['name']} - {contact['title']}")
                if contact['email'] and contact['email'] != 'email_not_unlocked@domain.com':
                    print(f"        Email: {contact['email']}")
                if contact['linkedin_url']:
                    print(f"        LinkedIn: {contact['linkedin_url']}")
                print(f"        Seniority: {contact['seniority']}")
                print(f"        Departments: {', '.join(contact['departments'])}")
                print()
        else:
            print("   No senior leadership contacts found")
        
        return result
    
    def generate_test_report(self, results: Dict) -> str:
        """Generate a test report."""
        report = []
        report.append("=" * 60)
        report.append("APOLLO LEADERSHIP ROLE SEARCH TEST REPORT")
        report.append("=" * 60)
        report.append(f"Test companies: {results['test_companies']}")
        report.append(f"Total contacts found: {results['total_contacts_found']}")
        report.append(f"Average contacts per company: {results['total_contacts_found']/results['test_companies']:.1f}")
        report.append("")
        
        for result in results['results']:
            report.append(f"COMPANY: {result['company_name']}")
            report.append(f"  Total contacts: {result['total_contacts']}")
            for role, count in result['role_breakdown'].items():
                report.append(f"    {role}: {count}")
            report.append("")
        
        return "\n".join(report)

async def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Apollo Leadership Role Search Test')
    parser.add_argument('--company-ids', type=str, help='Comma-separated list of company IDs to test')
    parser.add_argument('--test-roles', action='store_true', help='Run leadership role search test')
    parser.add_argument('--test-mohawk', action='store_true', help='Test senior leadership search with Mohawk Day Camp')
    
    args = parser.parse_args()
    
    if not args.test_roles and not args.company_ids and not args.test_mohawk:
        print("Error: Must specify either --test-roles, --test-mohawk, or --company-ids")
        sys.exit(1)
    
    try:
        async with ApolloLeadershipSearcher() as searcher:
            # Test connection
            if not await searcher.test_connection():
                print("❌ Apollo API connection failed")
                sys.exit(1)
            
            if args.test_roles:
                # Run leadership role search test
                results = await searcher.test_leadership_search()
                
                # Generate and display report
                report = searcher.generate_test_report(results)
                print("\n" + report)
                
                # Save report
                with open('outputs/apollo_leadership_search_test_report.txt', 'w') as f:
                    f.write(report)
                print(f"\n📋 Test report saved to outputs/apollo_leadership_search_test_report.txt")
            
            elif args.test_mohawk:
                # Test senior leadership search with Mohawk Day Camp
                result = await searcher.test_mohawk_camp()
                
                # Save detailed report
                with open('outputs/apollo_mohawk_senior_leadership_report.txt', 'w') as f:
                    f.write(f"Mohawk Day Camp Senior Leadership Search Results\n")
                    f.write(f"Generated: {datetime.now()}\n\n")
                    f.write(f"Company: {result['company_name']}\n")
                    f.write(f"Total contacts: {result['total_contacts']}\n\n")
                    f.write("Role breakdown:\n")
                    for role, count in result['role_breakdown'].items():
                        f.write(f"  {role}: {count}\n")
                    f.write("\nContacts:\n")
                    for contact in result['contacts']:
                        f.write(f"  {contact['name']} - {contact['title']}\n")
                        if contact['email']:
                            f.write(f"    Email: {contact['email']}\n")
                        if contact['linkedin_url']:
                            f.write(f"    LinkedIn: {contact['linkedin_url']}\n")
                        f.write(f"    Seniority: {contact['seniority']}\n")
                        f.write(f"    Departments: {', '.join(contact['departments'])}\n\n")
                
                print(f"\n📋 Detailed report saved to outputs/apollo_mohawk_senior_leadership_report.txt")
            
            elif args.company_ids:
                # Process specific companies (future implementation)
                company_ids = [int(x.strip()) for x in args.company_ids.split(',')]
                print(f"🔍 Processing specific companies: {company_ids}")
                print("This feature will be implemented next...")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
