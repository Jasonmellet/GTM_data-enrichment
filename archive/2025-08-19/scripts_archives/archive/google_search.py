#!/usr/bin/env python3
"""
Google Custom Search enricher for Broadway summer camps project.
Uses Google Custom Search API to find additional business and contact information.
"""

import os
import asyncio
import logging
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import argparse
import aiohttp
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleSearchEnricher:
    """Enrich summer camp data using Google Custom Search API."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_GOOGLE_CUSTOM_SEARCH_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_GOOGLE_CUSTOM_SEARCH_API_KEY")
        
        # You'll need to create a Custom Search Engine and get the search engine ID
        # https://programmablesearchengine.google.com/
        self.search_engine_id = os.getenv('BROADWAY_GOOGLE_SEARCH_ENGINE_ID', 'a412ec9c8f14b482a')
        if not self.search_engine_id:
            logger.warning("Missing BROADWAY_GOOGLE_SEARCH_ENGINE_ID - please create a Custom Search Engine")
            logger.warning("Visit: https://programmablesearchengine.google.com/")
        
        self.base_url = "https://www.googleapis.com/customsearch/v1"
        
        # Track API usage
        self.api_calls = 0
        self.total_results = 0
    
    async def test_api_connection(self) -> bool:
        """Test the Google Custom Search API connection."""
        try:
            # Simple test query
            test_query = "summer camp"
            params = {
                'key': self.api_key,
                'cx': self.search_engine_id,
                'q': test_query,
                'num': 1  # Just 1 result for testing
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        logger.info(f"✅ Google Custom Search API connection successful!")
                        logger.info(f"   Search engine ID: {self.search_engine_id}")
                        logger.info(f"   API key: {self.api_key[:10]}...")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Google Custom Search API error: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error testing Google Custom Search API: {e}")
            return False
    
    async def search_business_info(self, company_name: str, website_url: str = None) -> Optional[Dict]:
        """Search for business information using Google Custom Search."""
        try:
            # Create search queries for business info
            search_queries = [
                f'"{company_name}" summer camp',
                f'"{company_name}" contact information',
                f'"{company_name}" address phone',
                f'"{company_name}" about us'
            ]
            
            if website_url:
                domain = website_url.replace('https://', '').replace('http://', '').split('/')[0]
                search_queries.append(f'"{company_name}" site:{domain}')
            
            all_results = []
            
            for query in search_queries:
                params = {
                    'key': self.api_key,
                    'cx': self.search_engine_id,
                    'q': query,
                    'num': 5,  # Get 5 results per query
                    'dateRestrict': 'y1'  # Restrict to last year for recent info
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.base_url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.api_calls += 1
                            
                            if 'items' in data:
                                for item in data['items']:
                                    result = {
                                        'title': item.get('title', ''),
                                        'snippet': item.get('snippet', ''),
                                        'link': item.get('link', ''),
                                        'query': query
                                    }
                                    all_results.append(result)
                                    self.total_results += 1
                            
                            # Small delay between requests
                            await asyncio.sleep(0.5)
                        else:
                            logger.warning(f"Search failed for query '{query}': {response.status}")
            
            return {
                'company_name': company_name,
                'search_queries': search_queries,
                'results': all_results,
                'total_results': len(all_results)
            }
            
        except Exception as e:
            logger.error(f"Error searching for business info: {e}")
            return None
    
    async def search_contact_info(self, contact_name: str, company_name: str) -> Optional[Dict]:
        """Search for specific contact information."""
        try:
            # Create search queries for contact info
            search_queries = [
                f'"{contact_name}" "{company_name}"',
                f'"{contact_name}" "{company_name}" email contact',
                f'"{contact_name}" "{company_name}" linkedin',
                f'"{contact_name}" "{company_name}" director owner'
            ]
            
            all_results = []
            
            for query in search_queries:
                params = {
                    'key': self.api_key,
                    'cx': self.search_engine_id,
                    'q': query,
                    'num': 3,  # Get 3 results per query
                    'dateRestrict': 'y2'  # Restrict to last 2 years
                }
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.base_url, params=params, timeout=30) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.api_calls += 1
                            
                            if 'items' in data:
                                for item in data['items']:
                                    result = {
                                        'title': item.get('title', ''),
                                        'snippet': item.get('snippet', ''),
                                        'link': item.get('link', ''),
                                        'query': query
                                    }
                                    all_results.append(result)
                                    self.total_results += 1
                            
                            # Small delay between requests
                            await asyncio.sleep(0.5)
                        else:
                            logger.warning(f"Contact search failed for query '{query}': {response.status}")
            
            return {
                'contact_name': contact_name,
                'company_name': company_name,
                'search_queries': search_queries,
                'results': all_results,
                'total_results': len(all_results)
            }
            
        except Exception as e:
            logger.error(f"Error searching for contact info: {e}")
            return None
    
    def extract_contact_info(self, search_results: Dict) -> Dict:
        """Extract contact information from search results."""
        extracted_info = {
            'emails': [],
            'phones': [],
            'addresses': [],
            'social_media': [],
            'news_articles': []
        }
        
        for result in search_results.get('results', []):
            text = f"{result['title']} {result['snippet']}"
            
            # Extract emails
            import re
            emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            extracted_info['emails'].extend(emails)
            
            # Extract phone numbers
            phones = re.findall(r'\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})', text)
            for phone in phones:
                if isinstance(phone, tuple):
                    phone = ''.join(phone)
                extracted_info['phones'].append(phone)
            
            # Extract social media links
            if any(platform in result['link'].lower() for platform in ['linkedin', 'facebook', 'twitter', 'instagram']):
                extracted_info['social_media'].append(result['link'])
            
            # Identify news articles
            if any(keyword in result['link'].lower() for keyword in ['news', 'article', 'press', 'blog']):
                extracted_info['news_articles'].append(result['link'])
        
        # Remove duplicates
        for key in extracted_info:
            extracted_info[key] = list(set(extracted_info[key]))
        
        return extracted_info
    
    def generate_search_report(self, business_results: Dict, contact_results: List[Dict]) -> str:
        """Generate a summary report of search results."""
        report = []
        report.append("=" * 60)
        report.append("GOOGLE CUSTOM SEARCH ENRICHMENT REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total API Calls Made: {self.api_calls}")
        report.append(f"Total Results Found: {self.total_results}")
        report.append("")
        
        if business_results:
            report.append(f"BUSINESS SEARCH RESULTS:")
            report.append(f"  Company: {business_results['company_name']}")
            report.append(f"  Queries: {len(business_results['search_queries'])}")
            report.append(f"  Results: {business_results['total_results']}")
            
            # Extract and show contact info
            contact_info = self.extract_contact_info(business_results)
            if contact_info['emails']:
                report.append(f"  Emails Found: {len(contact_info['emails'])}")
            if contact_info['phones']:
                report.append(f"  Phones Found: {len(contact_info['phones'])}")
            if contact_info['social_media']:
                report.append(f"  Social Media: {len(contact_info['social_media'])}")
        
        if contact_results:
            report.append("")
            report.append("CONTACT SEARCH RESULTS:")
            for contact in contact_results:
                report.append(f"  {contact['contact_name']} ({contact['company_name']}): {contact['total_results']} results")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Google Custom Search Enricher for Broadway Summer Camps')
    parser.add_argument('--test', action='store_true', help='Test API connection only')
    parser.add_argument('--company', type=str, help='Company name to search for')
    parser.add_argument('--contact', type=str, help='Contact name to search for (requires --company)')
    parser.add_argument('--output', type=str, default='google_search_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize enricher
    try:
        enricher = GoogleSearchEnricher()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return
    
    # Test API connection first
    logger.info("Testing Google Custom Search API connection...")
    if not await enricher.test_api_connection():
        logger.error("❌ API connection failed. Please check your configuration.")
        return
    
    if args.test:
        logger.info("✅ API test completed successfully!")
        return
    
    # Run searches
    results = []
    
    if args.company:
        logger.info(f"Searching for business information: {args.company}")
        business_results = await enricher.search_business_info(args.company)
        if business_results:
            results.append(business_results)
            
            # Extract contact info
            contact_info = enricher.extract_contact_info(business_results)
            logger.info(f"Extracted contact info: {contact_info}")
    
    if args.contact and args.company:
        logger.info(f"Searching for contact information: {args.contact} at {args.company}")
        contact_results = await enricher.search_contact_info(args.contact, args.company)
        if contact_results:
            results.append(contact_results)
    
    # Generate and display report
    if results:
        report = enricher.generate_search_report(
            results[0] if results else None,
            results[1:] if len(results) > 1 else []
        )
        print(report)
        
        # Save results to file
        import pandas as pd
        # Flatten results for CSV
        csv_rows = []
        for result in results:
            if 'company_name' in result:  # Business search result
                for search_result in result['results']:
                    csv_rows.append({
                        'search_type': 'business',
                        'company_name': result['company_name'],
                        'title': search_result['title'],
                        'snippet': search_result['snippet'],
                        'link': search_result['link'],
                        'query': search_result['query']
                    })
            elif 'contact_name' in result:  # Contact search result
                for search_result in result['results']:
                    csv_rows.append({
                        'search_type': 'contact',
                        'contact_name': result['contact_name'],
                        'company_name': result['company_name'],
                        'title': search_result['title'],
                        'snippet': search_result['snippet'],
                        'link': search_result['link'],
                        'query': search_result['query']
                    })
        
        if csv_rows:
            df = pd.DataFrame(csv_rows)
            df.to_csv(args.output, index=False)
            logger.info(f"Saved {len(csv_rows)} search results to {args.output}")
    
    logger.info(f"Google Custom Search enrichment completed. Total API calls: {enricher.api_calls}")

if __name__ == "__main__":
    asyncio.run(main())
