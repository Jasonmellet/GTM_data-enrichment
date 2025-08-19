#!/usr/bin/env python3
"""
Google Maps API enricher for Broadway summer camps project.
Uses Google Maps Places API to find missing business data and verify business status.
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
from db_connection import get_db_connection

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleMapsEnricher:
    """Enrich summer camp data using Google Maps Places API."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_GOOGLE_MAPS_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_GOOGLE_MAPS_API_KEY")
        
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        
        # Track API usage and costs
        self.api_calls = 0
        self.total_results = 0
        
        # Google Maps API pricing (approximate)
        self.cost_per_search = 0.017  # $0.017 per search
        self.cost_per_details = 0.017  # $0.017 per place details
    
    async def test_api_connection(self) -> bool:
        """Test the Google Maps API connection."""
        try:
            # Simple test query using Places API
            test_query = "summer camp"
            params = {
                'query': test_query,
                'key': self.api_key
            }
            
            url = f"{self.base_url}/textsearch/json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        if data.get('status') == 'OK':
                            logger.info(f"✅ Google Maps API connection successful!")
                            logger.info(f"   API key: {self.api_key[:10]}...")
                            return True
                        elif data.get('status') == 'ZERO_RESULTS':
                            logger.info(f"✅ Google Maps API connection successful! (no results for test query)")
                            logger.info(f"   API key: {self.api_key[:10]}...")
                            return True
                        else:
                            logger.error(f"❌ Google Maps API error: {data.get('status')} - {data.get('error_message', 'Unknown error')}")
                            return False
                    else:
                        logger.error(f"❌ HTTP error: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error testing Google Maps API: {e}")
            return False
    
    async def search_business(self, company_name: str, address: str = None) -> Optional[Dict]:
        """Search for a business using Google Maps API."""
        try:
            # Build search query
            if address:
                query = f"{company_name} {address}"
            else:
                query = company_name
            
            params = {
                'query': query,
                'key': self.api_key
            }
            
            url = f"{self.base_url}/textsearch/json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        if data.get('status') == 'OK' and data.get('results'):
                            result = data['results'][0]
                            logger.info(f"Found business: {result.get('name')}")
                            return result
                        else:
                            logger.warning(f"No business found for: {company_name}")
                            return None
                    else:
                        logger.error(f"Search failed for {company_name}: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error searching for business {company_name}: {e}")
            return None
    
    async def get_place_details(self, place_id: str) -> Optional[Dict]:
        """Get detailed information about a place."""
        try:
            params = {
                'place_id': place_id,
                'key': self.api_key,
                'fields': 'name,formatted_address,formatted_phone_number,website,opening_hours,rating,user_ratings_total,types,business_status,geometry'
            }
            
            url = f"{self.base_url}/details/json"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        if data.get('status') == 'OK':
                            result = data.get('result', {})
                            logger.info(f"Retrieved details for: {result.get('name')}")
                            return result
                        else:
                            logger.error(f"Details failed for place {place_id}: {data.get('status')}")
                            return None
                    else:
                        logger.error(f"Details request failed: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting place details: {e}")
            return None
    
    async def enrich_organization(self, org_id: int, company_name: str, existing_data: Dict) -> Dict:
        """Enrich a single organization with Google Maps data."""
        logger.info(f"Enriching {company_name} (ID: {org_id}) with Google Maps data")
        
        # Check what data we already have (unless force is enabled)
        missing_fields = []
        if not existing_data.get('company_phone'):
            missing_fields.append('phone')
        if not existing_data.get('street') or not existing_data.get('city'):
            missing_fields.append('address')
        
        # If force is enabled, we'll enrich regardless of existing data
        if not missing_fields and not getattr(self, 'force_enrichment', False):
            logger.info(f"{company_name} already has complete data, skipping")
            return {'org_id': org_id, 'enriched': False, 'reason': 'Already complete'}
        
        # Search for the business
        search_result = await self.search_business(company_name, existing_data.get('street'))
        
        if not search_result:
            return {'org_id': org_id, 'enriched': False, 'reason': 'Business not found'}
        
        # Extract useful data from search result (textsearch provides most data directly)
        enrichment_data = {
            'org_id': org_id,
            'place_id': search_result.get('place_id'),
            'business_name': search_result.get('name'),
            'formatted_address': search_result.get('formatted_address'),
            'phone_number': search_result.get('formatted_phone_number'),
            'website': search_result.get('website'),
            'business_status': search_result.get('business_status'),
            'rating': search_result.get('rating'),
            'review_count': search_result.get('user_ratings_total'),
            'types': search_result.get('types', []),
            'opening_hours': search_result.get('opening_hours', {}),
            'enrichment_timestamp': datetime.now()
        }
        
        logger.info(f"Extracted data: Phone: {enrichment_data['phone_number']}, Address: {enrichment_data['formatted_address']}")
        
        # Parse the formatted address to extract city, state, zip
        address_parts = self.parse_formatted_address(enrichment_data['formatted_address'])
        
        # Persist the enriched data to the database
        if await self.persist_enrichment_to_db(org_id, enrichment_data, address_parts):
            enrichment_data['enriched'] = True
            enrichment_data['reason'] = 'Successfully enriched'
            logger.info(f"Successfully enriched {company_name} with Google Maps data")
        else:
            enrichment_data['enriched'] = False
            enrichment_data['reason'] = 'Database update failed'
            logger.error(f"Failed to persist enrichment data for {company_name}")
        
        return enrichment_data
    
    def parse_formatted_address(self, formatted_address: str) -> Dict[str, str]:
        """Parse Google Maps formatted address into components."""
        if not formatted_address:
            return {}
        
        # Google Maps format is typically: "Street, City, State ZIP, Country"
        parts = formatted_address.split(', ')
        
        address_parts = {}
        
        if len(parts) >= 1:
            address_parts['street'] = parts[0].strip()
        
        # Find city - look for the first part that doesn't look like a street continuation
        city_found = False
        for i in range(1, min(len(parts), 4)):
            part = parts[i].strip()
            part_lower = part.lower()
            
            # Skip if this looks like street continuation
            if (any(char.isdigit() for char in part) or 
                any(word in part_lower for word in ['st', 'street', 'ave', 'avenue', 'blvd', 'cir', 'circle', 'ln', 'lane', 'dr', 'drive', 'rd', 'road', 'way', 'ct', 'court', 'suite', 'ste', '#', 'corporate offices'])):
                continue
            
            # Skip if this looks like a country
            if part_lower in ['united states', 'usa', 'us']:
                continue
            
            # This looks like a city
            address_parts['city'] = part
            city_found = True
            break
        
        # Find state and ZIP - look for the pattern "State ZIP"
        for i in range(1, len(parts)):
            part = parts[i].strip()
            if ' ' in part and any(char.isdigit() for char in part):
                # This might be "State ZIP"
                state_zip_parts = part.split()
                if len(state_zip_parts) >= 2 and state_zip_parts[-1].isdigit():
                    # Last part is ZIP, everything before is state
                    zip_code = state_zip_parts[-1]
                    state = ' '.join(state_zip_parts[:-1])
                    address_parts['state'] = state
                    address_parts['zip'] = zip_code
                    break
        
        return address_parts
    
    async def persist_enrichment_to_db(self, org_id: int, enrichment_data: Dict, address_parts: Dict) -> bool:
        """Persist enriched data to the database."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Update organization with enriched data
                    update_fields = []
                    update_values = []
                    
                    # Update address fields if we have them
                    if address_parts.get('city'):
                        update_fields.append("city = %s")
                        update_values.append(address_parts['city'])
                    
                    if address_parts.get('state'):
                        update_fields.append("state = %s")
                        update_values.append(address_parts['state'])
                    
                    if address_parts.get('zip'):
                        update_fields.append("zip = %s")
                        update_values.append(address_parts['zip'])
                    
                    # Update phone if we found one
                    if enrichment_data.get('phone_number'):
                        update_fields.append("company_phone = %s")
                        update_values.append(enrichment_data['phone_number'])
                    
                    # Update categories if we found them
                    if enrichment_data.get('types'):
                        categories = ', '.join(enrichment_data['types'])
                        update_fields.append("categories = %s")
                        update_values.append(categories)
                    
                    # Mark as maps verified
                    update_fields.append("maps_verified = %s")
                    update_values.append(True)
                    
                    update_fields.append("last_maps_check = %s")
                    update_values.append(datetime.now())
                    
                    if update_fields:
                        # Add org_id to the end for the WHERE clause
                        update_values.append(org_id)
                        
                        # Build and execute the update query
                        update_query = f"""
                            UPDATE summer_camps.organizations 
                            SET {', '.join(update_fields)}
                            WHERE org_id = %s
                        """
                        
                        cur.execute(update_query, update_values)
                        conn.commit()
                        
                        logger.info(f"Updated organization {org_id} with Google Maps data")
                        return True
                    
                    return True  # No fields to update, but that's okay
                    
        except Exception as e:
            logger.error(f"Error persisting enrichment data to database: {e}")
            return False
    
    async def enrich_all_organizations(self, org_ids: Optional[List[int]] = None) -> List[Dict]:
        """Enrich all organizations or specific ones with Google Maps data."""
        # Get organizations to enrich
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if org_ids:
                    placeholders = ','.join(['%s'] * len(org_ids))
                    cur.execute(f"""
                        SELECT org_id, company_name, company_phone, street, city, state, zip 
                        FROM summer_camps.organizations 
                        WHERE org_id IN ({placeholders})
                        ORDER BY org_id
                    """, org_ids)
                else:
                    cur.execute("""
                        SELECT org_id, company_name, company_phone, street, city, state, zip 
                        FROM summer_camps.organizations 
                        ORDER BY org_id
                    """)
                
                organizations = cur.fetchall()
        
        logger.info(f"Starting Google Maps enrichment of {len(organizations)} organizations")
        
        # Process organizations sequentially to avoid rate limiting
        results = []
        for org in organizations:
            existing_data = {
                'company_phone': org[2],
                'street': org[3],
                'city': org[4],
                'state': org[5],
                'zip': org[6]
            }
            
            result = await self.enrich_organization(org[0], org[1], existing_data)
            results.append(result)
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        return results
    
    def save_enrichment_results(self, results: List[Dict], output_file: str):
        """Save enrichment results to CSV."""
        if not results:
            logger.warning("No results to save")
            return
        
        # Flatten results for CSV
        csv_rows = []
        for result in results:
            if result.get('enriched'):
                csv_rows.append({
                    'org_id': result['org_id'],
                    'place_id': result.get('place_id', ''),
                    'business_name': result.get('business_name', ''),
                    'formatted_address': result.get('formatted_address', ''),
                    'phone_number': result.get('phone_number', ''),
                    'website': result.get('website', ''),
                    'business_status': result.get('business_status', ''),
                    'rating': result.get('rating', ''),
                    'review_count': result.get('review_count', ''),
                    'types': ','.join(result.get('types', [])),
                    'enrichment_timestamp': result.get('enrichment_timestamp', '')
                })
            else:
                csv_rows.append({
                    'org_id': result['org_id'],
                    'enriched': False,
                    'reason': result.get('reason', 'Unknown')
                })
        
        # Save to CSV
        import pandas as pd
        df = pd.DataFrame(csv_rows)
        df.to_csv(output_file, index=False)
        logger.info(f"Saved {len(csv_rows)} enrichment results to {output_file}")
    
    def generate_enrichment_report(self, results: List[Dict]) -> str:
        """Generate a summary report of enrichment results."""
        report = []
        report.append("=" * 60)
        report.append("GOOGLE MAPS ENRICHMENT REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Organizations Processed: {len(results)}")
        report.append(f"Total API Calls Made: {self.api_calls}")
        
        # Calculate costs
        total_cost = (self.api_calls * self.cost_per_search) + (self.api_calls * self.cost_per_details)
        report.append(f"Estimated API Cost: ${total_cost:.3f}")
        report.append("")
        
        enriched_count = sum(1 for r in results if r.get('enriched'))
        report.append(f"Organizations Enriched: {enriched_count}")
        report.append(f"Organizations Skipped: {len(results) - enriched_count}")
        report.append("")
        
        if enriched_count > 0:
            report.append("ENRICHMENT RESULTS:")
            for result in results:
                if result.get('enriched'):
                    report.append(f"\n{result.get('business_name', 'Unknown')} (ID: {result['org_id']}):")
                    if result.get('phone_number'):
                        report.append(f"  Phone: {result['phone_number']}")
                    if result.get('formatted_address'):
                        report.append(f"  Address: {result['formatted_address']}")
                    if result.get('business_status'):
                        report.append(f"  Status: {result['business_status']}")
                    if result.get('rating'):
                        report.append(f"  Rating: {result['rating']}/5 ({result.get('review_count', 0)} reviews)")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Google Maps Enricher for Broadway Summer Camps')
    parser.add_argument('--test', action='store_true', help='Test API connection only')
    parser.add_argument('--org-ids', type=str, help='Comma-separated list of organization IDs to enrich')
    parser.add_argument('--all', action='store_true', help='Enrich all organizations in database')
    parser.add_argument('--force', action='store_true', help='Force re-enrichment even if data exists')
    parser.add_argument('--output', type=str, default='google_maps_enrichment_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize enricher
    try:
        enricher = GoogleMapsEnricher()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return
    
    # Test API connection first
    logger.info("Testing Google Maps API connection...")
    if not await enricher.test_api_connection():
        logger.error("❌ API connection failed. Please check your configuration.")
        return
    
    if args.test:
        logger.info("✅ API test completed successfully!")
        return
    
    # Determine which organizations to enrich
    org_ids = None
    if args.org_ids:
        org_ids = [int(x.strip()) for x in args.org_ids.split(',')]
    elif not args.all:
        # Default to first 3 for testing
        org_ids = [1, 2, 3]
        logger.info("No specific orgs specified, testing with first 3 organizations")
    
    # Set force enrichment flag if requested
    if args.force:
        enricher.force_enrichment = True
        logger.info("Force enrichment enabled - will re-enrich existing data")
    
    # Run enrichment
    logger.info(f"Starting Google Maps enrichment...")
    results = await enricher.enrich_all_organizations(org_ids)
    
    # Save results
    enricher.save_enrichment_results(results, args.output)
    
    # Generate and display report
    report = enricher.generate_enrichment_report(results)
    print(report)
    
    # Save report to file
    report_path = args.output.replace('.csv', '_report.txt')
    with open(report_path, 'w') as f:
        f.write(report)
    logger.info(f"Saved enrichment report to {report_path}")

if __name__ == "__main__":
    asyncio.run(main())
