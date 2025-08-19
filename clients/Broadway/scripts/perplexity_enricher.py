#!/usr/bin/env python3
"""
Perplexity enricher for Broadway summer camps project.
Finds leadership contacts and missing business data using targeted searches.
"""

import os
import asyncio
import logging
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import argparse
from db_connection import get_db_connection
import aiohttp
from dotenv import load_dotenv
import re # Added for regex in JSON parsing

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PerplexityEnricher:
    """Enrich summer camp data using Perplexity API for leadership contacts."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_PERPLEXITY_API_KEY') or os.getenv('PERPLEXITY_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_PERPLEXITY_API_KEY or PERPLEXITY_API_KEY")
        
        self.base_url = "https://api.perplexity.ai/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Track API usage and costs
        self.api_calls = 0
        self.total_tokens = 0
    
    def craft_leadership_search_prompt(self, company_name: str, website_url: str) -> str:
        """Craft a specific prompt to find leadership contacts."""
        return f"""Find the current leadership and key staff for {company_name} ({website_url}). 

Focus ONLY on finding:
1. **Camp Director** or **Executive Director** (full name)
2. **Owner** or **Founder** (full name) 
3. **Program Director** or **Summer Camp Director** (full name)
4. **Direct email addresses** for these people (not generic info@ emails)

Search strategy:
- Look for "about us", "our team", "leadership", "staff" pages
- Check recent news articles or press releases
- Look for LinkedIn profiles or professional directories
- Avoid outdated information

Return ONLY a JSON response in this exact format:
{{
    "leadership_contacts": [
        {{
            "name": "Full Name",
            "title": "Job Title", 
            "email": "direct.email@domain.com" or null if not found
        }}
    ],
    "missing_business_data": {{
        "address": "full address if missing" or null,
        "phone": "phone number if missing" or null,
        "default_email": "fallback email if no direct contacts found" or null
    }}
}}

If no leadership found, return empty arrays but still check for missing business data."""
    
    def craft_business_data_prompt(self, company_name: str, website_url: str) -> str:
        """Craft a prompt to find missing business data."""
        return f"""Find the current business information for {company_name} ({website_url}).

Focus ONLY on finding:
1. **Complete business address** (street, city, state, zip)
2. **Main business phone number**
3. **General contact email** (if no direct staff emails found)

Search strategy:
- Check business directories and listings
- Look for recent contact information
- Verify the business is still active and operating

Return ONLY a JSON response in this exact format:
{{
    "business_data": {{
        "address": "full street address",
        "phone": "phone number", 
        "email": "contact email"
    }}
}}"""
    
    def craft_business_categories_prompt(self, company_name: str, website_url: str) -> str:
        """Craft a prompt to classify the business type and categories."""
        return f"""Analyze {company_name} ({website_url}) and classify it into meaningful business categories.

Focus on finding:
1. **Primary Business Type** (e.g., "Summer Camp", "Day Camp", "Overnight Camp", "Sports Camp", "Arts Camp", "Academic Camp", "YMCA", "Community Center")
2. **Camp Specialties** (e.g., "Sports & Athletics", "Arts & Creativity", "Science & Technology", "Outdoor Adventure", "Academic Enrichment", "Special Needs", "Leadership Development")
3. **Age Groups Served** (e.g., "Ages 5-12", "Teens 13-17", "All Ages", "Elementary", "Middle School", "High School")
4. **Seasonal Focus** (e.g., "Summer Only", "Year-Round", "School Breaks", "Weekend Programs")
5. **Program Focus** (e.g., "Day Programs", "Overnight Programs", "After School", "Weekend Camps")

Search strategy:
- Visit the website and analyze program descriptions
- Look for mission statements and about pages
- Check program schedules and offerings
- Identify unique selling points and specialties

Return ONLY a JSON response in this exact format:
{{
    "business_categories": {{
        "primary_type": "main business category",
        "specialties": ["specialty1", "specialty2", "specialty3"],
        "age_groups": "age range served",
        "seasonal_focus": "when programs run",
        "program_focus": "type of programs offered"
    }}
}}

Be specific and avoid generic terms like 'point_of_interest' or 'establishment'."""
    
    async def search_perplexity(self, prompt: str, model: str = "sonar-pro") -> Optional[Dict]:
        """Make a Perplexity API call."""
        try:
            payload = {
                "model": model,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 2000,  # Increased for more complete responses
                "temperature": 0.7  # Increased from 0.1 for more creative, comprehensive responses
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Extract response content
                        content = data['choices'][0]['message']['content']
                        
                        # Try to parse JSON response
                        try:
                            return json.loads(content)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse JSON response: {content[:200]}...")
                            
                            # Try to extract partial data from truncated responses
                            partial_data = {}
                            
                            # Look for leadership contacts - try to extract what we can
                            if '"leadership_contacts"' in content:
                                try:
                                    # Find the start of leadership_contacts array
                                    start = content.find('"leadership_contacts"')
                                    if start != -1:
                                        # Look for the opening bracket
                                        array_start = content.find('[', start)
                                        if array_start != -1:
                                            # Count brackets to find the end of the array
                                            bracket_count = 0
                                            array_end = array_start
                                            for i, char in enumerate(content[array_start:], array_start):
                                                if char == '[':
                                                    bracket_count += 1
                                                elif char == ']':
                                                    bracket_count -= 1
                                                    if bracket_count == 0:
                                                        array_end = i + 1
                                                        break
                                            
                                            if array_end > array_start:
                                                # Extract the array content
                                                array_content = content[array_start:array_end]
                                                logger.info(f"Extracted partial array: {array_content[:100]}...")
                                                
                                                # Try to parse individual contact objects
                                                contacts = []
                                                # Look for individual contact objects within the array
                                                contact_pattern = r'\{[^{}]*"name"[^{}]*\}'
                                                contact_matches = re.findall(contact_pattern, array_content)
                                                
                                                for match in contact_matches:
                                                    try:
                                                        # Clean up the contact object
                                                        clean_match = match.replace('\\', '')
                                                        contact_data = json.loads(clean_match)
                                                        if contact_data.get('name'):
                                                            contacts.append(contact_data)
                                                    except:
                                                        continue
                                                
                                                if contacts:
                                                    partial_data['leadership_contacts'] = contacts
                                                    logger.info(f"Successfully extracted {len(contacts)} contacts from partial response")
                                except Exception as e:
                                    logger.warning(f"Failed to extract partial leadership contacts: {e}")
                            
                            # Look for business data
                            if '"business_data"' in content:
                                try:
                                    start = content.find('"business_data"')
                                    if start != -1:
                                        # Find the business data object
                                        obj_start = content.find('{', start)
                                        if obj_start != -1:
                                            # Count braces to find the end
                                            brace_count = 0
                                            obj_end = obj_start
                                            for i, char in enumerate(content[obj_start:], obj_start):
                                                if char == '{':
                                                    brace_count += 1
                                                elif char == '}':
                                                    brace_count -= 1
                                                    if brace_count == 0:
                                                        obj_end = i + 1
                                                        break
                                            
                                            if obj_end > obj_start:
                                                obj_content = content[obj_start:obj_end]
                                                business_data = json.loads(obj_content)
                                                partial_data['business_data'] = business_data
                                                logger.info(f"Successfully extracted business data from partial response")
                                except Exception as e:
                                    logger.warning(f"Failed to extract partial business data: {e}")
                            
                            return partial_data if partial_data else None
                    else:
                        logger.error(f"Perplexity API error: {response.status} - {await response.text()}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error calling Perplexity API: {e}")
            return None
    
    async def persist_enrichment_to_db(self, enrichment_result: Dict) -> bool:
        """Persist enriched data to the database."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    # Update organization with business data
                    business_data = enrichment_result.get('business_data', {})
                    if business_data:
                        update_fields = []
                        update_values = []
                        
                        if business_data.get('address'):
                            update_fields.append("street = %s")
                            update_values.append(business_data['address'])
                        
                        if business_data.get('phone'):
                            update_fields.append("company_phone = %s")
                            update_values.append(business_data['phone'])
                        
                        if business_data.get('email'):
                            update_fields.append("fallback_email = %s")
                            update_values.append(business_data['email'])
                        
                        # Add business categories if we have them
                        business_categories = enrichment_result.get('business_categories', {})
                        if business_categories:
                            # Format categories as a readable string
                            category_parts = []
                            if business_categories.get('primary_type'):
                                category_parts.append(f"Primary: {business_categories['primary_type']}")
                            if business_categories.get('specialties'):
                                category_parts.append(f"Specialties: {', '.join(business_categories['specialties'])}")
                            if business_categories.get('age_groups'):
                                category_parts.append(f"Ages: {business_categories['age_groups']}")
                            if business_categories.get('seasonal_focus'):
                                category_parts.append(f"Season: {business_categories['seasonal_focus']}")
                            if business_categories.get('program_focus'):
                                category_parts.append(f"Programs: {business_categories['program_focus']}")
                            
                            if category_parts:
                                update_fields.append("perplexity_categories = %s")
                                update_values.append(' | '.join(category_parts))
                        
                        if update_fields:
                            update_values.append(enrichment_result['org_id'])
                            update_query = f"""
                                UPDATE summer_camps.organizations 
                                SET {', '.join(update_fields)}
                                WHERE org_id = %s
                            """
                            cur.execute(update_query, update_values)
                            logger.info(f"Updated organization {enrichment_result['org_id']} with business data and categories")
                    
                    # Add leadership contacts to contacts table
                    leadership_contacts = enrichment_result.get('leadership_contacts', [])
                    for contact in leadership_contacts:
                        if contact.get('name'):
                            # Check if contact already exists
                            cur.execute("""
                                SELECT contact_id FROM summer_camps.contacts 
                                WHERE org_id = %s AND contact_name = %s
                            """, (enrichment_result['org_id'], contact['name']))
                            
                            existing_contact = cur.fetchone()
                            
                            if existing_contact:
                                # Update existing contact
                                cur.execute("""
                                    UPDATE summer_camps.contacts 
                                    SET role_title = %s, contact_email = %s, last_enriched_at = NOW()
                                    WHERE contact_id = %s
                                """, (
                                    contact.get('title', ''),
                                    contact.get('email'),
                                    existing_contact[0]
                                ))
                                logger.info(f"Updated existing contact: {contact['name']}")
                            else:
                                # Insert new contact
                                cur.execute("""
                                    INSERT INTO summer_camps.contacts 
                                    (org_id, contact_name, role_title, contact_email, is_primary_contact, email_quality, notes, created_at, last_enriched_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                                """, (
                                    enrichment_result['org_id'],
                                    contact['name'],
                                    contact.get('title', ''),
                                    contact.get('email'),
                                    False,  # Not primary contact
                                    'direct' if contact.get('email') else 'missing',
                                    f"Leadership contact found via Perplexity enrichment on {enrichment_result['enrichment_timestamp']}"
                                ))
                                logger.info(f"Added new leadership contact: {contact['name']}")
                    
                    conn.commit()
                    logger.info(f"Successfully persisted enrichment data for {enrichment_result['company_name']}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error persisting enrichment data to database: {e}")
            return False
    
    async def enrich_organization(self, org_id: int, company_name: str, website_url: str) -> Dict:
        """Enrich a single organization with leadership contacts and missing data."""
        logger.info(f"Enriching {company_name} (ID: {org_id})")
        
        # First, search for leadership contacts
        leadership_prompt = self.craft_leadership_search_prompt(company_name, website_url)
        leadership_data = await self.search_perplexity(leadership_prompt)
        
        # Then, search for missing business data
        business_prompt = self.craft_business_data_prompt(company_name, website_url)
        business_data = await self.search_perplexity(business_prompt)
        
        # Search for business categories
        categories_prompt = self.craft_business_categories_prompt(company_name, website_url)
        categories_data = await self.search_perplexity(categories_prompt)
        
        # Combine results
        enrichment_result = {
            'org_id': org_id,
            'company_name': company_name,
            'leadership_contacts': leadership_data.get('leadership_contacts', []) if leadership_data else [],
            'business_data': business_data.get('business_data', {}) if business_data else {},
            'business_categories': categories_data.get('business_categories', {}) if categories_data else {},
            'enrichment_timestamp': datetime.now(),
            'api_calls_made': 3  # Leadership + business data + categories searches
        }
        
        # Persist to database
        db_success = await self.persist_enrichment_to_db(enrichment_result)
        if db_success:
            enrichment_result['db_persisted'] = True
        else:
            enrichment_result['db_persisted'] = False
        
        logger.info(f"Found {len(enrichment_result['leadership_contacts'])} leadership contacts for {company_name}")
        return enrichment_result
    
    async def enrich_all_organizations(self, org_ids: Optional[List[int]] = None, max_workers: int = 5) -> List[Dict]:
        """Enrich all organizations or specific ones with concurrent processing."""
        # Get organizations to enrich
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if org_ids:
                    placeholders = ','.join(['%s'] * len(org_ids))
                    cur.execute(f"""
                        SELECT org_id, company_name, website_url 
                        FROM summer_camps.organizations 
                        WHERE org_id IN ({placeholders})
                        ORDER BY org_id
                    """, org_ids)
                else:
                    cur.execute("""
                        SELECT org_id, company_name, website_url 
                        FROM summer_camps.organizations 
                        ORDER BY org_id
                    """)
                
                organizations = cur.fetchall()
        
        logger.info(f"Starting enrichment of {len(organizations)} organizations with {max_workers} workers")
        
        # Create tasks for all organizations
        tasks = []
        for org in organizations:
            task = self.enrich_organization(org[0], org[1], org[2])
            tasks.append(task)
        
        # Execute tasks with concurrency limit
        semaphore = asyncio.Semaphore(max_workers)
        
        async def limited_enrich(task):
            async with semaphore:
                return await task
        
        limited_tasks = [limited_enrich(task) for task in tasks]
        results = await asyncio.gather(*limited_tasks, return_exceptions=True)
        
        # Filter out exceptions and log them
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error processing organization {i+1}: {result}")
            else:
                valid_results.append(result)
                logger.info(f"Completed enrichment for {result['company_name']} (ID: {result['org_id']})")
        
        return valid_results
    
    def save_enrichment_results(self, results: List[Dict], output_file: str):
        """Save enrichment results to CSV."""
        if not results:
            logger.warning("No results to save")
            return
        
        # Flatten results for CSV
        csv_rows = []
        for result in results:
            # Add leadership contacts
            for contact in result['leadership_contacts']:
                csv_rows.append({
                    'org_id': result['org_id'],
                    'company_name': result['company_name'],
                    'contact_name': contact.get('name', ''),
                    'contact_title': contact.get('title', ''),
                    'contact_email': contact.get('email', ''),
                    'data_type': 'leadership_contact',
                    'enrichment_timestamp': result['enrichment_timestamp']
                })
            
            # Add business data
            business_data = result['business_data']
            if business_data:
                csv_rows.append({
                    'org_id': result['org_id'],
                    'company_name': result['company_name'],
                    'address': business_data.get('address', ''),
                    'phone': business_data.get('phone', ''),
                    'email': business_data.get('email', ''),
                    'data_type': 'business_data',
                    'enrichment_timestamp': result['enrichment_timestamp']
                })
            
            # Add business categories
            business_categories = result.get('business_categories', {})
            if business_categories:
                csv_rows.append({
                    'org_id': result['org_id'],
                    'company_name': result['company_name'],
                    'primary_type': business_categories.get('primary_type', ''),
                    'specialties': ', '.join(business_categories.get('specialties', [])),
                    'age_groups': business_categories.get('age_groups', ''),
                    'seasonal_focus': business_categories.get('seasonal_focus', ''),
                    'program_focus': business_categories.get('program_focus', ''),
                    'data_type': 'business_categories',
                    'enrichment_timestamp': result['enrichment_timestamp']
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
        report.append("PERPLEXITY ENRICHMENT REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Organizations Enriched: {len(results)}")
        report.append(f"Total API Calls Made: {self.api_calls}")
        report.append("")
        
        total_contacts = sum(len(r['leadership_contacts']) for r in results)
        total_categories = sum(1 for r in results if r.get('business_categories'))
        report.append(f"Leadership Contacts Found: {total_contacts}")
        report.append(f"Organizations with Business Data: {sum(1 for r in results if r['business_data'])}")
        report.append(f"Organizations with Business Categories: {total_categories}")
        report.append("")
        
        report.append("DETAILED RESULTS:")
        for result in results:
            report.append(f"\n{result['company_name']} (ID: {result['org_id']}):")
            if result['leadership_contacts']:
                for contact in result['leadership_contacts']:
                    report.append(f"  - {contact.get('name', 'N/A')} ({contact.get('title', 'N/A')})")
                    if contact.get('email'):
                        report.append(f"    Email: {contact['email']}")
            else:
                report.append("  - No leadership contacts found")
            
            if result['business_data']:
                report.append("  Business Data:")
                for key, value in result['business_data'].items():
                    if value:
                        report.append(f"    {key}: {value}")
            
            if result.get('business_categories'):
                report.append("  Business Categories:")
                categories = result['business_categories']
                if categories.get('primary_type'):
                    report.append(f"    Primary Type: {categories['primary_type']}")
                if categories.get('specialties'):
                    report.append(f"    Specialties: {', '.join(categories['specialties'])}")
                if categories.get('age_groups'):
                    report.append(f"    Age Groups: {categories['age_groups']}")
                if categories.get('seasonal_focus'):
                    report.append(f"    Seasonal Focus: {categories['seasonal_focus']}")
                if categories.get('program_focus'):
                    report.append(f"    Program Focus: {categories['program_focus']}")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Perplexity Enricher for Broadway Summer Camps')
    parser.add_argument('--org-ids', type=str, help='Comma-separated list of organization IDs to enrich')
    parser.add_argument('--all', action='store_true', help='Enrich all organizations in database')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers (default: 5)')
    parser.add_argument('--output', type=str, default='perplexity_enrichment_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize enricher
    enricher = PerplexityEnricher()
    
    # Determine which organizations to enrich
    org_ids = None
    if args.org_ids:
        org_ids = [int(x.strip()) for x in args.org_ids.split(',')]
    elif not args.all:
        # Default to first 3 for testing
        org_ids = [1, 2, 3]
        logger.info("No specific orgs specified, testing with first 3 organizations")
    
    # Run enrichment
    logger.info(f"Starting Perplexity enrichment...")
    results = await enricher.enrich_all_organizations(org_ids, max_workers=args.workers)
    
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
