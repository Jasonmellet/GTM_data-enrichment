#!/usr/bin/env python3
"""
Apollo API enricher for Broadway summer camps project.
Finds direct email addresses for contacts using Apollo's people search API.
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

class ApolloEnricher:
    """Enrich contact data using Apollo API for direct email addresses."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_APOLLO_API_KEY')
        if not self.api_key:
            raise ValueError("Missing BROADWAY_APOLLO_API_KEY")
        
        self.base_url = "https://api.apollo.io/v1"
        self.headers = {
            "X-Api-Key": self.api_key,
            "Content-Type": "application/json"
        }
        
        # Track API usage and costs
        self.api_calls = 0
        self.emails_found = 0
        
        # Apollo API pricing (approximate)
        self.cost_per_search = 0.10  # $0.10 per people search
        self.cost_per_contact = 0.05  # $0.05 per contact detail
        
        # Smart title filtering for different business types
        self.camp_titles = [
            "camp director", "director", "associate director", "owner", "founder"
        ]
        self.store_titles = [
            "owner", "manager", "store manager", "general manager"
        ]
        self.activity_titles = [
            "activities director", "head coach", "program director", "owner"
        ]
    
    async def test_api_connection(self) -> bool:
        """Test the Apollo API connection."""
        try:
            # Simple test query using people search
            test_params = {
                "page": 1,
                "per_page": 1,
                "q_organization_domains": "google.com"
            }
            
            url = f"{self.base_url}/people/search"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=test_params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        logger.info(f"Response: {data}")
                        
                        if data.get('pagination', {}).get('total') is not None:
                            logger.info(f"✅ Apollo API connection successful!")
                            logger.info(f"   API key: {self.api_key[:10]}...")
                            return True
                        elif data.get('people') is not None:
                            logger.info(f"✅ Apollo API connection successful!")
                            logger.info(f"   API key: {self.api_key[:10]}...")
                            return True
                        else:
                            logger.error(f"❌ Apollo API error: {data.get('error', 'Unknown error')}")
                            return False
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ HTTP error: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"❌ Error testing Apollo API: {e}")
            return False
    
    async def search_contact(self, contact_name: str, company_name: str, company_domain: str = None) -> Optional[Dict]:
        """Search for a contact using Apollo API."""
        try:
            # Build search query
            search_params = {
                "page": 1,
                "per_page": 5,  # Get top 5 results
                "q_organization_name": company_name,
                "q_name": contact_name
            }
            
            # Add domain if available
            if company_domain:
                search_params["q_organization_domains"] = company_domain
            
            url = f"{self.base_url}/people/search"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=search_params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        if data.get('people') and len(data['people']) > 0:
                            # Get the best match (first result)
                            person = data['people'][0]
                            logger.info(f"Found contact: {person.get('name')} at {person.get('organization_name')}")
                            return person
                        else:
                            logger.warning(f"No contact found for: {contact_name} at {company_name}")
                            return None
                    else:
                        logger.error(f"Search failed for {contact_name}: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error searching for contact {contact_name}: {e}")
            return None
    
    async def get_contact_details(self, contact_id: str) -> Optional[Dict]:
        """Get detailed contact information including email."""
        try:
            params = {
                "id": contact_id
            }
            
            url = f"{self.base_url}/people/{contact_id}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        if data.get('person'):
                            person = data['person']
                            logger.info(f"Retrieved details for: {person.get('name')}")
                            return person
                        else:
                            logger.error(f"Details failed for contact {contact_id}")
                            return None
                    else:
                        logger.error(f"Details request failed: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting contact details: {e}")
            return None
    
    async def enrich_person_match(self, contact_name: str, company_name: str, company_domain: Optional[str], website_url: Optional[str], reveal_personal_emails: bool = True, reveal_phone_number: bool = False) -> Optional[Dict]:
        """Use Apollo People Enrichment (match) to retrieve direct email/phone in one call."""
        try:
            first_name = None
            last_name = None
            if contact_name and isinstance(contact_name, str):
                parts = [p for p in contact_name.strip().split() if p]
                if len(parts) >= 2:
                    first_name, last_name = parts[0], parts[-1]
                else:
                    first_name = parts[0]

            payload = {
                "first_name": first_name,
                "last_name": last_name,
                "name": contact_name,
                "organization_name": company_name,
                # Apollo accepts either domain or website
                "organization_domain": company_domain,
                "website_url": website_url,
                # Reveal flags (consumes credits if enabled per plan)
                "reveal_personal_emails": reveal_personal_emails,
                "reveal_phone_number": reveal_phone_number
            }

            url = f"{self.base_url}/people/match"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=payload, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        # Expected shape: { person: { email, ... } } or { matches: [...] }
                        if data.get("person"):
                            return data["person"]
                        if data.get("matches") and len(data["matches"]) > 0:
                            return data["matches"][0]
                        return None
                    else:
                        # 402/403/422 may occur if reveal not available on plan or insufficient info
                        _text = await response.text()
                        logger.warning(f"People match failed: {response.status} - {_text}")
                        return None
        except Exception as e:
            logger.error(f"Error calling people match: {e}")
            return None

    async def enrich_contact(self, contact_id: int, contact_name: str, org_id: int, company_name: str, website_url: str = None) -> Dict:
        """Enrich a single contact with Apollo data."""
        logger.info(f"Enriching contact {contact_name} (ID: {contact_id}) at {company_name}")
        
        # Extract domain from website URL if available
        company_domain = None
        if website_url:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(website_url)
                company_domain = parsed.netloc
            except:
                pass

        # 1) Try People Enrichment (match) first to directly reveal email/phone per docs
        person = await self.enrich_person_match(
            contact_name=contact_name,
            company_name=company_name,
            company_domain=company_domain,
            website_url=website_url,
            reveal_personal_emails=True,
            reveal_phone_number=False,
        )

        # 2) Fallback to search if match did not return usable email
        if not person:
            search_result = await self.search_contact(contact_name, company_name, company_domain)
            person = search_result
            if person:
                apollo_contact_id = person.get('id')
                if apollo_contact_id:
                    contact_details = await self.get_contact_details(apollo_contact_id)
                    if contact_details:
                        person = contact_details

        # Prepare output
        enrichment_data = {
            'contact_id': contact_id,
            'contact_name': contact_name,
            'org_id': org_id,
            'company_name': company_name,
            'apollo_contact_id': person.get('id') if person else None,
            'email': (person or {}).get('email'),
            'phone': (person or {}).get('phone'),
            'title': (person or {}).get('title'),
            'linkedin_url': (person or {}).get('linkedin_url'),
            'twitter_url': (person or {}).get('twitter_url'),
            'seniority': (person or {}).get('seniority'),
            'departments': (person or {}).get('departments', []),
            'subdepartments': (person or {}).get('subdepartments', []),
            'enrichment_timestamp': datetime.now()
        }

        # Determine email quality
        found_email = enrichment_data['email'] and enrichment_data['email'] != 'email_not_unlocked@domain.com'
        if found_email:
            self.emails_found += 1
            enrichment_data['enriched'] = True
            enrichment_data['reason'] = 'Email found'
            logger.info(f"Found email: {enrichment_data['email']} for {contact_name}")
        else:
            enrichment_data['enriched'] = False
            enrichment_data['reason'] = 'No unlocked email (or not found)'
            if enrichment_data['email'] == 'email_not_unlocked@domain.com':
                logger.warning("Email locked by Apollo – requires reveal credits or plan enabling.")

        # Persist to database
        if await self.persist_enrichment_to_db(enrichment_data):
            enrichment_data['db_persisted'] = True
        else:
            enrichment_data['db_persisted'] = False

        return enrichment_data
    
    async def smart_company_search(self, company_name: str, website_url: str = None, business_type: str = "camp") -> List[Dict]:
        """
        Smart company search - ONE API call that gets quality contacts based on business type.
        Returns filtered contacts with the best titles for the business type.
        """
        try:
            # Determine which titles to filter for based on business type
            if business_type == "camp":
                target_titles = self.camp_titles
            elif business_type == "store":
                target_titles = self.store_titles
            elif business_type == "activity":
                target_titles = self.activity_titles
            else:
                target_titles = self.camp_titles  # Default to camp titles
            
            logger.info(f"🔍 Smart search for {company_name} (business type: {business_type})")
            logger.info(f"   Target titles: {', '.join(target_titles)}")
            
            # Extract domain from website if available
            company_domain = None
            if website_url:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(website_url)
                    company_domain = parsed.netloc.lower()
                    if company_domain.startswith('www.'):
                        company_domain = company_domain[4:]
                except:
                    pass
            
            # Build search parameters - ONE API call for multiple roles
            # Using the correct endpoint and parameters from Apollo docs
            search_params = {
                "page": 1,
                "per_page": 50,  # Get more contacts, we'll filter for quality
                "q_organization_domains_list": [company_domain] if company_domain else []
            }
            
            # Add organization name as fallback if no domain
            if not company_domain:
                search_params["q_organization_name"] = company_name
            
            url = f"{self.base_url}/mixed_people/search"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=search_params, timeout=30) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Handle both 'people' and 'contacts' arrays (Apollo docs show both)
                        all_contacts = []
                        if data.get('people') and len(data['people']) > 0:
                            all_contacts.extend(data['people'])
                            logger.info(f"Found {len(data['people'])} people for {company_name}")
                        
                        if data.get('contacts') and len(data['contacts']) > 0:
                            all_contacts.extend(data['contacts'])
                            logger.info(f"Found {len(data['contacts'])} contacts for {company_name}")
                        
                        if all_contacts:
                            logger.info(f"Total contacts found: {len(all_contacts)}")
                            
                            # Log first few contacts for debugging
                            for i, contact in enumerate(all_contacts[:3]):
                                logger.info(f"   Sample contact {i+1}: {contact.get('name', 'N/A')} - {contact.get('title', 'N/A')} (Seniority: {contact.get('seniority', 'N/A')}, Email: {contact.get('email_status', 'N/A')})")
                            
                            # Filter for quality contacts using Apollo's built-in indicators
                            quality_contacts = []
                            for contact in all_contacts:
                                title = contact.get('title', '')
                                if not title:  # Skip contacts without titles
                                    continue
                                
                                title_lower = title.lower()
                                
                                # Check if title matches any of our target titles
                                title_match = any(target in title_lower for target in target_titles)
                                
                                # Check Apollo's quality indicators
                                seniority = contact.get('seniority', '')
                                email_status = contact.get('email_status', '')
                                
                                # Quality scoring based on multiple factors
                                quality_score = self._calculate_quality_score(contact, target_titles)
                                
                                # Accept contacts with either good title match OR high Apollo quality indicators
                                is_quality = (
                                    title_match or  # Good title match
                                    seniority in ['vp', 'director', 'head', 'chief', 'president', 'ceo', 'c-level'] or  # High seniority
                                    email_status == 'verified' or  # Verified email
                                    quality_score >= 15  # High quality score
                                )
                                
                                if is_quality:
                                    quality_contacts.append({
                                        'apollo_id': contact.get('id'),
                                        'name': contact.get('name', ''),
                                        'title': contact.get('title', ''),
                                        'email': contact.get('email', ''),
                                        'email_status': contact.get('email_status', ''),
                                        'linkedin_url': contact.get('linkedin_url', ''),
                                        'seniority': contact.get('seniority', ''),
                                        'departments': contact.get('departments', []),
                                        'functions': contact.get('functions', []),
                                        'organization_name': contact.get('organization', {}).get('name', company_name),
                                        'website_url': contact.get('organization', {}).get('website_url', website_url),
                                        'quality_score': quality_score,
                                        'apollo_quality_flags': {
                                            'seniority': seniority,
                                            'email_status': email_status,
                                            'title_match': title_match
                                        },
                                        # Enhanced data from Apollo docs
                                        'headline': contact.get('headline', ''),
                                        'city': contact.get('city', ''),
                                        'state': contact.get('state', ''),
                                        'country': contact.get('country', ''),
                                        'phone_numbers': contact.get('phone_numbers', []),
                                        'employment_history': contact.get('employment_history', []),
                                        # Additional social media URLs from Apollo
                                        'twitter_url': contact.get('twitter_url', ''),
                                        'github_url': contact.get('github_url', ''),
                                        'facebook_url': contact.get('facebook_url', ''),
                                        'photo_url': contact.get('photo_url', ''),
                                        'formatted_address': contact.get('formatted_address', '')
                                    })
                            
                            # Sort by quality score (highest first)
                            quality_contacts.sort(key=lambda x: x['quality_score'], reverse=True)
                            
                            # Limit to top 3 quality contacts per org
                            top_contacts = quality_contacts[:3]
                            
                            logger.info(f"✅ Found {len(top_contacts)} quality contacts for {company_name}")
                            logger.info(f"   Quality range: {top_contacts[0]['quality_score'] if top_contacts else 0} - {top_contacts[-1]['quality_score'] if top_contacts else 0}")
                            
                            # Unlock emails for quality contacts
                            unlocked_contacts = await self.unlock_emails_for_contacts(top_contacts)
                            return unlocked_contacts
                        else:
                            # Debug: Log what we actually got from Apollo
                            logger.info(f"No people found for {company_name}")
                            logger.info(f"Response data keys: {list(data.keys())}")
                            if 'people' in data:
                                logger.info(f"People array length: {len(data['people']) if data['people'] else 0}")
                            if 'contacts' in data:
                                logger.info(f"Contacts array length: {len(data['contacts']) if data['contacts'] else 0}")
                            return []
                            
                    else:
                        logger.warning(f"Search failed for {company_name}: {response.status}")
                        return []
                        
        except Exception as e:
            logger.error(f"Error in smart company search for {company_name}: {e}")
            return []
    
    def _calculate_quality_score(self, contact: Dict, target_titles: List[str]) -> int:
        """Calculate quality score for a contact based on title relevance and Apollo's quality indicators."""
        title = contact.get('title', '').lower()
        score = 0
        
        # Title relevance scoring
        for i, target in enumerate(target_titles):
            if target in title:
                # Higher score for more specific titles (earlier in list = better)
                score += (len(target_titles) - i) * 10
                break
        
        # Seniority bonus (leveraging Apollo's built-in indicators)
        seniority = contact.get('seniority', '').lower()
        if seniority in ['ceo', 'c-level', 'president', 'founder']:
            score += 25
        elif seniority in ['vp', 'director', 'head']:
            score += 20
        elif seniority in ['manager', 'lead']:
            score += 15
        elif seniority in ['senior', 'principal']:
            score += 10
        
        # Email status bonus (Apollo's email quality indicators)
        email_status = contact.get('email_status', '').lower()
        if email_status == 'verified':
            score += 15
        elif email_status == 'unverified':
            score += 5
        
        # Department relevance bonus (Apollo's department data)
        departments = contact.get('departments', [])
        if departments:
            # Bonus for leadership/management departments
            leadership_depts = ['executive', 'management', 'operations', 'general_management']
            if any(dept.lower() in leadership_depts for dept in departments):
                score += 10
        
        # Function relevance bonus (Apollo's function data)
        functions = contact.get('functions', [])
        if functions:
            # Bonus for leadership functions
            leadership_functions = ['executive', 'management', 'operations']
            if any(func.lower() in leadership_functions for func in functions):
                score += 8
        
        return score
    
    async def get_organizations_needing_contacts(self, limit: int = 50) -> List[Tuple[int, str, str]]:
        """Get organizations that need contacts, prioritized by those without any."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # First get organizations with no contacts, then those with few contacts
                cur.execute("""
                    SELECT o.org_id, o.company_name, o.website_url
                    FROM summer_camps.organizations o
                    LEFT JOIN summer_camps.contacts c ON o.org_id = c.org_id
                    WHERE c.contact_id IS NULL
                    ORDER BY o.org_id
                    LIMIT %s
                """, (limit,))
                
                orgs_without_contacts = cur.fetchall()
                
                # If we need more, get organizations with ≤2 contacts
                remaining = limit - len(orgs_without_contacts)
                if remaining > 0:
                    cur.execute("""
                        SELECT o.org_id, o.company_name, o.website_url
                        FROM summer_camps.organizations o
                        JOIN (
                            SELECT org_id, COUNT(contact_id) as contact_count
                            FROM summer_camps.contacts
                            GROUP BY org_id
                            HAVING COUNT(contact_id) <= 2
                        ) contact_counts ON o.org_id = contact_counts.org_id
                        ORDER BY o.org_id
                        LIMIT %s
                    """, (remaining,))
                    
                    orgs_with_few_contacts = cur.fetchall()
                    return orgs_without_contacts + orgs_with_few_contacts
                
                return orgs_without_contacts
    
    async def batch_smart_search(self, max_workers: int = 5, batch_size: int = 50) -> Dict:
        """Process multiple organizations with smart search using concurrent workers."""
        # Get organizations to process
        organizations = await self.get_organizations_needing_contacts(batch_size)
        
        if not organizations:
            return {
                'total_organizations': 0,
                'processed': 0,
                'total_contacts_found': 0,
                'total_contacts_saved': 0,
                'results': []
            }
        
        logger.info(f"🚀 Starting batch smart search for {len(organizations)} organizations with {max_workers} workers")
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_workers)
        
        async def process_organization(org_data):
            async with semaphore:
                org_id, company_name, website_url = org_data
                
                logger.info(f"🔍 Processing: {company_name}")
                
                # Determine business type (default to camp for summer camps)
                business_type = "camp"  # We can enhance this later
                
                # Run smart search
                contacts = await self.smart_company_search(company_name, website_url, business_type)
                
                # Save to database
                saved_count = await self.save_quality_contacts_to_db(org_id, contacts)
                
                # Small delay to respect rate limits
                await asyncio.sleep(1)
                
                return {
                    'org_id': org_id,
                    'company_name': company_name,
                    'contacts_found': len(contacts),
                    'contacts_saved': saved_count,
                    'business_type': business_type
                }
        
        # Process organizations concurrently
        tasks = [process_organization(org) for org in organizations]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and log them
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Organization processing error: {result}")
            else:
                valid_results.append(result)
                logger.info(f"✅ Completed: {result['company_name']} - {result['contacts_found']} contacts, {result['contacts_saved']} saved")
        
        return {
            'total_organizations': len(organizations),
            'processed': len(valid_results),
            'total_contacts_found': sum(r['contacts_found'] for r in valid_results),
            'total_contacts_saved': sum(r['contacts_saved'] for r in valid_results),
            'results': valid_results
        }
    
    async def save_quality_contacts_to_db(self, org_id: int, contacts: List[Dict]) -> int:
        """Save quality contacts discovered via smart company search to database."""
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
                        
                        # Extract phone number from phone_numbers array
                        phone = None
                        if contact.get('phone_numbers'):
                            # Get the first phone number
                            phone = contact['phone_numbers'][0].get('raw_number') or contact['phone_numbers'][0].get('sanitized_number')
                        
                        # Insert new quality contact with ALL enhanced Apollo data
                        cur.execute("""
                            INSERT INTO summer_camps.contacts 
                            (org_id, contact_name, contact_email, role_title, email_quality, 
                             last_enriched_at, notes, created_at, apollo_contact_id,
                             linkedin_url, twitter_url, github_url, facebook_url, photo_url,
                             headline, personal_city, personal_state, personal_country,
                             formatted_address, employment_history, contact_phone,
                             seniority, departments, functions, email_validation_score,
                             is_primary_contact)
                            VALUES (%s, %s, %s, %s, %s, NOW(), %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            org_id,
                            contact['name'],
                            contact['email'] if contact['email'] != 'email_not_unlocked@domain.com' else None,
                            contact['title'],
                            'direct' if contact['email'] and contact['email'] != 'email_not_unlocked@domain.com' else 'unknown',
                            f"Discovered via Apollo smart search | Quality Score: {contact['quality_score']} | Seniority: {contact['seniority']} | Departments: {', '.join(contact['departments'])}",
                            contact.get('apollo_id'),  # apollo_contact_id
                            contact.get('linkedin_url'),  # linkedin_url
                            contact.get('twitter_url'),  # twitter_url  
                            contact.get('github_url'),  # github_url
                            contact.get('facebook_url'),  # facebook_url
                            contact.get('photo_url'),  # photo_url
                            contact.get('headline'),  # headline
                            contact.get('city'),  # personal_city
                            contact.get('state'),  # personal_state
                            contact.get('country'),  # personal_country
                            contact.get('formatted_address'),  # formatted_address
                            json.dumps(contact.get('employment_history', [])) if contact.get('employment_history') else None,  # employment_history (JSON)
                            phone,  # contact_phone
                            contact.get('seniority'),  # seniority
                            json.dumps(contact.get('departments', [])) if contact.get('departments') else None,  # departments (JSON)
                            json.dumps(contact.get('functions', [])) if contact.get('functions') else None,  # functions (JSON)
                            contact['quality_score'],  # email_validation_score (repurposing for quality score)
                            contact['quality_score'] >= 50  # is_primary_contact (high quality contacts)
                        ))
                        
                        saved_count += 1
                    
                    conn.commit()
                    logger.info(f"Saved {saved_count} new quality contacts for organization {org_id}")
                    return saved_count
                    
        except Exception as e:
            logger.error(f"Error saving quality contacts for org {org_id}: {e}")
            return 0
    
    async def enrich_all_contacts(self, contact_ids: Optional[List[int]] = None) -> List[Dict]:
        """Enrich all contacts or specific ones."""
        # Get contacts to enrich
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if contact_ids:
                    placeholders = ','.join(['%s'] * len(contact_ids))
                    cur.execute(f"""
                        SELECT c.contact_id, c.contact_name, c.org_id, o.company_name, o.website_url
                        FROM summer_camps.contacts c
                        JOIN summer_camps.organizations o ON c.org_id = o.org_id
                        WHERE c.contact_id IN ({placeholders})
                        ORDER BY c.contact_id
                    """, contact_ids)
                else:
                    cur.execute("""
                        SELECT c.contact_id, c.contact_name, c.org_id, o.company_name, o.website_url
                        FROM summer_camps.contacts c
                        JOIN summer_camps.organizations o ON c.org_id = o.org_id
                        WHERE c.contact_name IS NOT NULL AND c.contact_name != ''
                        ORDER BY c.contact_id
                    """)
                
                contacts = cur.fetchall()
        
        logger.info(f"Starting Apollo enrichment of {len(contacts)} contacts")
        
        # Process contacts sequentially to avoid rate limiting
        results = []
        for contact in contacts:
            result = await self.enrich_contact(contact[0], contact[1], contact[2], contact[3], contact[4])
            results.append(result)
            
            # Small delay between requests
            await asyncio.sleep(1)
        
        return results
    
    async def persist_enrichment_to_db(self, enrichment_data: Dict) -> bool:
        """Persist enriched contact data to the database."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    
                    # Update contact with Apollo data
                    update_fields = []
                    update_values = []
                    
                    if enrichment_data.get('email'):
                        update_fields.append("contact_email = %s")
                        update_values.append(enrichment_data['email'])
                        update_fields.append("email_quality = %s")
                        update_values.append('direct')
                    
                    if enrichment_data.get('phone'):
                        update_fields.append("contact_phone = %s")
                        update_values.append(enrichment_data['phone'])
                    
                    if enrichment_data.get('title'):
                        update_fields.append("role_title = %s")
                        update_values.append(enrichment_data['title'])
                    
                    # Add Apollo metadata
                    update_fields.append("last_enriched_at = %s")
                    update_values.append(datetime.now())
                    
                    if update_fields:
                        # Add contact_id to the end for the WHERE clause
                        update_values.append(enrichment_data['contact_id'])
                        
                        # Build and execute the update query
                        update_query = f"""
                            UPDATE summer_camps.contacts 
                            SET {', '.join(update_fields)}
                            WHERE contact_id = %s
                        """
                        
                        cur.execute(update_query, update_values)
                        conn.commit()
                        
                        logger.info(f"Updated contact {enrichment_data['contact_id']} with Apollo data")
                        return True
                    
                    return True  # No fields to update, but that's okay
                    
        except Exception as e:
            logger.error(f"Error persisting enrichment data to database: {e}")
            return False
    
    async def enrich_bulk_contacts(self, contacts: List[Tuple[int, str, int, str, Optional[str]]], batch_size: int = 10,
                                   reveal_personal_emails: bool = True, reveal_phone_number: bool = False) -> List[Dict]:
        """Bulk-enrich contacts using Apollo Bulk People Enrichment (up to 10 per request).
        contacts: list of tuples (contact_id, contact_name, org_id, company_name, website_url)
        Returns list of per-contact enrichment dicts.
        """
        results: List[Dict] = []
        if not contacts:
            return results

        # Helper to extract domain
        def get_domain(url: Optional[str]) -> Optional[str]:
            if not url:
                return None
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                return parsed.netloc or None
            except:
                return None

        # Chunk contacts and call bulk endpoint
        for i in range(0, len(contacts), batch_size):
            chunk = contacts[i:i + batch_size]
            details = []
            id_index_map: List[int] = []  # parallel map to contact_id
            for contact_id, contact_name, org_id, company_name, website_url in chunk:
                first_name = None
                last_name = None
                if contact_name and isinstance(contact_name, str):
                    parts = [p for p in contact_name.strip().split() if p]
                    if len(parts) >= 2:
                        first_name, last_name = parts[0], parts[-1]
                    elif parts:
                        first_name = parts[0]
                details.append({
                    "first_name": first_name,
                    "last_name": last_name,
                    "name": contact_name,
                    "organization_name": company_name,
                    "organization_domain": get_domain(website_url),
                    "website_url": website_url,
                })
                id_index_map.append(contact_id)

            payload = {
                "details": details,
                "reveal_personal_emails": reveal_personal_emails,
                "reveal_phone_number": reveal_phone_number,
            }

            url = f"{self.base_url}/people/bulk_match"

            # Simple retry/backoff for 429
            attempt = 0
            while True:
                attempt += 1
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=self.headers, json=payload, timeout=60) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.api_calls += 1
                            matches = data.get("matches") or data.get("people") or []
                            # Align results by index if possible
                            for idx, contact_id in enumerate(id_index_map):
                                person = matches[idx] if idx < len(matches) else None
                                result = {
                                    'contact_id': contact_id,
                                    'email': (person or {}).get('email'),
                                    'phone': (person or {}).get('phone'),
                                    'title': (person or {}).get('title'),
                                    'linkedin_url': (person or {}).get('linkedin_url'),
                                    'seniority': (person or {}).get('seniority'),
                                    'departments': (person or {}).get('departments', []),
                                    'subdepartments': (person or {}).get('subdepartments', []),
                                    'enrichment_timestamp': datetime.now()
                                }
                                # Persist if found usable email
                                found_email = result['email'] and result['email'] != 'email_not_unlocked@domain.com'
                                if found_email:
                                    self.emails_found += 1
                                # Update DB
                                await self.persist_enrichment_to_db({
                                    'contact_id': result['contact_id'],
                                    'email': result['email'],
                                    'phone': result['phone'],
                                    'title': result['title'],
                                })
                                results.append(result)
                            break
                        elif response.status in (429, 503):
                            wait_s = min(30, 2 ** attempt)
                            logger.warning(f"Bulk match rate limited or unavailable ({response.status}), retrying in {wait_s}s...")
                            await asyncio.sleep(wait_s)
                            continue
                        else:
                            text = await response.text()
                            logger.warning(f"Bulk people match failed: {response.status} - {text}")
                            # Still append empty results for alignment
                            for contact_id in id_index_map:
                                await self.persist_enrichment_to_db({
                                    'contact_id': contact_id,
                                })
                                results.append({'contact_id': contact_id})
                            break
        return results

    async def get_contacts_missing_direct_email(self, limit: Optional[int] = None) -> List[Tuple[int, str, int, str, Optional[str]]]:
        """Fetch contacts with names but missing direct emails."""
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                sql = (
                    """
                    SELECT c.contact_id, c.contact_name, c.org_id, o.company_name, o.website_url
                    FROM summer_camps.contacts c
                    JOIN summer_camps.organizations o ON c.org_id = o.org_id
                    WHERE c.contact_name IS NOT NULL AND c.contact_name != ''
                      AND (c.contact_email IS NULL OR c.contact_email = '' OR c.email_quality IS NULL OR c.email_quality != 'direct')
                    ORDER BY c.contact_id
                    """
                )
                if limit and isinstance(limit, int):
                    sql += f" LIMIT {int(limit)}"
                cur.execute(sql)
                rows = cur.fetchall()
                return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]

    async def enrich_contact_email(self, apollo_contact_id: str) -> Optional[str]:
        """Enrich a contact using Apollo's enrichment endpoint to get their email address."""
        try:
            # Use Apollo's enrichment endpoint to get full contact data including email
            url = f"{self.base_url}/people/{apollo_contact_id}/enrich"
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Extract the enriched email
                        email = data.get('email')
                        if email and email != 'email_not_unlocked@domain.com':
                            logger.info(f"✅ Enriched email for contact {apollo_contact_id}: {email}")
                            return email
                        else:
                            logger.warning(f"❌ Email enrichment failed for contact {apollo_contact_id}")
                            return None
                    else:
                        logger.warning(f"Contact enrichment failed for contact {apollo_contact_id}: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error enriching contact {apollo_contact_id}: {e}")
            return None

    async def enrich_emails_for_contacts(self, contacts: List[Dict]) -> List[Dict]:
        """Enrich contacts using Apollo's enrichment endpoint to get their email addresses."""
        logger.info(f"🔓 Starting contact enrichment for {len(contacts)} contacts...")
        
        enriched_contacts = []
        for contact in contacts:
            apollo_id = contact.get('apollo_id')
            if not apollo_id:
                continue
                
            # Only enrich contacts without real emails
            if not contact.get('email') or contact['email'] == 'email_not_unlocked@domain.com':
                enriched_email = await self.enrich_contact_email(apollo_id)
                if enriched_email:
                    contact['email'] = enriched_email
                    contact['email_status'] = 'verified'  # Update status
                    logger.info(f"✅ Enriched email for {contact['name']}: {enriched_email}")
                else:
                    logger.info(f"❌ Could not enrich email for {contact['name']}")
                
                # Small delay to respect rate limits
                await asyncio.sleep(0.5)
            
            enriched_contacts.append(contact)
        
        logger.info(f"🔓 Contact enrichment completed for {len(enriched_contacts)} contacts")
        return enriched_contacts

    def save_enrichment_results(self, results: List[Dict], output_file: str):
        """Save enrichment results to CSV."""
        if not results:
            logger.warning("No results to save")
            return
        
        # Flatten results for CSV
        csv_rows = []
        for result in results:
            csv_rows.append({
                'contact_id': result['contact_id'],
                'contact_name': result['contact_name'],
                'org_id': result['org_id'],
                'company_name': result['company_name'],
                'email': result.get('email', ''),
                'phone': result.get('phone', ''),
                'title': result.get('title', ''),
                'linkedin_url': result.get('linkedin_url', ''),
                'seniority': result.get('seniority', ''),
                'departments': ', '.join(result.get('departments', [])),
                'enriched': result.get('enriched', False),
                'reason': result.get('reason', ''),
                'enrichment_timestamp': result.get('enrichment_timestamp', '')
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
        report.append("APOLLO ENRICHMENT REPORT")
        report.append("=" * 60)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Contacts Processed: {len(results)}")
        report.append(f"Total API Calls Made: {self.api_calls}")
        report.append(f"Emails Found: {self.emails_found}")
        
        # Calculate costs
        total_cost = (self.api_calls * self.cost_per_search) + (self.emails_found * self.cost_per_contact)
        report.append(f"Estimated API Cost: ${total_cost:.2f}")
        report.append("")
        
        enriched_count = sum(1 for r in results if r.get('enriched'))
        report.append(f"Contacts Enriched: {enriched_count}")
        report.append(f"Contacts Not Enriched: {len(results) - enriched_count}")
        report.append("")
        
        if enriched_count > 0:
            report.append("ENRICHMENT RESULTS:")
            for result in results:
                if result.get('enriched'):
                    report.append(f"\n{result.get('contact_name', 'Unknown')} at {result.get('company_name', 'Unknown')}:")
                    if result.get('email'):
                        report.append(f"  Email: {result['email']}")
                    if result.get('phone'):
                        report.append(f"  Phone: {result['phone']}")
                    if result.get('title'):
                        report.append(f"  Title: {result['title']}")
        
        return "\n".join(report)

    async def test_people_enrichment(self, contact_name: str, company_domain: str = None) -> Optional[Dict]:
        """Test Apollo's People Enrichment endpoint to get real email addresses."""
        try:
            # Use Apollo's People Enrichment endpoint
            url = f"{self.base_url}/people/match"
            
            # Build query parameters
            params = {
                'name': contact_name,
                'reveal_personal_emails': 'true',  # This should give us the real email!
                'reveal_phone_number': 'false'
            }
            
            # Add domain if available
            if company_domain:
                params['domain'] = company_domain
            
            logger.info(f"🔍 Testing People Enrichment for: {contact_name}")
            logger.info(f"   URL: {url}")
            logger.info(f"   Params: {params}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, headers=self.headers) as response:
                    logger.info(f"   Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Check if we got a person with a real email
                        if data.get('person') and data['person'].get('email'):
                            email = data['person']['email']
                            if email and email != 'email_not_unlocked@domain.com':
                                logger.info(f"✅ SUCCESS! Got real email: {email}")
                                return data['person']
                            else:
                                logger.warning(f"❌ Still got placeholder email: {email}")
                        else:
                            logger.warning(f"❌ No person data or email in response")
                        
                        # Log the full response for debugging
                        logger.info(f"   Full Response: {json.dumps(data, indent=2)}")
                        
                    else:
                        response_text = await response.text()
                        logger.error(f"   Error Response: {response_text}")
                        
                    return None
                        
        except Exception as e:
            logger.error(f"Error testing people enrichment for {contact_name}: {e}")
            return None

    async def test_find_people_with_filters(self, company_name: str, contact_name: str = None) -> Optional[Dict]:
        """Test Apollo's Find People Using Filters endpoint for better email discovery."""
        try:
            # Use Apollo's Find People endpoint with company filters
            url = f"{self.base_url}/people/search"
            
            # Build search parameters with company context
            search_params = {
                "page": 1,
                "per_page": 10,
                "q_organization_name": company_name
            }
            
            # Add contact name if provided
            if contact_name:
                search_params["q_name"] = contact_name
            
            logger.info(f"🔍 Testing Find People with Filters for: {company_name}")
            if contact_name:
                logger.info(f"   Contact: {contact_name}")
            logger.info(f"   URL: {url}")
            logger.info(f"   Params: {search_params}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=search_params, headers=self.headers) as response:
                    logger.info(f"   Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Check if we got people with emails
                        if data.get('people') and len(data['people']) > 0:
                            people = data['people']
                            logger.info(f"✅ Found {len(people)} people for {company_name}")
                            
                            # Look for people with real emails
                            for person in people[:3]:  # Check first 3
                                name = person.get('name', 'N/A')
                                email = person.get('email', 'N/A')
                                title = person.get('title', 'N/A')
                                
                                logger.info(f"   👤 {name} - {title}")
                                logger.info(f"      Email: {email}")
                                
                                # Check if this is a real email
                                if email and email != 'email_not_unlocked@domain.com':
                                    logger.info(f"      ✅ REAL EMAIL FOUND!")
                                    return person
                            
                            # Log the full response for debugging
                            logger.info(f"   Full Response Sample: {json.dumps(data['people'][0] if data['people'] else {}, indent=2)}")
                            
                        else:
                            logger.warning(f"❌ No people found for {company_name}")
                        
                    else:
                        response_text = await response.text()
                        logger.error(f"   Error Response: {response_text}")
                        
                    return None
                        
        except Exception as e:
            logger.error(f"Error testing find people with filters for {company_name}: {e}")
            return None

    async def test_find_people_on_sample_companies(self):
        """Test the Find People with Filters endpoint on sample companies from our database."""
        logger.info("🧪 Testing Apollo Find People with Filters endpoint...")
        
        # Get a few sample companies to test with
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT o.company_name, o.website_url
                    FROM summer_camps.organizations o
                    JOIN summer_camps.contacts c ON o.org_id = c.org_id
                    WHERE c.apollo_contact_id IS NOT NULL 
                    AND (c.contact_email IS NULL OR c.contact_email = 'email_not_unlocked@domain.com')
                    LIMIT 3
                """)
                sample_companies = cur.fetchall()
        
        if not sample_companies:
            logger.info("❌ No sample companies found to test")
            return
        
        logger.info(f"🧪 Testing find people on {len(sample_companies)} sample companies...")
        
        for company in sample_companies:
            company_name, website_url = company
            
            logger.info(f"\n🔍 Testing company: {company_name}")
            logger.info(f"   Website: {website_url or 'None'}")
            
            # Test find people with company context
            found_person = await self.test_find_people_with_filters(company_name)
            
            if found_person:
                logger.info(f"✅ Found person with real email for {company_name}")
                logger.info(f"   Name: {found_person.get('name', 'N/A')}")
                logger.info(f"   Email: {found_person.get('email', 'N/A')}")
                logger.info(f"   Title: {found_person.get('title', 'N/A')}")
            else:
                logger.info(f"❌ No real emails found for {company_name}")
            
            # Small delay between tests
            await asyncio.sleep(1)
        
        logger.info("🧪 Find People with Filters testing completed!")

    async def test_contact_enrichment(self, apollo_person_id: str, contact_name: str, company_name: str = None, company_domain: str = None) -> Optional[Dict]:
        """Test Apollo's People/Match Enrichment API to unlock email addresses."""
        try:
            # Use Apollo's People/Match endpoint for enrichment (correct approach)
            url = f"{self.base_url}/people/match"
            
            # Build enrichment parameters with company context
            params = {
                'name': contact_name,
                'reveal_personal_emails': 'true',  # This should unlock the email!
                'reveal_phone_number': 'false'
            }
            
            # Add company context for better matching
            if company_domain:
                params['domain'] = company_domain
            elif company_name:
                # Try using organization name if no domain
                params['organization_name'] = company_name
            
            logger.info(f"🔓 Testing People/Match Enrichment for: {contact_name}")
            logger.info(f"   Company: {company_name or 'None'}")
            logger.info(f"   Domain: {company_domain or 'None'}")
            logger.info(f"   URL: {url}")
            logger.info(f"   Params: {params}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, headers=self.headers) as response:
                    logger.info(f"   Response Status: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        self.api_calls += 1
                        
                        # Check if we got enriched data with email
                        if data.get('person') and data['person'].get('email'):
                            email = data['person']['email']
                            if email and email != 'email_not_unlocked@domain.com':
                                logger.info(f"✅ SUCCESS! People/Match enrichment unlocked email: {email}")
                                return data['person']
                            else:
                                logger.warning(f"❌ Still got placeholder email: {email}")
                        else:
                            logger.warning(f"❌ No email found in People/Match enrichment response")
                        
                        # Log the full response for debugging
                        logger.info(f"   Full People/Match Response: {json.dumps(data, indent=2)}")
                        
                    else:
                        response_text = await response.text()
                        logger.error(f"   Error Response: {response_text}")
                        
                    return None
                        
        except Exception as e:
            logger.error(f"Error testing People/Match enrichment for {contact_name}: {e}")
            return None

    async def test_enrichment_workflow(self):
        """Test the complete workflow: Find contact -> Enrich contact -> Get email."""
        logger.info("🧪 Testing Complete Apollo Enrichment Workflow...")
        
        # Get a sample contact that we know exists in Apollo
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.contact_name, c.apollo_contact_id, c.contact_email, o.company_name, o.website_url
                    FROM summer_camps.contacts c
                    JOIN summer_camps.organizations o ON c.org_id = o.org_id
                    WHERE c.apollo_contact_id IS NOT NULL 
                    AND (c.contact_email IS NULL OR c.contact_email = 'email_not_unlocked@domain.com')
                    LIMIT 2
                """)
                sample_contacts = cur.fetchall()
        
        if not sample_contacts:
            logger.info("❌ No sample contacts found to test")
            return
        
        logger.info(f"🧪 Testing enrichment workflow on {len(sample_contacts)} contacts...")
        
        for contact in sample_contacts:
            contact_name, apollo_id, current_email, company_name, website_url = contact
            
            # Extract domain from website URL
            company_domain = None
            if website_url:
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(website_url)
                    company_domain = parsed.netloc
                    if company_domain.startswith('www.'):
                        company_domain = company_domain[4:]
                except:
                    pass
            
            logger.info(f"\n🔍 WORKFLOW TEST: {contact_name}")
            logger.info(f"   Current email: {current_email or 'None'}")
            logger.info(f"   Apollo ID: {apollo_id}")
            logger.info(f"   Company: {company_name}")
            logger.info(f"   Website: {website_url}")
            logger.info(f"   Domain: {company_domain}")
            
            # Step 1: Try to enrich the contact using People/Match with company context
            logger.info(f"   🔓 Step 1: Attempting People/Match enrichment...")
            enriched_data = await self.test_contact_enrichment(apollo_id, contact_name, company_name, company_domain)
            
            if enriched_data:
                logger.info(f"✅ ENRICHMENT SUCCESSFUL for {contact_name}")
                logger.info(f"   Real email: {enriched_data.get('email', 'N/A')}")
                logger.info(f"   Title: {enriched_data.get('title', 'N/A')}")
                logger.info(f"   LinkedIn: {enriched_data.get('linkedin_url', 'N/A')}")
                
                # Step 2: Update the database with the unlocked email
                logger.info(f"   💾 Step 2: Updating database...")
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE summer_camps.contacts 
                            SET contact_email = %s, email_quality = 'direct', last_enriched_at = NOW()
                            WHERE apollo_contact_id = %s
                        """, (enriched_data['email'], apollo_id))
                        conn.commit()
                        logger.info(f"   ✅ Database updated successfully!")
                
            else:
                logger.info(f"❌ ENRICHMENT FAILED for {contact_name}")
            
            # Small delay between tests
            await asyncio.sleep(2)
        
        logger.info("🧪 Complete Enrichment Workflow testing completed!")

    async def comprehensive_email_discovery(self, batch_size: int = 50, workers: int = 5, limit_contacts: int = None) -> Dict:
        """
        Comprehensive two-phase email discovery strategy:
        Phase 1: Unlock emails for existing Apollo contacts
        Phase 2: Discover new Apollo contacts for organizations without them
        """
        try:
            logger.info(f"🚀 Starting COMPREHENSIVE EMAIL DISCOVERY for all contacts...")
            
            # Phase 1: Unlock emails for existing Apollo contacts
            logger.info(f"📋 PHASE 1: Unlocking emails for existing Apollo contacts...")
            phase1_result = await self.unlock_emails_for_contacts(limit=limit_contacts, workers=workers)
            
            if not phase1_result.get('success'):
                logger.error(f"❌ Phase 1 failed: {phase1_result.get('error')}")
                return phase1_result
            
            # Phase 2: Discover new Apollo contacts for organizations without them
            logger.info(f"📋 PHASE 2: Discovering new Apollo contacts for organizations...")
            phase2_result = await self.discover_new_apollo_contacts(batch_size=batch_size, workers=workers)
            
            if not phase2_result.get('success'):
                logger.error(f"❌ Phase 2 failed: {phase2_result.get('error')}")
                return phase2_result
            
            # Combine results
            total_emails_found = phase1_result.get('emails_unlocked', 0) + phase2_result.get('emails_found', 0)
            total_contacts_processed = phase1_result.get('contacts_processed', 0) + phase2_result.get('contacts_discovered', 0)
            
            logger.info(f"🎉 COMPREHENSIVE EMAIL DISCOVERY COMPLETED!")
            logger.info(f"   Phase 1 - Emails Unlocked: {phase1_result.get('emails_unlocked', 0)}")
            logger.info(f"   Phase 2 - New Contacts: {phase2_result.get('contacts_discovered', 0)}")
            logger.info(f"   Total Emails Found: {total_emails_found}")
            logger.info(f"   Total Contacts Processed: {total_contacts_processed}")
            
            return {
                "success": True,
                "phase1": phase1_result,
                "phase2": phase2_result,
                "total_emails_found": total_emails_found,
                "total_contacts_processed": total_contacts_processed
            }
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive email discovery: {e}")
            return {"success": False, "error": str(e)}

    async def unlock_emails_for_contacts(self, limit: int = None, workers: int = 5) -> Dict:
        """Unlock emails for existing contacts using Apollo enrichment."""
        try:
            logger.info(f"🔓 Starting email unlocking for existing contacts...")
            
            # Get contacts that need email unlocking
            contacts = await self.get_contacts_needing_email_unlock(limit)
            
            if not contacts:
                logger.info("✅ No contacts found needing email unlocking")
                return {"success": True, "contacts_processed": 0, "emails_unlocked": 0}
            
            logger.info(f"📋 Found {len(contacts)} contacts needing email unlocking")
            
            # Process contacts with rate limiting
            semaphore = asyncio.Semaphore(workers)
            tasks = []
            
            for contact in contacts:
                task = asyncio.create_task(
                    self._unlock_single_contact(contact, semaphore)
                )
                tasks.append(task)
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            successful_unlocks = 0
            failed_unlocks = 0
            
            for result in results:
                if isinstance(result, Exception):
                    failed_unlocks += 1
                    logger.error(f"❌ Email unlock failed: {result}")
                elif result and result.get('success'):
                    successful_unlocks += 1
                else:
                    failed_unlocks += 1
            
            logger.info(f"✅ Email unlocking completed!")
            logger.info(f"   Successful unlocks: {successful_unlocks}")
            logger.info(f"   Failed unlocks: {failed_unlocks}")
            logger.info(f"   Total processed: {len(contacts)}")
            
            return {
                "success": True,
                "contacts_processed": len(contacts),
                "emails_unlocked": successful_unlocks,
                "failed_unlocks": failed_unlocks
            }
            
        except Exception as e:
            logger.error(f"❌ Error in email unlocking: {e}")
            return {"success": False, "error": str(e)}

    async def discover_new_apollo_contacts(self, batch_size: int = 50, workers: int = 5) -> Dict:
        """
        Phase 2: Discover new Apollo contacts for organizations that don't have them.
        This converts non-Apollo contacts to Apollo contacts for future email unlocking.
        """
        try:
            logger.info(f"🔍 PHASE 2: Discovering new Apollo contacts for organizations...")
            
            # Get organizations without Apollo contacts
            organizations = await self.get_organizations_needing_apollo_contacts()
            
            if not organizations:
                logger.info("✅ All organizations already have Apollo contacts")
                return {"success": True, "contacts_discovered": 0, "emails_found": 0}
            
            logger.info(f"📋 Found {len(organizations)} organizations needing Apollo contacts")
            
            # Process in batches
            total_contacts_discovered = 0
            total_emails_found = 0
            
            for i in range(0, len(organizations), batch_size):
                batch = organizations[i:i + batch_size]
                logger.info(f"📦 Processing batch {i//batch_size + 1}: {len(batch)} organizations")
                
                # Process batch with rate limiting
                semaphore = asyncio.Semaphore(workers)
                tasks = []
                
                for org in batch:
                    task = asyncio.create_task(
                        self._discover_contacts_for_organization(org, semaphore)
                    )
                    tasks.append(task)
                
                # Wait for batch to complete
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process batch results
                for result in batch_results:
                    if isinstance(result, Exception):
                        logger.error(f"❌ Contact discovery failed: {result}")
                    elif result and result.get('success'):
                        total_contacts_discovered += result.get('contacts_discovered', 0)
                        total_emails_found += result.get('emails_found', 0)
                
                # Brief pause between batches
                if i + batch_size < len(organizations):
                    logger.info(f"⏳ Pausing between batches...")
                    await asyncio.sleep(2)
            
            logger.info(f"✅ Phase 2 completed!")
            logger.info(f"   New contacts discovered: {total_contacts_discovered}")
            logger.info(f"   Emails found: {total_emails_found}")
            
            return {
                "success": True,
                "contacts_discovered": total_contacts_discovered,
                "emails_found": total_emails_found
            }
            
        except Exception as e:
            logger.error(f"❌ Error in Phase 2 contact discovery: {e}")
            return {"success": False, "error": str(e)}

    async def get_organizations_needing_apollo_contacts(self) -> List[Dict]:
        """Get organizations that don't have Apollo contacts yet."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT o.org_id, o.company_name, o.website_url
                        FROM summer_camps.organizations o
                        LEFT JOIN summer_camps.contacts c ON o.org_id = c.org_id 
                            AND c.apollo_contact_id IS NOT NULL 
                            AND c.apollo_contact_id != ''
                        WHERE c.contact_id IS NULL  -- No Apollo contacts found yet
                        AND o.website_url IS NOT NULL 
                        AND o.website_url != ''
                        AND o.website_url NOT LIKE '%placeholder%'
                        ORDER BY o.org_id
                    """)
                    
                    organizations = []
                    for row in cur.fetchall():
                        org_id, company_name, website_url = row
                        organizations.append({
                            'org_id': org_id,
                            'company_name': company_name,
                            'website_url': website_url
                        })
                    
                    return organizations
                    
        except Exception as e:
            logger.error(f"❌ Error getting organizations needing Apollo contacts: {e}")
            return []

    async def _discover_contacts_for_organization(self, organization: Dict, semaphore: asyncio.Semaphore) -> Dict:
        """Discover Apollo contacts for a single organization."""
        async with semaphore:
            try:
                org_id = organization['org_id']
                company_name = organization['company_name']
                website_url = organization['website_url']
                
                logger.info(f"🔍 Discovering contacts for: {company_name}")
                
                # Use smart company search to find contacts
                contacts = await self.smart_company_search(company_name, website_url, business_type='camp')
                
                if not contacts:
                    logger.info(f"   No contacts found for {company_name}")
                    return {"success": True, "contacts_discovered": 0, "emails_found": 0}
                
                # Save discovered contacts to database
                saved_count = await self.save_quality_contacts_to_db(org_id, contacts)
                
                logger.info(f"   ✅ Discovered {len(contacts)} contacts, saved {saved_count}")
                
                return {
                    "success": True,
                    "contacts_discovered": len(contacts),
                    "emails_found": 0  # These will be unlocked in Phase 1 of next run
                }
                
            except Exception as e:
                logger.error(f"❌ Error discovering contacts for {organization.get('company_name', 'Unknown')}: {e}")
                return {"success": False, "error": str(e)}

    async def get_contacts_needing_email_unlock(self, limit: int = None) -> List[Dict]:
        """Get contacts that have Apollo IDs but need email unlocking."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    query = """
                        SELECT 
                            c.contact_id, c.contact_name, c.apollo_contact_id, 
                            c.org_id, o.company_name, o.website_url
                        FROM summer_camps.contacts c
                        JOIN summer_camps.organizations o ON c.org_id = o.org_id
                        WHERE c.apollo_contact_id IS NOT NULL 
                        AND c.apollo_contact_id != ''
                        AND (c.contact_email IS NULL OR c.contact_email = '' OR c.contact_email LIKE '%email_not_unlocked%')
                        ORDER BY c.contact_id
                    """
                    
                    if limit and isinstance(limit, int):
                        query += f" LIMIT {limit}"
                    
                    cur.execute(query)
                    
                    contacts = []
                    for row in cur.fetchall():
                        contact_id, contact_name, apollo_contact_id, org_id, company_name, website_url = row
                        contacts.append({
                            'contact_id': contact_id,
                            'contact_name': contact_name,
                            'apollo_contact_id': apollo_contact_id,
                            'org_id': org_id,
                            'company_name': company_name,
                            'website_url': website_url
                        })
                    
                    return contacts
                    
        except Exception as e:
            logger.error(f"❌ Error getting contacts needing email unlock: {e}")
            return []

    async def _unlock_single_contact(self, contact: Dict, semaphore: asyncio.Semaphore) -> Dict:
        """Unlock email for a single contact using Apollo enrichment."""
        async with semaphore:
            try:
                contact_name = contact['contact_name']
                apollo_contact_id = contact['apollo_contact_id']
                company_name = contact['company_name']
                website_url = contact['website_url']
                
                logger.info(f"🔓 Unlocking email for: {contact_name} at {company_name}")
                
                # Extract domain from website URL
                domain = None
                if website_url:
                    try:
                        from urllib.parse import urlparse
                        parsed = urlparse(website_url)
                        domain = parsed.netloc
                        if domain.startswith('www.'):
                            domain = domain[4:]
                    except:
                        domain = None
                
                if not domain:
                    logger.info(f"   ⚠️ No domain found for {company_name}, skipping")
                    return {"success": False, "error": "No domain available"}
                
                # Use contact enrichment to unlock email
                enriched_data = await self.test_contact_enrichment(
                    apollo_contact_id, contact_name, company_name, domain
                )
                
                if enriched_data and enriched_data.get('email'):
                    # Update database with unlocked email
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE summer_camps.contacts 
                                SET contact_email = %s, email_quality = 'direct', last_enriched_at = NOW()
                                WHERE contact_id = %s
                            """, (enriched_data['email'], contact['contact_id']))
                            conn.commit()
                    
                    logger.info(f"   ✅ Email unlocked: {enriched_data['email']}")
                    return {"success": True, "email": enriched_data['email']}
                else:
                    logger.info(f"   ❌ Email unlock failed for {contact_name}")
                    return {"success": False, "error": "No email found"}
                
            except Exception as e:
                logger.error(f"❌ Error unlocking email for {contact.get('contact_name', 'Unknown')}: {e}")
                return {"success": False, "error": str(e)}

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Apollo Enricher for Broadway Summer Camps')
    parser.add_argument('--test', action='store_true', help='Test API connection only')
    parser.add_argument('--contact-ids', type=str, help='Comma-separated list of contact IDs to enrich')
    parser.add_argument('--all', action='store_true', help='Enrich all contacts in database')
    parser.add_argument('--bulk-all', action='store_true', help='Bulk enrich (in batches of 10) all contacts missing direct emails')
    parser.add_argument('--smart-search', type=str, help='Smart company search: company name or "test" for sample')
    parser.add_argument('--business-type', type=str, default='camp', choices=['camp', 'store', 'activity'], help='Business type for smart search (default: camp)')
    parser.add_argument('--batch-50', action='store_true', help='Process next 50 organizations with smart search')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers for batch processing (default: 5)')
    parser.add_argument('--unlock-emails', action='store_true', help='Unlock emails for existing contacts with Apollo IDs')
    parser.add_argument('--test-enrichment', action='store_true', help='Test Apollo People Enrichment endpoint on sample contacts')
    parser.add_argument('--test-find-people', action='store_true', help='Test Apollo Find People with Filters endpoint on sample companies')
    parser.add_argument('--test-enrichment-workflow', action='store_true', help='Test complete Apollo enrichment workflow: Find -> Enrich -> Get Email')
    parser.add_argument('--comprehensive-discovery', action='store_true', help='Run comprehensive two-phase email discovery: Phase 1 (unlock existing) + Phase 2 (discover new)')
    parser.add_argument('--output', type=str, default='apollo_enrichment_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize enricher
    try:
        enricher = ApolloEnricher()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return
    
    # Test API connection first
    logger.info("Testing Apollo API connection...")
    if not await enricher.test_api_connection():
        logger.error("❌ API connection failed. Please check your configuration.")
        return
    
    if args.test:
        logger.info("✅ API test completed successfully!")
        return
    
    # Smart company search mode
    if args.smart_search:
        if args.smart_search.lower() == "test":
            # Test with Mohawk Day Camp
            test_company = "Mohawk. Day .Camp"
            test_website = "https://www.campmohawk.com/"
            logger.info(f"🧪 Testing smart company search with: {test_company}")
            
            # Get organization ID from database
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT org_id FROM summer_camps.organizations 
                        WHERE company_name = %s
                    """, (test_company,))
                    result = cur.fetchone()
                    if result:
                        org_id = result[0]
                        logger.info(f"Found organization ID: {org_id}")
                        
                        # Run smart search
                        contacts = await enricher.smart_company_search(test_company, test_website, args.business_type)
                        
                        if contacts:
                            # Save to database
                            saved_count = await enricher.save_quality_contacts_to_db(org_id, contacts)
                            logger.info(f"✅ Smart search completed! Found {len(contacts)} quality contacts, saved {saved_count} to database")
                            
                            # Display results
                            print(f"\n📊 SMART SEARCH RESULTS FOR: {test_company}")
                            print(f"   Business Type: {args.business_type}")
                            print(f"   Quality Contacts Found: {len(contacts)}")
                            print(f"   Saved to Database: {saved_count}")
                            print("\n   Top Quality Contacts:")
                            for i, contact in enumerate(contacts[:5], 1):
                                print(f"     {i}. {contact['name']} - {contact['title']}")
                                print(f"        Quality Score: {contact['quality_score']}")
                                if contact['email'] and contact['email'] != 'email_not_unlocked@domain.com':
                                    print(f"        Email: {contact['email']}")
                                print()
                        else:
                            logger.info("No quality contacts found")
                    else:
                        logger.error(f"Organization not found in database: {test_company}")
        else:
            # Search for specific company
            company_name = args.smart_search
            logger.info(f"🔍 Smart company search for: {company_name}")
            logger.info(f"   Business type: {args.business_type}")
            
            # Get organization info from database
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT org_id, company_name, website_url 
                        FROM summer_camps.organizations 
                        WHERE company_name ILIKE %s
                    """, (f'%{company_name}%',))
                    result = cur.fetchone()
                    if result:
                        org_id, db_company_name, website_url = result
                        logger.info(f"Found organization: {db_company_name} (ID: {org_id})")
                        
                        # Run smart search
                        contacts = await enricher.smart_company_search(db_company_name, website_url, args.business_type)
                        
                        if contacts:
                            # Save to database
                            saved_count = await enricher.save_quality_contacts_to_db(org_id, contacts)
                            logger.info(f"✅ Smart search completed! Found {len(contacts)} quality contacts, saved {saved_count} to database")
                            
                            # Display results
                            print(f"\n📊 SMART SEARCH RESULTS FOR: {db_company_name}")
                            print(f"   Business Type: {args.business_type}")
                            print(f"   Quality Contacts Found: {len(contacts)}")
                            print(f"   Saved to Database: {saved_count}")
                        else:
                            logger.info("No quality contacts found")
                    else:
                        logger.error(f"Organization not found in database: {company_name}")
        
        return
    
    # Batch processing mode
    if args.batch_50:
        logger.info("🚀 Starting batch processing of next 50 organizations...")
        logger.info(f"   Workers: {args.workers}")
        logger.info(f"   Batch size: 50 organizations")
        
        # Run batch smart search
        batch_results = await enricher.batch_smart_search(max_workers=args.workers, batch_size=50)
        
        # Display results
        print(f"\n📊 BATCH PROCESSING RESULTS:")
        print(f"Total organizations: {batch_results['total_organizations']}")
        print(f"Successfully processed: {batch_results['processed']}")
        print(f"Total contacts found: {batch_results['total_contacts_found']}")
        print(f"Total contacts saved: {batch_results['total_contacts_saved']}")
        print(f"Average contacts per organization: {batch_results['total_contacts_found']/batch_results['total_organizations']:.1f}")
        
        # Save detailed results
        import pandas as pd
        df = pd.DataFrame(batch_results['results'])
        output_file = f"outputs/apollo_batch_50_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        print(f"\n📊 Detailed results saved to: {output_file}")
        
        # Save summary report
        report_path = f"outputs/apollo_batch_50_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_path, 'w') as f:
            f.write(f"Apollo Batch Processing Report\n")
            f.write(f"Generated: {datetime.now()}\n\n")
            f.write(f"Total organizations: {batch_results['total_organizations']}\n")
            f.write(f"Successfully processed: {batch_results['processed']}\n")
            f.write(f"Total contacts found: {batch_results['total_contacts_found']}\n")
            f.write(f"Total contacts saved: {batch_results['total_contacts_saved']}\n\n")
            f.write("Organization Results:\n")
            for result in batch_results['results']:
                f.write(f"  {result['company_name']}: {result['contacts_found']} found, {result['contacts_saved']} saved\n")
        
        print(f"📋 Summary report saved to: {report_path}")
        return
    
    # Email unlocking mode
    if args.unlock_emails:
        logger.info("🔓 Starting email unlock for existing contacts with Apollo IDs...")
        
        # Get contacts that have Apollo IDs but no real emails
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT contact_id, contact_name, apollo_contact_id, contact_email
                    FROM summer_camps.contacts 
                    WHERE apollo_contact_id IS NOT NULL 
                    AND (contact_email IS NULL OR contact_email = 'email_not_unlocked@domain.com')
                    ORDER BY contact_id
                    LIMIT 5
                """)
                contacts_to_unlock = cur.fetchall()
        
        if not contacts_to_unlock:
            logger.info("✅ No contacts need email unlocking")
            return
        
        logger.info(f"🔓 Found {len(contacts_to_unlock)} contacts needing email unlock")
        
        # Convert to the format expected by unlock_emails_for_contacts
        contacts_dict = []
        for contact in contacts_to_unlock:
            contacts_dict.append({
                'apollo_id': contact[2],  # apollo_contact_id
                'name': contact[1],       # contact_name
                'email': contact[3]       # current email
            })
        
        # Unlock emails
        unlocked_contacts = await enricher.enrich_emails_for_contacts(contacts_dict)
        
        # Update database with unlocked emails
        updated_count = 0
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for contact in unlocked_contacts:
                    if contact.get('email') and contact['email'] != 'email_not_unlocked@domain.com':
                        cur.execute("""
                            UPDATE summer_camps.contacts 
                            SET contact_email = %s, email_quality = 'direct', last_enriched_at = NOW()
                            WHERE apollo_contact_id = %s
                        """, (contact['email'], contact['apollo_id']))
                        updated_count += 1
                conn.commit()
        logger.info(f"✅ Email unlock completed! Updated {updated_count} contacts in database")
        
        # Display results
        print(f"\n🔓 EMAIL UNLOCK RESULTS:")
        print(f"Contacts processed: {len(contacts_to_unlock)}")
        print(f"Emails unlocked: {updated_count}")
        print(f"Success rate: {(updated_count/len(contacts_to_unlock)*100):.1f}%")
        return
    
    # Test People Enrichment endpoint
    if args.test_enrichment:
        logger.info("🧪 Starting Apollo People Enrichment endpoint testing...")
        await enricher.test_enrichment_on_sample_contacts()
        return
    
    # Test Find People with Filters endpoint
    if args.test_find_people:
        logger.info("🧪 Starting Apollo Find People with Filters endpoint testing...")
        await enricher.test_find_people_on_sample_companies()
        return
    
    # Test Complete Enrichment Workflow
    if args.test_enrichment_workflow:
        logger.info("🧪 Starting Complete Apollo Enrichment Workflow testing...")
        await enricher.test_enrichment_workflow()
        return
    
    # Comprehensive Two-Phase Email Discovery
    if args.comprehensive_discovery:
        logger.info("🚀 Starting comprehensive two-phase email discovery...")
        logger.info("📋 This will:")
        logger.info("   1. Unlock emails for existing Apollo contacts (Phase 1)")
        logger.info("   2. Discover new Apollo contacts for organizations (Phase 2)")
        logger.info("   3. Process in batches of 50 with 5 workers")
        
        # Get batch size and workers from arguments
        batch_size = 50
        workers = args.workers
        
        # For testing, limit to first 5 contacts
        limit_contacts = 5
        
        result = await enricher.comprehensive_email_discovery(
            batch_size=batch_size, 
            workers=workers, 
            limit_contacts=limit_contacts
        )
        
        if result.get('success'):
            logger.info("🎉 Comprehensive email discovery completed successfully!")
            logger.info(f"   Phase 1 - Emails Unlocked: {result.get('phase1', {}).get('emails_unlocked', 0)}")
            logger.info(f"   Phase 2 - New Contacts: {result.get('phase2', {}).get('contacts_discovered', 0)}")
            logger.info(f"   Total Emails Found: {result.get('total_emails_found', 0)}")
            logger.info(f"   Total Contacts Processed: {result.get('total_contacts_processed', 0)}")
        else:
            logger.error(f"❌ Comprehensive email discovery failed: {result.get('error')}")
        return
    
    # Bulk mode
    if args.bulk_all:
        logger.info("Starting Apollo BULK enrichment (batches of 10) for contacts missing direct emails...")
        contacts = await enricher.get_contacts_missing_direct_email()
        logger.info(f"Bulk queue size: {len(contacts)}")
        bulk_results = await enricher.enrich_bulk_contacts(contacts, batch_size=10, reveal_personal_emails=True, reveal_phone_number=False)
        # Save basic CSV
        enricher.save_enrichment_results([
            {
                'contact_id': r.get('contact_id'),
                'contact_name': '',
                'org_id': None,
                'company_name': '',
                'email': r.get('email', ''),
                'phone': r.get('phone', ''),
                'title': r.get('title', ''),
                'enriched': bool(r.get('email')),
                'reason': '',
                'enrichment_timestamp': r.get('enrichment_timestamp', '')
            } for r in bulk_results
        ], args.output)
        report = enricher.generate_enrichment_report(bulk_results)
        print(report)
        report_path = args.output.replace('.csv', '_report.txt')
        with open(report_path, 'w') as f:
            f.write(report)
        logger.info(f"Saved bulk enrichment report to {report_path}")
        return

    # Determine which contacts to enrich (single flow)
    contact_ids = None
    if args.contact_ids:
        contact_ids = [int(x.strip()) for x in args.contact_ids.split(',')]
    elif not args.all:
        # Default to first 3 for testing
        contact_ids = [1, 2, 3]
        logger.info("No specific contacts specified, testing with first 3 contacts")

    # Run enrichment (single)
    logger.info(f"Starting Apollo enrichment...")
    results = await enricher.enrich_all_contacts(contact_ids)

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
