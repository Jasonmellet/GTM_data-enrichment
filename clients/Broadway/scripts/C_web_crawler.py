#!/usr/bin/env python3
"""
Multi-worker web crawler module for Broadway summer camps project.
Handles website health checks, contact extraction, and robots.txt/sitemap analysis.
"""

import asyncio
import aiohttp
import pandas as pd
import os
import sys
import time
import logging
from typing import Dict, List, Tuple, Optional, Set
from urllib.parse import urljoin, urlparse
import re
from bs4 import BeautifulSoup
import json
from dataclasses import dataclass
from datetime import datetime
import argparse
from db_connection import get_db_connection
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("Playwright not available. Install with: pip install playwright && playwright install")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class CrawlResult:
    """Results from crawling a single website."""
    org_id: int
    company_name: str
    website_url: str
    website_status: str
    http_status_code: int
    response_time: float
    is_placeholder: bool
    robots_txt_summary: str
    sitemap_summary: str
    contact_pages_found: List[str]
    contacts_found: List[Dict]
    crawl_notes: str
    crawl_timestamp: datetime

class WebCrawler:
    """Multi-worker web crawler for summer camp websites."""
    
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers
        self.session = None
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Placeholder detection patterns
        self.placeholder_patterns = [
            r'buy this domain',
            r'domain for sale',
            r'coming soon',
            r'under construction',
            r'parked page',
            r'domain parking',
            r'this domain may be for sale',
            r'domain auction',
            r'domain broker'
        ]
        
        # Contact-related keywords
        self.contact_keywords = [
            'director', 'owner', 'manager', 'assistant director', 'camp director',
            'program director', 'executive director', 'founder', 'president',
            'ceo', 'coordinator', 'supervisor', 'head', 'lead'
        ]
    
    async def init_session(self):
        """Initialize aiohttp session."""
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector
        )
    
    async def close_session(self):
        """Close aiohttp session."""
        if self.session:
            await self.session.close()
    
    def is_placeholder_site(self, html_content: str, title: str) -> bool:
        """Detect if a website is a placeholder."""
        content_lower = html_content.lower()
        title_lower = title.lower()
        
        for pattern in self.placeholder_patterns:
            if re.search(pattern, content_lower) or re.search(pattern, title_lower):
                return True
        
        return False
    
    async def fetch_page(self, url: str) -> Tuple[int, str, float, str]:
        """Fetch a single page with error handling."""
        start_time = time.time()
        
        try:
            async with self.session.get(url, allow_redirects=True) as response:
                response_time = time.time() - start_time
                content = await response.text()
                return response.status, content, response_time, response.url
        except Exception as e:
            response_time = time.time() - start_time
            return 0, str(e), response_time, url
    
    async def analyze_robots_txt(self, base_url: str) -> str:
        """Analyze robots.txt file and provide AI summary."""
        robots_url = urljoin(base_url, '/robots.txt')
        
        try:
            status, content, _, _ = await self.fetch_page(robots_url)
            
            if status == 200 and content.strip():
                # Simple AI-like analysis
                lines = content.split('\n')
                user_agents = []
                disallows = []
                allows = []
                sitemap = None
                
                for line in lines:
                    line = line.strip().lower()
                    if line.startswith('user-agent:'):
                        user_agents.append(line.split(':', 1)[1].strip())
                    elif line.startswith('disallow:'):
                        disallows.append(line.split(':', 1)[1].strip())
                    elif line.startswith('allow:'):
                        allows.append(line.split(':', 1)[1].strip())
                    elif line.startswith('sitemap:'):
                        sitemap = line.split(':', 1)[1].strip()
                
                summary = f"Robots.txt found with {len(user_agents)} user agents, {len(disallows)} disallows, {len(allows)} allows"
                if sitemap:
                    summary += f", sitemap at {sitemap}"
                
                return summary
            else:
                return "No robots.txt found or accessible"
                
        except Exception as e:
            return f"Error analyzing robots.txt: {str(e)}"
    
    async def analyze_sitemap(self, base_url: str) -> str:
        """Analyze sitemap and provide AI summary."""
        sitemap_urls = [
            urljoin(base_url, '/sitemap.xml'),
            urljoin(base_url, '/sitemap_index.xml'),
            urljoin(base_url, '/sitemap/'),
            urljoin(base_url, '/sitemap/sitemap.xml')
        ]
        
        for sitemap_url in sitemap_urls:
            try:
                status, content, _, _ = await self.fetch_page(sitemap_url)
                
                if status == 200 and content.strip():
                    # Simple XML analysis
                    if '<?xml' in content and 'urlset' in content:
                        urls = re.findall(r'<loc>(.*?)</loc>', content)
                        if urls:
                            return f"Sitemap found with {len(urls)} URLs, including: {', '.join(urls[:5])}{'...' if len(urls) > 5 else ''}"
                        else:
                            return "Sitemap found but no URLs detected"
                    else:
                        return "Sitemap found but not in expected XML format"
                        
            except Exception as e:
                continue
        
        return "No sitemap found"
    
    def extract_contacts_from_html(self, html_content: str, base_url: str) -> List[Dict]:
        """Extract contact information from HTML content with intelligent filtering."""
        soup = BeautifulSoup(html_content, 'html.parser')
        contacts = []
        
        # Remove script and style tags to avoid extracting JS/CSS values
        for script in soup(["script", "style", "noscript"]):
            script.decompose()
        
        # Get clean text content
        text_content = soup.get_text()
        
        # Look for REAL contact information with better validation
        
        # 1. Extract and validate EMAILS (only business-relevant ones)
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(email_pattern, text_content)
        
        for email in emails:
            email = email.lower().strip()
            # Filter out common false positives
            if (email and 
                not email.startswith(('info@', 'contact@', 'hello@', 'admin@', 'noreply@', 'test@')) and
                not email.endswith(('.js', '.css', '.png', '.jpg', '.gif')) and
                not any(word in email for word in ['example', 'test', 'demo', 'sample', 'placeholder']) and
                len(email) < 50):  # Reasonable email length
                
                contacts.append({
                    'type': 'email',
                    'value': email,
                    'quality': 'direct',
                    'source': 'website'
                })
        
        # 2. Extract and validate PHONE NUMBERS (only business-relevant ones)
        # Look for phone numbers in specific contexts
        phone_patterns = [
            # Standard US phone formats
            r'\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            # International formats
            r'\+?1[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            # Toll-free numbers
            r'1?[-.\s]?(800|888|877|866|855|844|833)[-.\s]?(\d{3})[-.\s]?(\d{4})'
        ]
        
        for pattern in phone_patterns:
            matches = re.finditer(pattern, text_content)
            for match in matches:
                phone = match.group(0)
                # Clean the phone number
                clean_phone = re.sub(r'[^\d\+]', '', phone)
                
                # Validation: must be reasonable length and not look like random data
                if (len(clean_phone) >= 10 and 
                    len(clean_phone) <= 15 and
                    not clean_phone.startswith('000') and
                    not clean_phone.startswith('999') and
                    not all(d == clean_phone[0] for d in clean_phone) and  # Not all same digits
                    not clean_phone in ['1234567890', '0000000000', '9999999999']):
                    
                    # Check if this phone appears near contact-related text
                    context_start = max(0, match.start() - 100)
                    context_end = min(len(text_content), match.end() + 100)
                    context = text_content[context_start:context_end].lower()
                    
                    contact_keywords = ['phone', 'call', 'contact', 'tel', 'telephone', 'reach', 'call us', 'contact us']
                    if any(keyword in context for keyword in contact_keywords):
                        contacts.append({
                            'type': 'phone',
                            'value': clean_phone,
                            'quality': 'direct',
                            'source': 'website'
                        })
        
        # 3. Extract REAL CONTACT NAMES with job titles (much more selective)
        # Look for actual people names, not random text
        name_patterns = [
            # Look for patterns like "John Smith, Director" or "Director: John Smith"
            r'([A-Z][a-z]+ [A-Z][a-z]+)[,\s]*(?:Director|Owner|Manager|Coordinator|Founder|President|CEO)',
            r'(?:Director|Owner|Manager|Coordinator|Founder|President|CEO)[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)',
            # Look for names in contact sections
            r'Contact[:\s]*([A-Z][a-z]+ [A-Z][a-z]+)',
            r'([A-Z][a-z]+ [A-Z][a-z]+)[,\s]*Contact'
        ]
        
        for pattern in name_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                # Validate this looks like a real name
                if (len(name.split()) == 2 and  # First and last name
                    all(word.isalpha() for word in name.split()) and  # Only letters
                    len(name) >= 6 and  # Reasonable name length
                    len(name) <= 30 and
                    not any(word.lower() in ['google', 'tag', 'manager', 'container', 'theme', 'widget'] for word in name.split())):
                    
                    # Extract job title from context
                    job_title = None
                    for keyword in self.contact_keywords:
                        if keyword.lower() in pattern.lower():
                            job_title = keyword
                            break
                    
                    contacts.append({
                        'type': 'contact_name',
                        'value': name,
                        'job_title': job_title,
                        'quality': 'direct',
                        'source': 'website'
                    })
        
        # 4. Look for contact information in structured data (schema.org, etc.)
        # This is more reliable than text parsing
        structured_contacts = self.extract_structured_contacts(soup)
        contacts.extend(structured_contacts)
        
        # 5. Remove duplicates and filter out low-quality entries
        unique_contacts = []
        seen_values = set()
        
        for contact in contacts:
            # Create a unique key for deduplication
            if contact['type'] == 'phone':
                key = re.sub(r'[^\d]', '', contact['value'])  # Just digits for phones
            else:
                key = contact['value'].lower()
            
            if key not in seen_values and len(key) > 0:
                # Additional quality checks
                if self.is_high_quality_contact(contact):
                    unique_contacts.append(contact)
                    seen_values.add(key)
        
        return unique_contacts
    
    def extract_structured_contacts(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract contacts from structured data like schema.org markup."""
        contacts = []
        
        # Look for schema.org Person or Organization markup
        person_schemas = soup.find_all(attrs={"itemtype": re.compile(r".*Person.*")})
        org_schemas = soup.find_all(attrs={"itemtype": re.compile(r".*Organization.*")})
        
        for schema in person_schemas + org_schemas:
            # Extract name
            name_elem = schema.find(attrs={"itemprop": "name"})
            if name_elem:
                name = name_elem.get_text().strip()
                if self.is_valid_name(name):
                    # Extract job title
                    job_title = None
                    job_elem = schema.find(attrs={"itemprop": "jobTitle"})
                    if job_elem:
                        job_title = job_elem.get_text().strip()
                    
                    # Extract email
                    email_elem = schema.find(attrs={"itemprop": "email"})
                    email = None
                    if email_elem:
                        email = email_elem.get_text().strip()
                        if self.is_valid_email(email):
                            contacts.append({
                                'type': 'email',
                                'value': email,
                                'quality': 'structured',
                                'source': 'schema.org'
                            })
                    
                    # Extract phone
                    phone_elem = schema.find(attrs={"itemprop": "telephone"})
                    phone = None
                    if phone_elem:
                        phone = phone_elem.get_text().strip()
                        if self.is_valid_phone(phone):
                            contacts.append({
                                'type': 'phone',
                                'value': phone,
                                'quality': 'structured',
                                'source': 'schema.org'
                            })
                    
                    contacts.append({
                        'type': 'contact_name',
                        'value': name,
                        'job_title': job_title,
                        'quality': 'structured',
                        'source': 'schema.org'
                    })
        
        return contacts
    
    def is_valid_name(self, name: str) -> bool:
        """Validate if a string looks like a real person's name."""
        if not name or len(name) < 3 or len(name) > 50:
            return False
        
        # Must contain at least two words (first and last name)
        words = name.split()
        if len(words) < 2:
            return False
        
        # Each word should be alphabetic and reasonable length
        for word in words:
            if not word.isalpha() or len(word) < 2 or len(word) > 20:
                return False
        
        # Filter out common false positives
        false_positives = ['google', 'tag', 'manager', 'container', 'theme', 'widget', 
                          'script', 'style', 'div', 'span', 'class', 'id']
        
        name_lower = name.lower()
        if any(fp in name_lower for fp in false_positives):
            return False
        
        return True
    
    def is_valid_email(self, email: str) -> bool:
        """Validate if an email looks legitimate."""
        if not email or '@' not in email:
            return False
        
        # Basic email format validation
        email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'
        if not re.match(email_pattern, email):
            return False
        
        # Filter out common false positives
        false_positives = ['example.com', 'test.com', 'domain.com', 'placeholder.com']
        domain = email.split('@')[1].lower()
        if domain in false_positives:
            return False
        
        return True
    
    def is_valid_phone(self, phone: str) -> bool:
        """Validate if a phone number looks legitimate."""
        if not phone:
            return False
        
        # Clean the phone number
        clean_phone = re.sub(r'[^\d\+]', '', phone)
        
        # Must be reasonable length
        if len(clean_phone) < 10 or len(clean_phone) > 15:
            return False
        
        # Must not be all the same digit
        if all(d == clean_phone[0] for d in clean_phone):
            return False
        
        # Must not be common test numbers
        test_numbers = ['1234567890', '0000000000', '9999999999', '1111111111']
        if clean_phone in test_numbers:
            return False
        
        return True
    
    def is_high_quality_contact(self, contact: Dict) -> bool:
        """Determine if a contact entry is high quality."""
        if contact['type'] == 'email':
            return self.is_valid_email(contact['value'])
        elif contact['type'] == 'phone':
            return self.is_valid_phone(contact['value'])
        elif contact['type'] == 'contact_name':
            return self.is_valid_name(contact['value'])
        
        return False
    
    async def crawl_single_site(self, org_data: Dict) -> CrawlResult:
        """Crawl a single website and extract information."""
        start_time = time.time()
        
        try:
            website_url = org_data['website_url']
            if not website_url:
                return CrawlResult(
                    org_id=org_data['org_id'],
                    company_name=org_data['company_name'],
                    website_url='',
                    website_status='No website',
                    http_status_code=0,
                    response_time=0,
                    is_placeholder=False,
                    robots_txt_summary='N/A',
                    sitemap_summary='N/A',
                    contact_pages_found=[],
                    contacts_found=[],
                    crawl_notes='No website URL provided',
                    crawl_timestamp=datetime.now()
                )
            
            # Check what data we already have (to avoid redundant crawling)
            existing_data = await self.get_existing_organization_data(org_data['org_id'])
            missing_fields = self.identify_missing_fields(existing_data)
            
            # If we have all the basic business info, focus only on personal contacts
            if not missing_fields:
                crawl_notes = ["All basic business data already available, focusing on personal contacts only"]
            else:
                crawl_notes = [f"Missing data to find: {', '.join(missing_fields)}"]
            
            # Normalize URL
            if not website_url.startswith(('http://', 'https://')):
                website_url = 'https://' + website_url
            
            # Fetch main page
            status, content, response_time, final_url = await self.fetch_page(website_url)
            
            # Analyze content
            soup = BeautifulSoup(content, 'html.parser')
            title = soup.title.string if soup.title else ''
            is_placeholder = self.is_placeholder_site(content, title)
            
            # Determine website status
            if status == 200 and not is_placeholder:
                website_status = 'Working'
            elif is_placeholder:
                website_status = 'Placeholder'
            elif status == 404:
                website_status = 'Not Found'
            elif status == 0:
                website_status = 'Connection Failed'
            else:
                website_status = f'HTTP {status}'
            
            # Analyze robots.txt and sitemap
            robots_summary = await self.analyze_robots_txt(website_url)
            sitemap_summary = await self.analyze_sitemap(website_url)
            
            # Extract contacts from HTML
            html_contacts = self.extract_enhanced_contacts_from_html(soup)
            
            # Extract contacts using JavaScript rendering (for dynamic content)
            js_contacts = await self.extract_contacts_with_js(website_url)
            
            # Combine both contact sources
            all_contacts = {}
            for key in set(list(html_contacts.keys()) + list(js_contacts.keys())):
                if key == 'emails':
                    all_contacts[key] = list(set(html_contacts.get(key, []) + js_contacts.get(key, [])))
                elif key == 'phones':
                    all_contacts[key] = list(set(html_contacts.get(key, []) + js_contacts.get(key, [])))
                elif key == 'names':
                    all_contacts[key] = list(set(html_contacts.get(key, []) + js_contacts.get(key, [])))
                else:
                    all_contacts[key] = js_contacts.get(key, html_contacts.get(key))
            
            # Convert to the format expected by the rest of the code
            contacts_found = []
            for contact_type, values in all_contacts.items():
                if isinstance(values, list):
                    for value in values:
                        contacts_found.append({
                            'type': contact_type,
                            'value': value,
                            'source': 'combined'
                        })
                else:
                    contacts_found.append({
                        'type': contact_type,
                        'value': values,
                        'source': 'combined'
                    })
            
            # Extract business information
            business_info = self.extract_business_info(soup, existing_data, missing_fields)
            
            # Look for contact/about pages with intelligent stopping
            contact_pages = []
            pages_crawled = 1  # We've already crawled the main page
            
            if status == 200 and not is_placeholder:
                # Find links to contact/about pages
                for link in soup.find_all('a', href=True):
                    href = link.get('href', '').lower()
                    text = link.get_text().lower()
                    
                    if any(keyword in href or keyword in text for keyword in ['contact', 'about', 'team', 'staff']):
                        full_url = urljoin(website_url, link['href'])
                        contact_pages.append(full_url)
                        
                        # Calculate current confidence
                        current_confidence, confidence_reason = self.calculate_crawl_confidence(
                            contacts_found, contact_pages, website_status, is_placeholder
                        )
                        
                        # Decide whether to continue crawling
                        should_continue, continue_reason = self.should_continue_crawling(
                            current_confidence, contacts_found, pages_crawled
                        )
                        
                        # If we should continue and haven't hit the limit, crawl this page
                        if should_continue and pages_crawled < 5:
                            try:
                                page_status, page_content, _, _ = await self.fetch_page(full_url)
                                if page_status == 200:
                                    page_soup = BeautifulSoup(page_content, 'html.parser')
                                    
                                    # Extract business info from this page too
                                    page_business_info = self.extract_business_info(page_soup, existing_data, missing_fields)
                                    business_info.update(page_business_info)
                                    
                                    # Extract contacts
                                    page_contacts = self.extract_contacts_from_html(page_content, full_url)
                                    all_contacts.update(page_contacts) # Append to all_contacts
                                    pages_crawled += 1
                                    
                                    # Recalculate confidence after each page
                                    current_confidence, confidence_reason = self.calculate_crawl_confidence(
                                        all_contacts, contact_pages, website_status, is_placeholder
                                    )
                                    
                                    # Check if we should stop after this page
                                    should_continue, continue_reason = self.should_continue_crawling(
                                        current_confidence, all_contacts, pages_crawled
                                    )
                                    
                                    if not should_continue:
                                        logger.info(f"Stopping crawl for {org_data['company_name']}: {continue_reason}")
                                        break
                                        
                            except Exception as e:
                                logger.warning(f"Error fetching contact page {full_url}: {e}")
                        else:
                            if not should_continue:
                                logger.info(f"Not crawling additional pages for {org_data['company_name']}: {continue_reason}")
                                break
            
            # Remove duplicates
            unique_contacts = []
            seen_values = set()
            for contact in contacts_found:  # Use contacts_found instead of all_contacts.values()
                if contact['type'] == 'phone':
                    key = re.sub(r'[^\d]', '', contact['value'])  # Just digits for phones
                else:
                    key = contact['value'].lower()
                
                if key not in seen_values and len(key) > 0:
                    unique_contacts.append(contact)
                    seen_values.add(key)
            
            # Calculate final confidence score
            final_confidence, confidence_reason = self.calculate_crawl_confidence(
                unique_contacts, contact_pages, website_status, is_placeholder
            )
            
            # Persist any business data we found to the database
            if business_info:
                persistence_success = await self.persist_business_data(org_data['org_id'], business_info)
                if persistence_success:
                    crawl_notes.append("Business data successfully saved to database")
                else:
                    crawl_notes.append("Failed to save business data to database")
            
            # Add business info to crawl notes
            if business_info:
                for field, value in business_info.items():
                    if value:
                        crawl_notes.append(f"Found {field}: {value}")
            
            if is_placeholder:
                crawl_notes.append("Site appears to be placeholder/for sale")
            if status != 200:
                crawl_notes.append(f"HTTP status: {status}")
            if not unique_contacts:
                crawl_notes.append("No contact information found")
            
            crawl_notes.append(f"Confidence: {final_confidence}% - {confidence_reason}")
            crawl_notes.append(f"Pages crawled: {pages_crawled}")
            
            return CrawlResult(
                org_id=org_data['org_id'],
                company_name=org_data['company_name'],
                website_url=website_url,
                website_status=website_status,
                http_status_code=status,
                response_time=response_time,
                is_placeholder=is_placeholder,
                robots_txt_summary=robots_summary,
                sitemap_summary=sitemap_summary,
                contact_pages_found=contact_pages[:5],  # Limit to 5
                contacts_found=unique_contacts,
                crawl_notes='; '.join(crawl_notes) if crawl_notes else 'Crawl completed successfully',
                crawl_timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Error crawling {org_data['company_name']}: {e}")
            return CrawlResult(
                org_id=org_data['org_id'],
                company_name=org_data['company_name'],
                website_url=org_data.get('website_url', ''),
                website_status='Error',
                http_status_code=0,
                response_time=0,
                is_placeholder=False,
                robots_txt_summary='Error',
                sitemap_summary='Error',
                contact_pages_found=[],
                contacts_found=[],
                crawl_notes=f'Crawl error: {str(e)}',
                crawl_timestamp=datetime.now()
            )
    
    async def get_existing_organization_data(self, org_id: int) -> Dict:
        """Get existing organization data from database to avoid redundant crawling."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT company_name, company_phone, street, city, state, zip, country, website_url
                        FROM summer_camps.organizations 
                        WHERE org_id = %s
                    """, (org_id,))
                    result = cur.fetchone()
                    
                    if result:
                        return {
                            'company_name': result[0],
                            'company_phone': result[1],
                            'street': result[2],
                            'city': result[3],
                            'state': result[4],
                            'zip': result[5],
                            'country': result[6],
                            'website_url': result[7]
                        }
                    return {}
        except Exception as e:
            logger.warning(f"Could not fetch existing data for org {org_id}: {e}")
            return {}
    
    def identify_missing_fields(self, existing_data: Dict) -> List[str]:
        """Identify which business fields are missing and need to be found."""
        missing = []
        
        # Check for missing basic business info
        if not existing_data.get('company_phone'):
            missing.append('phone')
        if not existing_data.get('street') or not existing_data.get('city'):
            missing.append('address')
        if not existing_data.get('state') or not existing_data.get('zip'):
            missing.append('location')
        
        return missing
    
    def extract_business_info(self, soup: BeautifulSoup, existing_data: Dict, missing_fields: List[str]) -> Dict:
        """Extract business information like address and phone from HTML."""
        business_info = {}
        
        # Only extract what we're missing
        if 'phone' in missing_fields:
            phone = self.extract_business_phone(soup)
            if phone:
                business_info['company_phone'] = phone
        
        if 'address' in missing_fields or 'location' in missing_fields:
            address_info = self.extract_business_address(soup)
            business_info.update(address_info)
        
        return business_info
    
    def extract_business_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract the main business phone number."""
        # Look for phone numbers in specific business contexts
        phone_patterns = [
            r'\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'\+?1[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'1?[-.\s]?(800|888|877|866|855|844|833)[-.\s]?(\d{3})[-.\s]?(\d{4})'
        ]
        
        # Look in specific areas where business phone is likely to be
        phone_areas = [
            'header', 'footer', 'contact', 'about', 'main', 'nav'
        ]
        
        for area in phone_areas:
            elements = soup.find_all(attrs={"class": re.compile(area, re.I)})
            elements.extend(soup.find_all(attrs={"id": re.compile(area, re.I)}))
            
            for element in elements:
                text = element.get_text()
                for pattern in phone_patterns:
                    matches = re.finditer(pattern, text)
                    for match in matches:
                        phone = match.group(0)
                        clean_phone = re.sub(r'[^\d\+]', '', phone)
                        
                        # Validate this looks like a business phone
                        if (len(clean_phone) >= 10 and 
                            len(clean_phone) <= 15 and
                            not clean_phone.startswith('000') and
                            not clean_phone.startswith('999') and
                            not all(d == clean_phone[0] for d in clean_phone)):
                            
                            # Check if this phone appears near business-related text
                            context_start = max(0, match.start() - 50)
                            context_end = min(len(text), match.end() + 50)
                            context = text[context_start:context_end].lower()
                            
                            business_keywords = ['phone', 'call', 'contact', 'tel', 'telephone', 'reach', 'main']
                            if any(keyword in context for keyword in business_keywords):
                                return clean_phone
        
        return None
    
    def extract_business_address(self, soup: BeautifulSoup) -> Dict:
        """Extract business address information."""
        address_info = {}
        
        # Look for address in structured data first (schema.org)
        address_elem = soup.find(attrs={"itemprop": "address"})
        if address_elem:
            street = address_elem.find(attrs={"itemprop": "streetAddress"})
            if street:
                address_info['street'] = street.get_text().strip()
            
            city = address_elem.find(attrs={"itemprop": "addressLocality"})
            if city:
                address_info['city'] = city.get_text().strip()
            
            state = address_elem.find(attrs={"itemprop": "addressRegion"})
            if state:
                address_info['state'] = state.get_text().strip()
            
            zip_code = address_elem.find(attrs={"itemprop": "postalCode"})
            if zip_code:
                address_info['zip'] = zip_code.get_text().strip()
        
        # If no structured data, look for address patterns in text
        if not address_info.get('street'):
            # Look for address patterns
            address_patterns = [
                r'(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Way|Place|Pl))',
                r'(\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Way|Place|Pl))',
            ]
            
            text = soup.get_text()
            for pattern in address_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Look for context around the address
                    for match in matches:
                        match_start = text.find(match)
                        if match_start != -1:
                            context_start = max(0, match_start - 100)
                            context_end = min(len(text), match_start + len(match) + 100)
                            context = text[context_start:context_end].lower()
                            
                            # Check if this looks like a business address
                            address_keywords = ['address', 'location', 'find us', 'visit us', 'our location']
                            if any(keyword in context for keyword in address_keywords):
                                address_info['street'] = match.strip()
                                break
        
        return address_info
    
    async def crawl_all_sites(self, org_ids: Optional[List[int]] = None) -> List[CrawlResult]:
        """Crawl all sites or specific organization IDs."""
        await self.init_session()
        
        try:
            # Get organizations to crawl
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
            
            logger.info(f"Starting crawl of {len(organizations)} organizations with {self.max_workers} workers")
            
            # Create tasks for all organizations
            tasks = []
            for org in organizations:
                task = self.crawl_single_site({
                    'org_id': org[0],
                    'company_name': org[1],
                    'website_url': org[2]
                })
                tasks.append(task)
            
            # Execute tasks with concurrency limit
            semaphore = asyncio.Semaphore(self.max_workers)
            
            async def limited_crawl(task):
                async with semaphore:
                    return await task
            
            limited_tasks = [limited_crawl(task) for task in tasks]
            results = await asyncio.gather(*limited_tasks, return_exceptions=True)
            
            # Filter out exceptions and log them
            valid_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Error processing organization {i+1}: {result}")
                else:
                    valid_results.append(result)
                    logger.info(f"Completed crawl for {result.company_name} (ID: {result.org_id})")
            
            return valid_results
            
        finally:
            await self.close_session()
    
    def save_results_to_csv(self, results: List[CrawlResult], output_path: str):
        """Save crawl results to CSV."""
        data = []
        for result in results:
            data.append({
                'org_id': result.org_id,
                'company_name': result.company_name,
                'website_url': result.website_url,
                'website_status': result.website_status,
                'http_status_code': result.http_status_code,
                'response_time': result.response_time,
                'is_placeholder': result.is_placeholder,
                'robots_txt_summary': result.robots_txt_summary,
                'sitemap_summary': result.sitemap_summary,
                'contact_pages_found': '; '.join(result.contact_pages_found),
                'contacts_found': json.dumps(result.contacts_found),
                'crawl_notes': result.crawl_notes,
                'crawl_timestamp': result.crawl_timestamp.isoformat()
            })
        
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved crawl results to {output_path}")
    
    def generate_crawl_report(self, results: List[CrawlResult]) -> str:
        """Generate a comprehensive crawl report."""
        report = []
        report.append("=" * 80)
        report.append("BROADWAY WEB CRAWLER REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Organizations Crawled: {len(results)}")
        report.append("")
        
        # Summary statistics
        working_sites = sum(1 for r in results if r.website_status == 'Working')
        placeholder_sites = sum(1 for r in results if r.is_placeholder)
        error_sites = sum(1 for r in results if r.website_status in ['Error', 'Connection Failed'])
        contact_found = sum(1 for r in results if r.contacts_found)
        
        report.append("SUMMARY STATISTICS:")
        report.append(f"  Working Websites: {working_sites}")
        report.append(f"  Placeholder Sites: {placeholder_sites}")
        report.append(f"  Error Sites: {error_sites}")
        report.append(f"  Sites with Contacts: {contact_found}")
        report.append("")
        
        # Detailed results
        report.append("DETAILED RESULTS:")
        for result in results:
            report.append(f"\n{result.company_name} (ID: {result.org_id}):")
            report.append(f"  Website: {result.website_url}")
            report.append(f"  Status: {result.website_status}")
            report.append(f"  Response Time: {result.response_time:.2f}s")
            report.append(f"  Robots.txt: {result.robots_txt_summary}")
            report.append(f"  Sitemap: {result.sitemap_summary}")
            
            if result.contacts_found:
                report.append(f"  Contacts Found: {len(result.contacts_found)}")
                for contact in result.contacts_found[:3]:  # Show first 3
                    report.append(f"    - {contact['type']}: {contact['value']}")
            else:
                report.append("  Contacts: None found")
            
            if result.crawl_notes:
                report.append(f"  Notes: {result.crawl_notes}")
        
        report.append("\n" + "=" * 80)
        report.append("CRAWL COMPLETE")
        
        return "\n".join(report)

    def calculate_crawl_confidence(self, contacts_found: List[Dict], contact_pages_found: List[str], 
                                 website_status: str, is_placeholder: bool) -> Tuple[int, str]:
        """
        Calculate confidence score for the crawl results.
        Returns (confidence_percentage, reasoning)
        """
        if is_placeholder or website_status != 'Working':
            return 0, "Website is placeholder or not working"
        
        if not contacts_found:
            return 15, "No contacts found yet, but will explore further pages"
        
        # Score different types of contacts
        score = 0
        max_score = 100
        
        # Email quality scoring
        emails = [c for c in contacts_found if c['type'] == 'email']
        if emails:
            for email in emails:
                if email['quality'] == 'structured':
                    score += 25  # Schema.org data is high quality
                elif email['quality'] == 'direct':
                    if not email['value'].startswith(('info@', 'contact@', 'hello@')):
                        score += 20  # Direct personal email
                    else:
                        score += 10  # Generic business email
        
        # Phone quality scoring
        phones = [c for c in contacts_found if c['type'] == 'phone']
        if phones:
            for phone in phones:
                if phone['quality'] == 'structured':
                    score += 20  # Schema.org data
                elif phone['quality'] == 'direct':
                    score += 15  # Direct phone number
        
        # Contact name quality scoring
        names = [c for c in contacts_found if c['type'] == 'contact_name']
        if names:
            for name in names:
                if name['quality'] == 'structured':
                    score += 20  # Schema.org data
                elif name['quality'] == 'direct':
                    if name.get('job_title'):
                        score += 15  # Name with job title
                    else:
                        score += 10  # Just name
        
        # Contact page discovery bonus
        if len(contact_pages_found) >= 3:
            score += 10  # Found multiple contact pages
        elif len(contact_pages_found) >= 1:
            score += 5   # Found at least one contact page
        
        # Cap the score
        score = min(score, max_score)
        
        # Determine reasoning
        if score >= 80:
            reasoning = "High confidence: Found substantial contact data"
        elif score >= 40:
            reasoning = f"Medium confidence: Found {len(contacts_found)} contacts, score {score}/100"
        else:
            reasoning = f"Low confidence: Minimal contact data found, score {score}/100"
        
        return score, reasoning
    
    def should_continue_crawling(self, confidence_score: int, contacts_found: List[Dict], 
                               pages_crawled: int) -> Tuple[bool, str]:
        """
        Determine if we should continue crawling or move to other enrichment methods.
        Returns (should_continue, reasoning)
        """
        # High confidence - we're done here
        if confidence_score >= 80:
            return False, f"High confidence achieved ({confidence_score}%), moving to next enrichment method"
        
        # Medium confidence - try a few more pages
        if confidence_score >= 40:
            if pages_crawled >= 5:  # Already tried enough pages
                return False, f"Medium confidence ({confidence_score}%) after {pages_crawled} pages, diminishing returns"
            else:
                return True, f"Medium confidence ({confidence_score}%), trying {5 - pages_crawled} more pages"
        
        # Low confidence - still try a few more pages for potential hidden contacts
        if confidence_score < 40:
            if pages_crawled >= 3:  # Try at least 3 pages before giving up
                return False, f"Low confidence ({confidence_score}%) after {pages_crawled} pages, minimal returns expected"
            else:
                return True, f"Low confidence ({confidence_score}%), but trying {3 - pages_crawled} more pages for hidden contacts"
        
        return False, "Unknown confidence level, stopping crawl"

    async def persist_business_data(self, org_id: int, business_info: Dict) -> bool:
        """Persist extracted business data to the database."""
        if not business_info:
            return True  # Nothing to persist
        
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Build the update query dynamically based on what we found
                    update_fields = []
                    update_values = []
                    
                    if 'company_phone' in business_info and business_info['company_phone']:
                        update_fields.append("company_phone = %s")
                        update_values.append(business_info['company_phone'])
                    
                    if 'street' in business_info and business_info['street']:
                        update_fields.append("street = %s")
                        update_values.append(business_info['street'])
                    
                    if 'city' in business_info and business_info['city']:
                        update_fields.append("city = %s")
                        update_values.append(business_info['city'])
                    
                    if 'state' in business_info and business_info['state']:
                        update_fields.append("state = %s")
                        update_values.append(business_info['state'])
                    
                    if 'zip' in business_info and business_info['zip']:
                        update_fields.append("zip = %s")
                        update_values.append(business_info['zip'])
                    
                    if not update_fields:
                        return True  # No fields to update
                    
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
                    
                    logger.info(f"Updated organization {org_id} with business data: {list(business_info.keys())}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error persisting business data for organization {org_id}: {e}")
            return False

    async def extract_contacts_with_js(self, url: str) -> Dict:
        """Extract contacts using JavaScript rendering for dynamic content."""
        if not PLAYWRIGHT_AVAILABLE:
            return {}
        
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                
                # Set realistic user agent
                await page.set_extra_http_headers({
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                })
                
                # Navigate and wait for content to load
                await page.goto(url, wait_until='networkidle', timeout=30000)
                
                # Wait for common contact-related elements
                await page.wait_for_timeout(2000)  # Wait for JS to execute
                
                # Look for contact forms, team sections, etc.
                contact_data = {}
                
                # Extract from contact forms
                forms = await page.query_selector_all('form')
                for form in forms:
                    form_text = await form.inner_text()
                    if any(keyword in form_text.lower() for keyword in ['contact', 'email', 'phone', 'message']):
                        # Look for email inputs
                        email_inputs = await form.query_selector_all('input[type="email"]')
                        for email_input in email_inputs:
                            placeholder = await email_input.get_attribute('placeholder')
                            if placeholder and '@' in placeholder:
                                contact_data['form_email_placeholder'] = placeholder
                
                # Extract from team/staff sections
                team_selectors = [
                    '[class*="team"]', '[class*="staff"]', '[class*="leadership"]',
                    '[class*="about"]', '[class*="contact"]', '[class*="people"]'
                ]
                
                for selector in team_selectors:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        text = await element.inner_text()
                        # Look for name patterns
                        names = re.findall(r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', text)
                        if names:
                            if 'names' not in contact_data:
                                contact_data['names'] = []
                            contact_data['names'].extend(names[:5])  # Limit to 5 names
                
                await browser.close()
                return contact_data
                
        except Exception as e:
            logger.warning(f"JavaScript rendering failed for {url}: {e}")
            return {}
    
    def extract_enhanced_contacts_from_html(self, soup: BeautifulSoup) -> Dict:
        """Enhanced contact extraction with multiple strategies."""
        contacts = {}
        
        # Remove script and style tags for cleaner text
        for script in soup(["script", "style"]):
            script.decompose()
        
        # Extract emails with better patterns
        email_patterns = [
            r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            r'[A-Za-z0-9._%+-]+\s*@\s*[A-Za-z0-9.-]+\s*\.\s*[A-Z|a-z]{2,}'
        ]
        
        all_emails = []
        for pattern in email_patterns:
            emails = re.findall(pattern, soup.get_text())
            all_emails.extend(emails)
        
        # Filter and validate emails
        valid_emails = []
        for email in all_emails:
            email = email.strip()
            if self.is_valid_email(email) and email not in valid_emails:
                valid_emails.append(email)
        
        if valid_emails:
            contacts['emails'] = valid_emails
        
        # Extract phone numbers with better patterns
        phone_patterns = [
            r'\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'\+?1[-.\s]?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})',
            r'1?[-.\s]?(800|888|877|866|855|844|833)[-.\s]?(\d{3})[-.\s]?(\d{4})'
        ]
        
        all_phones = []
        for pattern in phone_patterns:
            phones = re.findall(pattern, soup.get_text())
            for phone in phones:
                if isinstance(phone, tuple):
                    phone = ''.join(phone)
                phone = re.sub(r'[^\d\+]', '', phone)
                if self.is_valid_phone(phone) and phone not in all_phones:
                    all_phones.append(phone)
        
        if all_phones:
            contacts['phones'] = all_phones
        
        # Extract names with better patterns
        name_patterns = [
            r'\b[A-Z][a-z]+ [A-Z][a-z]+\b',
            r'\b[A-Z][a-z]+ [A-Z][a-z]+ [A-Z][a-z]+\b',  # First Middle Last
            r'\b[A-Z][a-z]+ [A-Z]\. [A-Z][a-z]+\b'  # First M. Last
        ]
        
        all_names = []
        for pattern in name_patterns:
            names = re.findall(pattern, soup.get_text())
            for name in names:
                if self.is_valid_name(name) and name not in all_names:
                    all_names.append(name)
        
        if all_names:
            contacts['names'] = all_names[:10]  # Limit to 10 names
        
        # Extract from structured data (schema.org)
        structured_contacts = self.extract_structured_contacts(soup)
        if structured_contacts:
            contacts.update(structured_contacts)
        
        return contacts

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Broadway Web Crawler')
    parser.add_argument('--workers', type=int, default=5, help='Number of worker threads (default: 5)')
    parser.add_argument('--org-ids', type=str, help='Comma-separated list of organization IDs to crawl')
    parser.add_argument('--test', action='store_true', help='Test mode - crawl first 3 organizations')
    parser.add_argument('--all', action='store_true', help='Crawl all organizations in the database')
    parser.add_argument('--output', type=str, default='crawl_results.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize crawler
    crawler = WebCrawler(max_workers=args.workers)
    
    # Determine which organizations to crawl
    org_ids = None
    if args.org_ids:
        org_ids = [int(x.strip()) for x in args.org_ids.split(',')]
    elif args.test:
        org_ids = [1, 2, 3]  # Test with first 3
    elif args.all:
        # If --all is used, org_ids will be None, meaning all organizations will be crawled
        pass 
    
    # Run crawl
    logger.info(f"Starting web crawler with {args.workers} workers")
    results = await crawler.crawl_all_sites(org_ids)
    
    # Save results
    crawler.save_results_to_csv(results, args.output)
    
    # Generate and display report
    report = crawler.generate_crawl_report(results)
    print(report)
    
    # Save report to file
    report_path = args.output.replace('.csv', '_report.txt')
    with open(report_path, 'w') as f:
        f.write(report)
    logger.info(f"Saved crawl report to {report_path}")

if __name__ == "__main__":
    asyncio.run(main())
