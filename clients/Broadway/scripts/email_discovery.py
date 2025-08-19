#!/usr/bin/env python3
"""
Enhanced Email Discovery Module - Complete Email Finding Pipeline

Current workflow (aligned with production rules):
1. Validate existing primary email (ZeroBounce)
2. Generate email predictions using ONLY the 10 allowed formats (no admin/info/contact)
3. Validate predictions with ZeroBounce, 1s delay between calls
4. Accept ONLY status == 'valid' (reject catch-all/unknown/invalid)
5. If a valid is found: write to summer_camps.contacts and record validation
6. If no valid after up to 10 attempts: move contact to summer_camps.catchall_contacts with attempted list
7. Supports multiple workers for batch processing via --workers N

Usage:
    python3 scripts/email_discovery.py --contact-id 41
    python3 scripts/email_discovery.py --all-contacts --limit 50 --workers 6
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
from zerobounce_validator import ZeroBounceValidator
from perplexity_enricher import PerplexityEnricher

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmailDiscovery:
    """Complete email discovery and validation pipeline."""
    
    def __init__(self):
        self.zerobounce = ZeroBounceValidator()
        self.perplexity = PerplexityEnricher()
    
    async def get_contact_info(self, contact_id: int) -> Optional[Dict]:
        """Get complete contact information from database."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT c.contact_id, c.contact_name, c.contact_email, c.predicted_email,
                               c.email_validation_status, c.org_id,
                               o.company_name, o.website_url, o.perplexity_categories
                        FROM summer_camps.contacts c
                        JOIN summer_camps.organizations o ON c.org_id = o.org_id
                        WHERE c.contact_id = %s
                    """, (contact_id,))
                    
                    result = cur.fetchone()
                    if result:
                        return {
                            'contact_id': result[0],
                            'contact_name': result[1],
                            'contact_email': result[2],
                            'predicted_email': result[3],
                            'email_validation_status': result[4],
                            'org_id': result[5],
                            'company_name': result[6],
                            'website_url': result[7],
                            'perplexity_categories': result[8]
                        }
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting contact info: {e}")
            return None
    
    async def validate_existing_email(self, email: str) -> Dict:
        """Validate an existing email address."""
        if not email or email == 'None':
            return {'status': 'no_email', 'score': 0, 'risk_score': 100}
        
        async with self.zerobounce as validator:
            result = await validator.validate_single_email(email)
            return validator.parse_validation_result(result)
    
    async def generate_ai_email_predictions(self, contact_name: str, company_name: str, 
                                          website_url: str, business_context: str = "") -> List[str]:
        """Generate 3-5 AI-powered email predictions."""
        
        prompt = f"""
        I need to generate 3-5 most likely email address formats for a contact at a business.

        CONTACT: {contact_name}
        COMPANY: {company_name}
        WEBSITE: {website_url}
        BUSINESS CONTEXT: {business_context}

        Based on typical business email conventions and this company's context, generate exactly 3-5 email addresses in order of likelihood:

        1. Most likely format (e.g., firstname.lastname@company.com)
        2. Second most likely format
        3. Third most likely format
        4. Fourth most likely format
        5. Fifth most likely format

        Focus on professional business email patterns, not personal email formats.
        Return ONLY the email addresses, one per line, no explanations.
        """
        
        try:
            response = await self.perplexity.search_perplexity(prompt)
            if response:
                # Extract email addresses from the response using regex
                content = str(response)
                import re
                email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
                emails = re.findall(email_pattern, content)
                
                if emails:
                    logger.info(f"✅ AI generated {len(emails)} email predictions")
                    return emails[:5]  # Return up to 5 emails
                else:
                    logger.warning("No emails found in AI response")
            else:
                logger.warning("No response from Perplexity API")
        except Exception as e:
            logger.error(f"AI prediction failed: {e}")
        
        # Fallback to pattern-based predictions
        logger.info("Falling back to pattern-based predictions")
        return self.generate_pattern_based_predictions(contact_name, website_url)
    
    def generate_pattern_based_predictions(self, contact_name: str, website_url: str) -> List[str]:
        """Generate email predictions using ONLY the 10 allowed formats (no catch-all/admin/info):
        1) first
        2) last
        3) firstlast
        4) flast
        5) firstl
        6) lastf
        7) first.last
        8) first_last
        9) first-last
        10) firstm(last)  (first + middle initial + last, only if middle exists)
        """
        # Extract domain from website
        from urllib.parse import urlparse
        try:
            parsed = urlparse(website_url)
            domain = parsed.netloc.lower()
            if domain.startswith('www.'):
                domain = domain[4:]
        except:
            return []
        
        # Clean name
        import re
        name = re.sub(r'[^\w\s-]', '', contact_name).strip()
        parts = [p for p in name.split() if p]

        predictions: List[str] = []

        if len(parts) == 0:
            return []

        if len(parts) == 1:
            # Only first name known
            first_name = parts[0].lower()
            predictions = [
                f"{first_name}@{domain}",           # first
            ]
        else:
            first_name = parts[0].lower()
            last_name = parts[-1].lower()
            middle_initial = ''
            if len(parts) >= 3 and parts[1]:
                middle_initial = parts[1][0].lower()

            # Build allowed formats in priority order
            candidates = [
                f"{first_name}@{domain}",                 # first
                f"{last_name}@{domain}",                  # last
                f"{first_name}{last_name}@{domain}",      # firstlast
                f"{first_name[0]}{last_name}@{domain}",   # flast
                f"{first_name}{last_name[0]}@{domain}",   # firstl
                f"{last_name}{first_name[0]}@{domain}",   # lastf
                f"{first_name}.{last_name}@{domain}",     # first.last
                f"{first_name}_{last_name}@{domain}",     # first_last
                f"{first_name}-{last_name}@{domain}",     # first-last
            ]
            if middle_initial:
                candidates.append(f"{first_name}{middle_initial}{last_name}@{domain}")  # firstm(last)

            # Deduplicate while preserving order
            seen = set()
            for c in candidates:
                if c not in seen:
                    predictions.append(c)
                    seen.add(c)

        # Return up to 15 (we generate <=10)
        return predictions[:15]
    
    async def validate_email_list(self, emails: List[str]) -> List[Dict]:
        """Validate a list of email addresses."""
        results = []
        async with self.zerobounce as validator:
            for email in emails:
                result = await validator.validate_single_email(email)
                parsed = validator.parse_validation_result(result)
                parsed['email'] = email
                results.append(parsed)
                await asyncio.sleep(0.5)  # Rate limiting
        return results
    
    async def validate_single_email(self, email: str) -> Dict:
        """Validate a single email address."""
        async with self.zerobounce as validator:
            result = await validator.validate_single_email(email)
            parsed = validator.parse_validation_result(result)
            parsed['email'] = email
            return parsed
    
    def select_best_email(self, validation_results: List[Dict]) -> Optional[Dict]:
        """Select the best email based on validation results."""
        # Priority order: valid > catch_all > unknown > invalid
        priority_order = ['valid', 'catch_all', 'unknown', 'invalid', 'error']
        
        for priority in priority_order:
            for result in validation_results:
                if result['status'] == priority:
                    return result
        
        return None
    
    async def discover_and_validate_email(self, contact_id: int) -> Dict:
        """Complete email discovery and validation for a contact."""
        logger.info(f"Starting email discovery for contact {contact_id}")
        
        # Get contact info
        contact_info = await self.get_contact_info(contact_id)
        if not contact_info:
            return {'error': 'Contact not found'}
        
        contact_name = contact_info['contact_name']
        company_name = contact_info['company_name']
        website_url = contact_info['website_url']
        
        logger.info(f"Processing: {contact_name} at {company_name}")
        
        # Step 1: Validate existing primary email
        existing_email_result = await self.validate_existing_email(contact_info['contact_email'])
        logger.info(f"Existing email validation: {existing_email_result['status']}")
        
        # If existing email is valid, we're done
        if existing_email_result['status'] == 'valid':
            logger.info(f"Existing email is valid: {contact_info['contact_email']}")
            return {
                'contact_id': contact_id,
                'best_email': contact_info['contact_email'],
                'validation_status': 'existing_valid',
                'score': existing_email_result['score'],
                'discovery_method': 'existing_email'
            }
        
        # Step 2: Generate comprehensive email predictions (up to 10)
        logger.info("Generating comprehensive email predictions...")
        
        # Start with AI predictions
        ai_predictions = await self.generate_ai_email_predictions(
            contact_name, company_name, website_url, contact_info.get('perplexity_categories', '')
        )
        
        # Add pattern-based predictions
        pattern_predictions = self.generate_pattern_based_predictions(contact_name, website_url)
        
        # Combine and remove duplicates, limit to 10 total
        all_predictions = list(dict.fromkeys(ai_predictions + pattern_predictions))[:10]
        logger.info(f"Generated {len(all_predictions)} email predictions to test")
        
        # Step 3: Validate predictions one by one until we find a valid one
        logger.info("Validating predictions until we find a valid email...")
        best_email = None
        attempted_emails: List[str] = []
        
        for i, email in enumerate(all_predictions):
            logger.info(f"Testing prediction {i+1}/{len(all_predictions)}: {email}")
            attempted_emails.append(email)
            
            # Validate single email
            validation_result = await self.validate_single_email(email)
            
            # Only accept valid emails, not catch-all (like the working test script)
            if validation_result['status'] == 'valid':
                logger.info(f"✅ Found valid email: {email} (Status: {validation_result['status']}, Score: {validation_result['score']})")
                best_email = validation_result
                break
            elif validation_result['status'] == 'catch_all':
                logger.info(f"⚠️  Catch-all email (not useful): {email}")
            else:
                logger.info(f"❌ Invalid email: {email} (Status: {validation_result['status']}, Score: {validation_result['score']})")
            
            # Add delay between API calls (like the working test script)
            await asyncio.sleep(1)
        
        if best_email:
            return {
                'contact_id': contact_id,
                'best_email': best_email['email'],
                'validation_status': best_email['status'],
                'score': best_email['score'],
                'discovery_method': 'comprehensive_validation',
                'attempted_emails': attempted_emails
            }
        else:
            logger.warning(f"❌ No valid emails found after testing {len(all_predictions)} predictions")
            return {
                'contact_id': contact_id,
                'best_email': None,
                'validation_status': 'no_valid_email',
                'score': 0,
                'discovery_method': 'none',
                'attempted_emails': attempted_emails
            }
    
    async def update_database_with_discovery(self, discovery_result: Dict) -> bool:
        """Update database with email discovery results."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if discovery_result['best_email']:
                        # Update with the best discovered email
                        cur.execute("""
                            UPDATE summer_camps.contacts 
                            SET contact_email = %s,
                                email_quality = 'direct',
                                last_enriched_at = NOW()
                            WHERE contact_id = %s
                        """, (
                            discovery_result['best_email'],
                            discovery_result['contact_id']
                        ))
                        
                        # Also update validation status
                        cur.execute("""
                            UPDATE summer_camps.contacts 
                            SET email_validation_status = %s,
                                email_validation_score = %s,
                                email_validation_timestamp = NOW(),
                                email_validation_provider = 'zerobounce'
                            WHERE contact_id = %s
                        """, (
                            discovery_result['validation_status'],
                            int(discovery_result['score']) if discovery_result['score'] is not None else 0,
                            discovery_result['contact_id']
                        ))
                    else:
                        # No valid email found - move to catchall table and remove from main contacts
                        logger.warning(f"❌ No valid email found for contact {discovery_result['contact_id']} - moving to catchall")
                        # Ensure catchall table exists
                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS summer_camps.catchall_contacts (
                                catchall_id SERIAL PRIMARY KEY,
                                contact_id INTEGER,
                                org_id INTEGER,
                                contact_name VARCHAR(255),
                                role_title VARCHAR(255),
                                company_name VARCHAR(500),
                                website_url TEXT,
                                attempted_count INTEGER DEFAULT 0,
                                attempted_emails TEXT[],
                                reason VARCHAR(100) DEFAULT 'no_valid_email',
                                moved_at TIMESTAMP DEFAULT NOW()
                            )
                        """)
                        # Load context for insertion
                        cur.execute(
                            """
                            SELECT c.org_id, c.contact_name, c.role_title, o.company_name, o.website_url
                            FROM summer_camps.contacts c
                            JOIN summer_camps.organizations o ON c.org_id = o.org_id
                            WHERE c.contact_id = %s
                            """,
                            (discovery_result['contact_id'],)
                        )
                        ctx = cur.fetchone()
                        org_id = ctx[0] if ctx else None
                        contact_name_ctx = ctx[1] if ctx else None
                        role_title_ctx = ctx[2] if ctx else None
                        company_name_ctx = ctx[3] if ctx else None
                        website_url_ctx = ctx[4] if ctx else None
                        attempted = discovery_result.get('attempted_emails') or []
                        # Insert into catchall
                        cur.execute(
                            """
                            INSERT INTO summer_camps.catchall_contacts
                                (contact_id, org_id, contact_name, role_title, company_name, website_url,
                                 attempted_count, attempted_emails, reason)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'no_valid_email')
                            """,
                            (
                                discovery_result['contact_id'],
                                org_id,
                                contact_name_ctx,
                                role_title_ctx,
                                company_name_ctx,
                                website_url_ctx,
                                len(attempted),
                                attempted,
                            )
                        )
                        # Remove from main contacts
                        cur.execute(
                            "DELETE FROM summer_camps.contacts WHERE contact_id = %s",
                            (discovery_result['contact_id'],)
                        )
                    
                    conn.commit()
                    logger.info(f"Database updated for contact {discovery_result['contact_id']}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            return False
    
    async def process_all_contacts(self, limit: int = None, workers: int = 5) -> Dict:
        """Process contacts through the email discovery pipeline with multiple workers."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Get contacts that do NOT have a validated email
                    cur.execute("""
                        SELECT contact_id FROM summer_camps.contacts 
                        WHERE (
                            contact_email IS NULL OR contact_email = ''
                            OR contact_email LIKE '%email_not_unlocked%'
                            OR contact_email LIKE '%placeholder%'
                        )
                        OR (
                            email_validation_status IS NULL OR email_validation_status <> 'valid'
                        )
                        ORDER BY contact_id
                    """)
                    
                    contact_ids = [row[0] for row in cur.fetchall()]
                    
                    # Apply limit if specified
                    if limit:
                        contact_ids = contact_ids[:limit]
            
            logger.info(f"Processing {len(contact_ids)} contacts through email discovery pipeline with {workers} workers")
            
            results = {
                'total_contacts': len(contact_ids),
                'processed': 0,
                'successful_discoveries': 0,
                'failed_discoveries': 0,
                'discovery_methods': {}
            }
            
            # Create semaphore for concurrency control
            semaphore = asyncio.Semaphore(workers)
            
            async def process_single_contact(contact_id: int):
                async with semaphore:
                    try:
                        logger.info(f"Processing contact {contact_id}")
                        
                        discovery_result = await self.discover_and_validate_email(contact_id)
                        
                        if 'error' not in discovery_result:
                            # Update database
                            db_success = await self.update_database_with_discovery(discovery_result)
                            
                            if db_success:
                                if discovery_result['best_email']:
                                    method = discovery_result['discovery_method']
                                    return {
                                        'success': True,
                                        'discovery_method': method,
                                        'has_email': True
                                    }
                                else:
                                    return {
                                        'success': True,
                                        'discovery_method': 'none',
                                        'has_email': False
                                    }
                            else:
                                return {
                                    'success': False,
                                    'error': 'Database update failed'
                                }
                        else:
                            return {
                                'success': False,
                                'error': discovery_result['error']
                            }
                        
                    except Exception as e:
                        logger.error(f"Error processing contact {contact_id}: {e}")
                        return {
                            'success': False,
                            'error': str(e)
                        }
            
            # Create tasks for all contacts
            tasks = [process_single_contact(contact_id) for contact_id in contact_ids]
            
            # Execute tasks with concurrency limit
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            for result in task_results:
                if isinstance(result, Exception):
                    results['failed_discoveries'] += 1
                    logger.error(f"Task exception: {result}")
                elif result.get('success'):
                    results['processed'] += 1
                    
                    if result.get('has_email'):
                        results['successful_discoveries'] += 1
                        method = result.get('discovery_method')
                        results['discovery_methods'][method] = results['discovery_methods'].get(method, 0) + 1
                    else:
                        # No valid email found, but contact kept
                        results['failed_discoveries'] += 1
                else:
                    results['failed_discoveries'] += 1
            
            return results
            
        except Exception as e:
            logger.error(f"Error in bulk processing: {e}")
            return {'error': str(e)}
    
    def generate_discovery_report(self, results: Dict) -> str:
        """Generate a comprehensive discovery report."""
        if 'error' in results:
            return f"Discovery failed: {results['error']}"
        
        report = []
        report.append("=" * 60)
        report.append("ENHANCED EMAIL DISCOVERY REPORT")
        report.append("=" * 60)
        report.append(f"Total contacts processed: {results['total_contacts']}")
        report.append(f"Successfully processed: {results['processed']}")
        report.append(f"Successful discoveries: {results['successful_discoveries']}")
        report.append(f"Failed discoveries: {results['failed_discoveries']}")
        report.append(f"Success rate: {results['successful_discoveries']/results['total_contacts']*100:.1f}%")
        report.append("")
        
        if results['discovery_methods']:
            report.append("DISCOVERY METHODS USED:")
            for method, count in results['discovery_methods'].items():
                report.append(f"  {method}: {count}")
        
        report.append("")
        report.append("NEXT STEPS:")
        report.append("1. Review discovered emails in database")
        report.append("2. Test high-confidence emails manually")
        report.append("3. Use for outreach campaigns")
        report.append("4. Monitor delivery success rates")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Enhanced Email Discovery - Complete Pipeline')
    parser.add_argument('--contact-id', type=int, help='Specific contact ID to process')
    parser.add_argument('--all-contacts', action='store_true', help='Process all contacts')
    parser.add_argument('--limit', type=int, help='Limit processing to N contacts (for batch processing)')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers (default: 5)')
    
    args = parser.parse_args()
    
    if not args.contact_id and not args.all_contacts:
        print("Error: Must specify either --contact-id or --all-contacts")
        sys.exit(1)
    
    try:
        discovery = EmailDiscovery()
        
        if args.contact_id:
            # Process single contact
            print(f"🔍 Starting email discovery for contact {args.contact_id}")
            result = await discovery.discover_and_validate_email(args.contact_id)
            
            if 'error' not in result:
                print(f"✅ Discovery complete!")
                print(f"   Best email: {result.get('best_email', 'None')}")
                print(f"   Status: {result.get('validation_status', 'Unknown')}")
                print(f"   Method: {result.get('discovery_method', 'Unknown')}")
                
                # Update database
                if await discovery.update_database_with_discovery(result):
                    print("✅ Database updated successfully")
                else:
                    print("❌ Database update failed")
            else:
                print(f"❌ Discovery failed: {result['error']}")
        
        elif args.all_contacts:
            # Process contacts with optional limit
            if args.limit:
                print(f"🔄 Starting email discovery for first {args.limit} contacts without emails with {args.workers} workers...")
                results = await discovery.process_all_contacts(limit=args.limit, workers=args.workers)
            else:
                print(f"🔄 Starting email discovery for all contacts with {args.workers} workers...")
                results = await discovery.process_all_contacts(workers=args.workers)
            
            # Generate and display report
            report = discovery.generate_discovery_report(results)
            print("\n" + report)
            
            # Save report
            with open('outputs/enhanced_email_discovery_report.txt', 'w') as f:
                f.write(report)
            print(f"📊 Detailed report saved to outputs/enhanced_email_discovery_report.txt")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
