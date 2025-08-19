#!/usr/bin/env python3
"""
Email Validator Module - ZeroBounce Integration

This module validates email addresses using the ZeroBounce API and stores
validation results in the database.

Usage:
    python3 scripts/email_validator.py --email "test@example.com"
    python3 scripts/email_validator.py --contact-id 41
    python3 scripts/email_validator.py --all-predicted
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Dict, List, Optional
from datetime import datetime
import aiohttp
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_connection import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ZeroBounceValidator:
    """Validates emails using ZeroBounce API."""
    
    def __init__(self):
        self.api_key = os.getenv('BROADWAY_ZEROBOUNCE_API_KEY')
        if not self.api_key:
            raise ValueError("BROADWAY_ZEROBOUNCE_API_KEY environment variable not set")
        
        self.base_url = "https://api.zerobounce.net/v2"
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def validate_single_email(self, email: str) -> Dict:
        """Validate a single email address."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        url = f"{self.base_url}/validate"
        params = {
            'api_key': self.api_key,
            'email': email,
            'ip_address': ''  # Optional: IP address for additional validation
        }
        
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"Validation result for {email}: {result.get('status', 'unknown')}")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"ZeroBounce API error: {response.status} - {error_text}")
                    return {'error': f"API error: {response.status}"}
                    
        except Exception as e:
            logger.error(f"Error validating {email}: {e}")
            return {'error': str(e)}
    
    async def validate_bulk_emails(self, emails: List[str]) -> List[Dict]:
        """Validate multiple email addresses in bulk."""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        url = f"{self.base_url}/bulk-validate"
        data = {
            'api_key': self.api_key,
            'emails': emails
        }
        
        try:
            async with self.session.post(url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"Bulk validation completed for {len(emails)} emails")
                    return result
                else:
                    error_text = await response.text()
                    logger.error(f"ZeroBounce bulk API error: {response.status} - {error_text}")
                    return [{'error': f"API error: {response.status}"} for _ in emails]
                    
        except Exception as e:
            logger.error(f"Error in bulk validation: {e}")
            return [{'error': str(e)} for _ in emails]
    
    def parse_validation_result(self, result: Dict) -> Dict:
        """Parse and standardize validation result."""
        if 'error' in result:
            return {
                'status': 'error',
                'score': 0,
                'risk_score': 100,
                'details': result
            }
        
        # Map ZeroBounce status to our standard format
        status_mapping = {
            'valid': 'valid',
            'invalid': 'invalid',
            'catch-all': 'catch_all',
            'disposable': 'disposable',
            'unknown': 'unknown',
            'spamtrap': 'spamtrap',
            'abuse': 'abuse',
            'dont_send': 'dont_send'
        }
        
        status = status_mapping.get(result.get('status', 'unknown'), 'unknown')
        score = result.get('score', 0)
        risk_score = result.get('risk_score', 100)
        
        return {
            'status': status,
            'score': score,
            'risk_score': risk_score,
            'details': result
        }

class EmailValidator:
    """Main email validation orchestrator."""
    
    def __init__(self):
        self.zerobounce = ZeroBounceValidator()
    
    async def validate_contact_email(self, contact_id: int) -> bool:
        """Validate email for a specific contact."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Get contact email
                    cur.execute("""
                        SELECT contact_email, predicted_email, contact_name 
                        FROM summer_camps.contacts 
                        WHERE contact_id = %s
                    """, (contact_id,))
                    
                    result = cur.fetchone()
                    if not result:
                        logger.error(f"Contact {contact_id} not found")
                        return False
                    
                    contact_email, predicted_email, contact_name = result
                    
                    # Use predicted email if contact email is missing
                    email_to_validate = contact_email if contact_email and contact_email != 'None' else predicted_email
                    
                    if not email_to_validate or email_to_validate == 'None':
                        logger.warning(f"No email to validate for contact {contact_id}")
                        return False
                    
                    logger.info(f"Validating email for {contact_name}: {email_to_validate}")
                    
                    # Validate email
                    async with self.zerobounce as validator:
                        validation_result = await validator.validate_single_email(email_to_validate)
                    
                    # Parse result
                    parsed_result = validator.parse_validation_result(validation_result)
                    
                    # Update database
                    cur.execute("""
                        UPDATE summer_camps.contacts 
                        SET email_validation_status = %s,
                            email_validation_score = %s,
                            email_validation_risk_score = %s,
                            email_validation_timestamp = NOW(),
                            email_validation_provider = 'zerobounce',
                            email_validation_details = %s
                        WHERE contact_id = %s
                    """, (
                        parsed_result['status'],
                        parsed_result['score'],
                        parsed_result['risk_score'],
                        json.dumps(parsed_result['details']),
                        contact_id
                    ))
                    
                    conn.commit()
                    logger.info(f"Updated contact {contact_id} with validation results: {parsed_result['status']}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error validating contact {contact_id}: {e}")
            return False
    
    async def validate_all_predicted_emails(self) -> Dict:
        """Validate all contacts with predicted emails."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Get all contacts with predicted emails
                    cur.execute("""
                        SELECT contact_id, predicted_email, contact_name 
                        FROM summer_camps.contacts 
                        WHERE predicted_email IS NOT NULL 
                        AND predicted_email != ''
                        ORDER BY contact_id
                    """)
                    
                    contacts = cur.fetchall()
                    logger.info(f"Found {len(contacts)} contacts with predicted emails to validate")
                    
                    results = {
                        'total': len(contacts),
                        'validated': 0,
                        'errors': 0,
                        'status_counts': {}
                    }
                    
                    # Validate each contact
                    async with self.zerobounce as validator:
                        for contact_id, predicted_email, contact_name in contacts:
                            try:
                                logger.info(f"Validating {contact_name}: {predicted_email}")
                                
                                validation_result = await validator.validate_single_email(predicted_email)
                                parsed_result = validator.parse_validation_result(validation_result)
                                
                                # Update database
                                cur.execute("""
                                    UPDATE summer_camps.contacts 
                                    SET email_validation_status = %s,
                                        email_validation_score = %s,
                                        email_validation_risk_score = %s,
                                        email_validation_timestamp = NOW(),
                                        email_validation_provider = 'zerobounce',
                                        email_validation_details = %s
                                    WHERE contact_id = %s
                                """, (
                                    parsed_result['status'],
                                    parsed_result['score'],
                                    parsed_result['risk_score'],
                                    json.dumps(parsed_result['details']),
                                    contact_id
                                ))
                                
                                results['validated'] += 1
                                results['status_counts'][parsed_result['status']] = results['status_counts'].get(parsed_result['status'], 0) + 1
                                
                                # Small delay to avoid rate limiting
                                await asyncio.sleep(0.5)
                                
                            except Exception as e:
                                logger.error(f"Error validating contact {contact_id}: {e}")
                                results['errors'] += 1
                    
                    conn.commit()
                    logger.info(f"Validation complete: {results['validated']} validated, {results['errors']} errors")
                    return results
                    
        except Exception as e:
            logger.error(f"Error in bulk validation: {e}")
            return {'error': str(e)}
    
    def generate_validation_report(self, results: Dict) -> str:
        """Generate a validation summary report."""
        if 'error' in results:
            return f"Validation failed: {results['error']}"
        
        report = []
        report.append("=" * 60)
        report.append("EMAIL VALIDATION REPORT - ZEROBOUNCE")
        report.append("=" * 60)
        report.append(f"Total contacts processed: {results['total']}")
        report.append(f"Successfully validated: {results['validated']}")
        report.append(f"Errors encountered: {results['errors']}")
        report.append("")
        
        if results['status_counts']:
            report.append("VALIDATION STATUS BREAKDOWN:")
            for status, count in results['status_counts'].items():
                report.append(f"  {status}: {count}")
        
        report.append("")
        report.append("NEXT STEPS:")
        report.append("1. Review 'valid' emails for outreach")
        report.append("2. Investigate 'catch_all' emails manually")
        report.append("3. Avoid 'disposable' and 'spamtrap' emails")
        report.append("4. Update database with confirmed working emails")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Email Validator - ZeroBounce Integration')
    parser.add_argument('--email', type=str, help='Single email to validate')
    parser.add_argument('--contact-id', type=int, help='Contact ID to validate')
    parser.add_argument('--all-predicted', action='store_true', help='Validate all contacts with predicted emails')
    
    args = parser.parse_args()
    
    if not any([args.email, args.contact_id, args.all_predicted]):
        print("Error: Must specify --email, --contact-id, or --all-predicted")
        sys.exit(1)
    
    try:
        validator = EmailValidator()
        
        if args.email:
            # Validate single email
            print(f"Validating email: {args.email}")
            async with validator.zerobounce as zb:
                result = await zb.validate_single_email(args.email)
                parsed = zb.parse_validation_result(result)
                print(f"Result: {parsed['status']} (Score: {parsed['score']}, Risk: {parsed['risk_score']})")
                print(f"Details: {json.dumps(parsed['details'], indent=2)}")
        
        elif args.contact_id:
            # Validate specific contact
            print(f"Validating contact ID: {args.contact_id}")
            success = await validator.validate_contact_email(args.contact_id)
            if success:
                print("✅ Contact validated and database updated")
            else:
                print("❌ Contact validation failed")
        
        elif args.all_predicted:
            # Validate all predicted emails
            print("🔄 Validating all contacts with predicted emails...")
            results = await validator.validate_all_predicted_emails()
            
            # Generate and display report
            report = validator.generate_validation_report(results)
            print("\n" + report)
            
            # Save report
            with open('outputs/email_validation_report.txt', 'w') as f:
                f.write(report)
            print(f"📊 Detailed report saved to outputs/email_validation_report.txt")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
