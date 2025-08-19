#!/usr/bin/env python3
"""
Email Predictor Module - Last Ditch Effort for Email Discovery

This module attempts to predict email addresses for contacts missing them using:
1. Pattern-based prediction (common email formats)
2. AI-powered prediction via Perplexity API
3. Business context analysis

Usage:
    python3 scripts/email_predictor.py --org-ids "11,12,13" --output "outputs/email_predictions.csv"
    python3 scripts/email_predictor.py --all-contacts --output "outputs/all_email_predictions.csv"
"""

import asyncio
import argparse
import csv
import logging
import os
import re
import sys
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse
import pandas as pd

# Add the scripts directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_connection import get_db_connection
from perplexity_enricher import PerplexityEnricher

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EmailPredictor:
    """Predicts email addresses using pattern matching and AI analysis."""
    
    def __init__(self):
        self.perplexity = PerplexityEnricher()
        self.common_domains = {
            'gmail.com': 'personal',
            'yahoo.com': 'personal', 
            'hotmail.com': 'personal',
            'outlook.com': 'personal'
        }
    
    def extract_company_domain(self, website_url: str) -> Optional[str]:
        """Extract company domain from website URL."""
        if not website_url or pd.isna(website_url):
            return None
        
        try:
            parsed = urlparse(website_url)
            domain = parsed.netloc.lower()
            
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            # Skip personal email domains
            if domain in self.common_domains:
                return None
                
            return domain
        except Exception as e:
            logger.warning(f"Could not parse URL {website_url}: {e}")
            return None
    
    def clean_name_for_email(self, name: str) -> Tuple[str, str]:
        """Clean and split name into first and last name for email generation."""
        if not name or pd.isna(name):
            return "", ""
        
        # Clean the name
        name = str(name).strip()
        name = re.sub(r'[^\w\s]', '', name)  # Remove special characters
        name = re.sub(r'\s+', ' ', name)      # Normalize whitespace
        
        # Split into parts
        parts = name.split()
        
        if len(parts) == 1:
            return parts[0].lower(), ""
        elif len(parts) >= 2:
            return parts[0].lower(), parts[-1].lower()
        else:
            return "", ""
    
    def generate_pattern_emails(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """Generate common email patterns for a given name and domain."""
        if not domain or not first_name:
            return []
        
        emails = []
        
        # Common business email patterns
        patterns = [
            f"{first_name}.{last_name}@{domain}",
            f"{first_name}{last_name}@{domain}",
            f"{first_name[0]}.{last_name}@{domain}",
            f"{first_name}@{domain}",
            f"{last_name}.{first_name}@{domain}",
            f"{first_name}_{last_name}@{domain}",
            f"{first_name}-{last_name}@{domain}"
        ]
        
        # Add patterns only if we have both names
        if last_name:
            emails.extend(patterns)
        else:
            # Single name patterns
            emails.append(f"{first_name}@{domain}")
        
        return emails
    
    def predict_email_patterns(self, contact_name: str, website_url: str) -> List[str]:
        """Predict emails using pattern matching."""
        domain = self.extract_company_domain(website_url)
        if not domain:
            return []
        
        first_name, last_name = self.clean_name_for_email(contact_name)
        if not first_name:
            return []
        
        return self.generate_pattern_emails(first_name, last_name, domain)
    
    async def predict_email_ai(self, contact_name: str, company_name: str, website_url: str, 
                              business_context: str = "") -> Dict:
        """Use Perplexity AI to predict email addresses."""
        
        prompt = f"""
        I need to predict the most likely email address format for a contact at a business.

        CONTACT: {contact_name}
        COMPANY: {company_name}
        WEBSITE: {website_url}
        BUSINESS CONTEXT: {business_context}

        Based on typical business email conventions and this company's context, predict:
        1. The most likely email format (e.g., firstname.lastname@company.com)
        2. Alternative email formats that might be used
        3. Confidence level for each prediction
        4. Reasoning based on industry standards

        Focus on professional business email patterns, not personal email formats.
        Return your analysis in a clear, structured format.
        """
        
        try:
            response = await self.perplexity.search_perplexity(prompt)
            if response and response.get('leadership_contacts'):
                # Extract the response content
                ai_content = str(response)
                return {
                    'ai_prediction': ai_content,
                    'confidence': 'high' if 'likely' in ai_content.lower() else 'medium'
                }
        except Exception as e:
            logger.error(f"AI prediction failed for {contact_name}: {e}")
        
        return {'ai_prediction': None, 'confidence': 'low'}
    
    def get_contacts_missing_emails(self, org_ids: Optional[List[int]] = None) -> List[Dict]:
        """Get contacts missing email addresses."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if org_ids:
                        # Get contacts for specific organizations
                        placeholders = ','.join(['%s'] * len(org_ids))
                        query = f"""
                            SELECT c.contact_id, c.contact_name, c.org_id, 
                                   o.company_name, o.website_url, o.perplexity_categories
                            FROM summer_camps.contacts c
                            JOIN summer_camps.organizations o ON c.org_id = o.org_id
                            WHERE c.org_id IN ({placeholders})
                            AND (c.contact_email IS NULL OR c.contact_email = '' OR c.contact_email = 'None')
                            AND c.contact_name IS NOT NULL AND c.contact_name != ''
                            ORDER BY c.org_id, c.contact_name
                        """
                        cur.execute(query, org_ids)
                    else:
                        # Get all contacts missing emails
                        query = """
                            SELECT c.contact_id, c.contact_name, c.org_id, 
                                   o.company_name, o.website_url, o.perplexity_categories
                            FROM summer_camps.contacts c
                            JOIN summer_camps.organizations o ON c.org_id = o.org_id
                            WHERE (c.contact_email IS NULL OR c.contact_email = '' OR c.contact_email = 'None')
                            AND c.contact_name IS NOT NULL AND c.contact_name != ''
                            ORDER BY c.org_id, c.contact_name
                        """
                        cur.execute(query)
                    
                    contacts = []
                    for row in cur.fetchall():
                        contacts.append({
                            'contact_id': row[0],
                            'contact_name': row[1],
                            'org_id': row[2],
                            'company_name': row[3],
                            'website_url': row[4],
                            'perplexity_categories': row[5]
                        })
                    
                    return contacts
                    
        except Exception as e:
            logger.error(f"Error getting contacts missing emails: {e}")
            return []
    
    async def predict_emails_for_contacts(self, contacts: List[Dict]) -> List[Dict]:
        """Predict emails for a list of contacts."""
        results = []
        
        for contact in contacts:
            logger.info(f"Predicting emails for {contact['contact_name']} at {contact['company_name']}")
            
            # Pattern-based prediction
            pattern_emails = self.predict_email_patterns(
                contact['contact_name'], 
                contact['website_url']
            )
            
            # AI-based prediction
            ai_result = await self.predict_email_ai(
                contact['contact_name'],
                contact['company_name'], 
                contact['website_url'],
                contact.get('perplexity_categories', '')
            )
            
            # Combine results
            result = {
                'contact_id': contact['contact_id'],
                'org_id': contact['org_id'],
                'contact_name': contact['contact_name'],
                'company_name': contact['company_name'],
                'website_url': contact['website_url'],
                'pattern_emails': pattern_emails,
                'ai_prediction': ai_result.get('ai_prediction'),
                'ai_confidence': ai_result.get('confidence'),
                'best_prediction': pattern_emails[0] if pattern_emails else None,
                'total_predictions': len(pattern_emails)
            }
            
            results.append(result)
            
            # Small delay to avoid overwhelming APIs
            await asyncio.sleep(0.5)
        
        return results
    
    def save_predictions_to_csv(self, predictions: List[Dict], output_path: str) -> bool:
        """Save email predictions to CSV file."""
        try:
            # Flatten the predictions for CSV
            csv_data = []
            for pred in predictions:
                # Create a row for each pattern email
                if pred['pattern_emails']:
                    for i, email in enumerate(pred['pattern_emails']):
                        csv_data.append({
                            'contact_id': pred['contact_id'],
                            'org_id': pred['org_id'],
                            'contact_name': pred['contact_name'],
                            'company_name': pred['company_name'],
                            'website_url': pred['website_url'],
                            'prediction_type': 'pattern',
                            'predicted_email': email,
                            'confidence': 'high' if i == 0 else 'medium',
                            'ai_prediction': pred['ai_prediction'],
                            'ai_confidence': pred['ai_confidence']
                        })
                else:
                    # No pattern emails, just AI prediction
                    csv_data.append({
                        'contact_id': pred['contact_id'],
                        'org_id': pred['org_id'],
                        'contact_name': pred['contact_name'],
                        'company_name': pred['company_name'],
                        'website_url': pred['website_url'],
                        'prediction_type': 'ai_only',
                        'predicted_email': '',
                        'confidence': 'low',
                        'ai_prediction': pred['ai_prediction'],
                        'ai_confidence': pred['ai_confidence']
                    })
            
            # Save to CSV
            df = pd.DataFrame(csv_data)
            df.to_csv(output_path, index=False)
            
            logger.info(f"Saved {len(csv_data)} email predictions to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving predictions to CSV: {e}")
            return False
    
    def update_database_with_predictions(self, predictions: List[Dict]) -> bool:
        """Update the database with the best email predictions."""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    updated_count = 0
                    
                    for pred in predictions:
                        if pred['best_prediction']:
                            # Update the contact with the best predicted email
                            cur.execute("""
                                UPDATE summer_camps.contacts 
                                SET predicted_email = %s, 
                                    email_prediction_confidence = %s,
                                    email_prediction_timestamp = NOW(),
                                    email_prediction_method = 'pattern_based'
                                WHERE contact_id = %s
                            """, (
                                pred['best_prediction'],
                                'high',
                                pred['contact_id']
                            ))
                            
                            if cur.rowcount > 0:
                                updated_count += 1
                                logger.info(f"Updated contact {pred['contact_id']} with predicted email: {pred['best_prediction']}")
                    
                    conn.commit()
                    logger.info(f"Successfully updated {updated_count} contacts with predicted emails")
                    return True
                    
        except Exception as e:
            logger.error(f"Error updating database with predictions: {e}")
            return False
    
    def generate_prediction_report(self, predictions: List[Dict]) -> str:
        """Generate a summary report of email predictions."""
        total_contacts = len(predictions)
        contacts_with_patterns = sum(1 for p in predictions if p['pattern_emails'])
        total_predictions = sum(p['total_predictions'] for p in predictions)
        
        report = []
        report.append("=" * 60)
        report.append("EMAIL PREDICTION REPORT - LAST DITCH EFFORT")
        report.append("=" * 60)
        report.append(f"Contacts processed: {total_contacts}")
        report.append(f"Contacts with pattern predictions: {contacts_with_patterns}")
        report.append(f"Total email predictions generated: {total_predictions}")
        report.append(f"Average predictions per contact: {total_predictions/total_contacts:.1f}")
        report.append("")
        
        report.append("PREDICTION BREAKDOWN:")
        for pred in predictions:
            report.append(f"  {pred['contact_name']} at {pred['company_name']}")
            if pred['pattern_emails']:
                report.append(f"    Pattern emails: {len(pred['pattern_emails'])}")
                report.append(f"    Best guess: {pred['best_prediction']}")
            else:
                report.append(f"    No pattern emails (AI only)")
            report.append(f"    AI confidence: {pred['ai_confidence']}")
            report.append("")
        
        report.append("NEXT STEPS:")
        report.append("1. Review pattern-based predictions (highest confidence)")
        report.append("2. Test top predictions manually")
        report.append("3. Use AI insights for alternative formats")
        report.append("4. Update database with confirmed emails")
        
        return "\n".join(report)

async def main():
    """Main function for command line usage."""
    parser = argparse.ArgumentParser(description='Email Predictor - Last Ditch Effort Module')
    parser.add_argument('--org-ids', type=str, help='Comma-separated list of organization IDs')
    parser.add_argument('--all-contacts', action='store_true', help='Process all contacts missing emails')
    parser.add_argument('--output', type=str, default='outputs/email_predictions.csv', 
                       help='Output CSV file path')
    
    args = parser.parse_args()
    
    if not args.org_ids and not args.all_contacts:
        print("Error: Must specify either --org-ids or --all-contacts")
        sys.exit(1)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    
    predictor = EmailPredictor()
    
    # Get contacts to process
    if args.org_ids:
        org_ids = [int(x.strip()) for x in args.org_ids.split(',')]
        contacts = predictor.get_contacts_missing_emails(org_ids)
        print(f"Found {len(contacts)} contacts missing emails in organizations {org_ids}")
    else:
        contacts = predictor.get_contacts_missing_emails()
        print(f"Found {len(contacts)} contacts missing emails across all organizations")
    
    if not contacts:
        print("No contacts found missing emails. All set!")
        return
    
    # Predict emails
    print(f"Predicting emails for {len(contacts)} contacts...")
    predictions = await predictor.predict_emails_for_contacts(contacts)
    
    # Save results to CSV
    if predictor.save_predictions_to_csv(predictions, args.output):
        print(f"✅ Email predictions saved to {args.output}")
        
        # Update database with best predictions
        print("🔄 Updating database with best email predictions...")
        if predictor.update_database_with_predictions(predictions):
            print("✅ Database updated with predicted emails")
        else:
            print("⚠️  Database update failed, but CSV was saved")
        
        # Generate and display report
        report = predictor.generate_prediction_report(predictions)
        print("\n" + report)
        
        # Save report
        report_path = args.output.replace('.csv', '_report.txt')
        with open(report_path, 'w') as f:
            f.write(report)
        print(f"📊 Detailed report saved to {report_path}")
    else:
        print("❌ Failed to save predictions")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
