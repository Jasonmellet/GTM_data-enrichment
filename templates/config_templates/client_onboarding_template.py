# Client Onboarding Template
# Copy this file to clients/[client_name]/config/client_config.py and customize

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# CLIENT INFORMATION - CUSTOMIZE THESE SECTIONS
# ============================================================================

# Basic Client Information
CLIENT_NAME = "[CLIENT_NAME]"                    # e.g., "Acme Corp"
CLIENT_INDUSTRY = "[CLIENT_INDUSTRY]"            # e.g., "Software Development"
CLIENT_PRODUCT = "[CLIENT_PRODUCT]"              # e.g., "AI-Powered Analytics Platform"
CLIENT_WEBSITE = "[CLIENT_WEBSITE]"              # e.g., "https://www.acmecorp.com"

# ============================================================================
# API CONFIGURATION - UPDATE WITH YOUR API KEYS
# ============================================================================

# API Keys (load from environment variables)
PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Google Sheets Configuration
GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
GOOGLE_SHEET_NAME = "Sheet1"  # Update if different

# ============================================================================
# RESEARCH CONFIGURATION - CUSTOMIZE FOR YOUR INDUSTRY
# ============================================================================

# Research Fields (customize descriptions for your use case)
RESEARCH_FIELDS = {
    'company_research': 'Company overview and mission',
    'contact_research': 'Contact background and role',
    'industry_pain_points': 'Industry challenges and pain points',
    'opportunity_match': 'How your product fits their needs',  # Update this description
    'research_quality': 'Quality score (1-10)'
}

# ============================================================================
# EMAIL CONFIGURATION - CUSTOMIZE FOR YOUR BUSINESS MODEL
# ============================================================================

# Email Structure (customize CTAs for your business)
EMAIL_STRUCTURE = {
    'email_1': {
        'cta_type': 'reply_to_email',
        'cta_description': 'Simply reply to this email'
    },
    'email_2': {
        'cta_type': 'visit_website',
        'cta_description': 'Visit our website',
        'website_url': '[YOUR_WEBSITE_URL]'  # Update this
    },
    'email_3': {
        'cta_type': 'request_demo',  # Customize based on your offer
        'cta_description': 'Request a demo',  # Customize this
        'demo_url': '[YOUR_DEMO_URL]'  # Update this
    }
}

# ============================================================================
# GOOGLE SHEETS COLUMN MAPPING - UPDATE BASED ON YOUR SHEET STRUCTURE
# ============================================================================

# Column Mappings (update these to match your Google Sheet)
SHEET_COLUMNS = {
    'research': {
        'company_research': 'AV',      # Update if different
        'contact_research': 'AW',      # Update if different
        'industry_pain_points': 'AX',  # Update if different
        'opportunity_match': 'AY',     # Update if different
        'research_quality': 'AZ'       # Update if different
    },
    'emails': {
        'email_1': {
            'subject': 'BA',           # Update if different
            'icebreaker': 'BB',        # Update if different
            'body': 'BC',              # Update if different
            'cta': 'BD'                # Update if different
        },
        'email_2': {
            'subject': 'BE',           # Update if different
            'icebreaker': 'BF',        # Update if different
            'body': 'BG',              # Update if different
            'cta': 'BH'                # Update if different
        },
        'email_3': {
            'subject': 'BI',           # Update if different
            'icebreaker': 'BJ',        # Update if different
            'body': 'BK',              # Update if different
            'cta': 'BL'                # Update if different
        }
    }
}

# ============================================================================
# QUALITY THRESHOLDS - CUSTOMIZE BASED ON YOUR STANDARDS
# ============================================================================

# Research Quality Thresholds
RESEARCH_QUALITY_THRESHOLD = 6  # Minimum quality score to generate emails

# ============================================================================
# RESEARCH PROMPTS - CUSTOMIZE FOR YOUR INDUSTRY AND PRODUCT
# ============================================================================

# Research Prompts (customize for your industry and product)
RESEARCH_PROMPTS = {
    'company': """Research {company_name} ({website}) and provide a concise summary including:
1. Company description and mission
2. Main products and services  
3. Company size and scale
4. Industry focus
5. Key business areas that might use [YOUR_PRODUCT]  # Update this

Return a clean, actionable summary in 3-4 sentences.""",
    
    'contact': """Research {contact_name} at {company_name} and provide insights on:
1. Professional background and role
2. Current responsibilities
3. Industry expertise
4. Potential pain points in their role
5. How they might benefit from [YOUR_PRODUCT] solutions  # Update this

Focus on actionable insights for cold email personalization.""",
    
    'pain_points': """Research the [INDUSTRY] pain points that {company_name} likely faces:  # Update industry
1. [INDUSTRY_SPECIFIC_CHALLENGE_1]  # Update these
2. [INDUSTRY_SPECIFIC_CHALLENGE_2]  # Update these
3. [INDUSTRY_SPECIFIC_CHALLENGE_3]  # Update these
4. [INDUSTRY_SPECIFIC_CHALLENGE_4]  # Update these
5. [INDUSTRY_SPECIFIC_CHALLENGE_5]  # Update these

Provide specific, industry-relevant pain points that can be addressed in cold emails.""",
    
    'opportunity': """Based on {company_name}'s business model and {contact_name}'s role, explain:
1. How [YOUR_PRODUCT] could benefit their operations  # Update this
2. Specific use cases for their products
3. Potential cost savings or efficiency gains
4. Competitive advantages they could gain
5. Why this contact would be interested

Make it specific to their business needs."""
}

# ============================================================================
# EMAIL GENERATION PROMPTS - CUSTOMIZE FOR YOUR BUSINESS
# ============================================================================

# Email Generation Prompts (customize for your business and product)
EMAIL_PROMPTS = {
    'master_prompt': """You are an expert cold email copywriter for [YOUR_COMPANY], a leading [YOUR_INDUSTRY] company.  # Update these

TARGET COMPANY SPECIFIC INFORMATION:
Company: {company_name}
Company Website: {website}
Company Industry: {industry}
Company LinkedIn: {company_linkedin}

CONTACT SPECIFIC INFORMATION:
Contact Name: {first_name} {last_name}
Job Title: {contact_title}
Contact LinkedIn: {contact_linkedin}

RESEARCH DATA (USE THIS SPECIFIC INFORMATION):
Company Research: {company_summary}
Contact Research: {contact_summary}
Industry Pain Points: {pain_points}
Opportunity Match: {opportunity_match}

Create THREE completely different, HIGHLY PERSONALIZED cold emails for {first_name} at {company_name}.
Each email MUST reference specific details from the research data above.

CRITICAL REQUIREMENTS:
- MENTION {company_name} specifically in each email
- REFERENCE {first_name}'s role as {contact_title}
- USE specific pain points from research data in the body
- REFERENCE their industry ({industry}) and specific challenges
- MENTION their website ({website}) or LinkedIn if relevant
- NO greetings (don't start with "Hi [Name]" or "Hello")
- Total word count for icebreaker + body + CTA: MAX 150 words
- Subject lines must be original and non-spammy
- Icebreakers cannot contain the person's name but should reference their role/company

EMAIL STRUCTURE:
- Email 1: Subject + Icebreaker + Body + CTA (reply to email)
- Email 2: Subject + Icebreaker + Body + CTA (visit website)
- Email 3: Subject + Icebreaker + Body + CTA ([YOUR_CTA_TYPE])  # Update this

CTA REQUIREMENTS:
- Email 1: "Simply reply to this email" (but make it compelling, not generic)
- Email 2: "Visit our website" BUT make it specific about what they'll discover
- Email 3: "[YOUR_CTA_DESCRIPTION]" BUT make it compelling about what they'll get  # Update this
- CTAs must be SEPARATE from the body content
- Each CTA should be 1-2 sentences max and drive specific action

PERSONALIZATION REQUIREMENTS:
- Reference their specific products/services (from research)
- INCORPORATE SPECIFIC PAIN POINTS from research data
- Reference their company's mission/values if mentioned in research
- Use their industry-specific language and terminology
- Make it clear you've researched THEIR company specifically

PSYCHOLOGICAL ELEMENTS TO INCLUDE:
- Subject: Curiosity gaps, numbers, questions, urgency
- Icebreaker: Industry trends, recent events, specific observations about THEIR company
- Body: Social proof, specific benefits, problem-solution alignment for THEIR needs
- CTA: Low-commitment, urgency, FOMO, clear next steps with specific benefits

FORMAT YOUR RESPONSE EXACTLY LIKE THIS:
EMAIL 1:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from research]
CTA: [CTA content - separate from body]

EMAIL 2:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from research]
CTA: [CTA content - separate from body]

EMAIL 3:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from research]
CTA: [CTA content - separate from body]

Focus on promoting [YOUR_PRODUCT] and addressing {company_name}'s specific pain points with YOUR product benefits. Make each email feel like it was written specifically for {first_name} and {company_name}, not a generic template."""
}

# ============================================================================
# FILE PATHS - USUALLY DON'T NEED TO CHANGE
# ============================================================================

# File Paths
DATA_DIR = "data"
OUTPUTS_DIR = "outputs"
SCRIPTS_DIR = "scripts"
CONFIG_DIR = "config"

# ============================================================================
# LOGGING CONFIGURATION - USUALLY DON'T NEED TO CHANGE
# ============================================================================

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "[client_name]_pipeline.log"  # Update this with your client name

# ============================================================================
# ONBOARDING CHECKLIST
# ============================================================================
"""
ONBOARDING CHECKLIST:
1. ✅ Update CLIENT_NAME, CLIENT_INDUSTRY, CLIENT_PRODUCT, CLIENT_WEBSITE
2. ✅ Update EMAIL_STRUCTURE with your specific CTAs and URLs
3. ✅ Update SHEET_COLUMNS to match your Google Sheet structure
4. ✅ Customize RESEARCH_PROMPTS for your industry and product
5. ✅ Customize EMAIL_PROMPTS for your business and offerings
6. ✅ Update LOG_FILE name with your client name
7. ✅ Test with a small dataset first
8. ✅ Verify Google Sheets column mappings
9. ✅ Check API key permissions and quotas
10. ✅ Review generated content for quality and relevance
"""
