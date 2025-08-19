# Schreiber Foods Client Configuration
# This file contains all client-specific settings and configurations

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Client Information
CLIENT_NAME = "Schreiber Foods"
CLIENT_INDUSTRY = "Dairy Manufacturing"
CLIENT_PRODUCT = "Heat-Stable Cream Cheese"
CLIENT_WEBSITE = "https://www.schreiberfoodsproducts.com"

# API Keys (load from environment variables)
PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Google Sheets Configuration
GOOGLE_SHEET_URL = os.getenv('GOOGLE_SHEET_URL')
GOOGLE_SHEET_NAME = "Sheet1"

# Research Configuration
RESEARCH_FIELDS = {
    'company_research': 'Company overview and mission',
    'contact_research': 'Contact background and role',
    'industry_pain_points': 'Industry challenges and pain points',
    'schreiber_opportunity': 'How heat-stable cream cheese fits',
    'research_quality': 'Quality score (1-10)'
}

# Email Configuration
EMAIL_STRUCTURE = {
    'email_1': {
        'cta_type': 'reply_to_email',
        'cta_description': 'Simply reply to this email'
    },
    'email_2': {
        'cta_type': 'visit_website',
        'cta_description': 'Visit our website',
        'website_url': 'https://www.schreiberfoodsproducts.com/about/'
    },
    'email_3': {
        'cta_type': 'request_sample',
        'cta_description': 'Request a free sample',
        'sample_url': 'https://www.schreiberfoodsproducts.com/request-sample/'
    }
}

# Column Mappings for Google Sheets
SHEET_COLUMNS = {
    'research': {
        'company_research': 'AV',
        'contact_research': 'AW',
        'industry_pain_points': 'AX',
        'schreiber_opportunity': 'AY',
        'research_quality': 'AZ'
    },
    'emails': {
        'email_1': {
            'subject': 'BA',
            'icebreaker': 'BB',
            'body': 'BC',
            'cta': 'BD'
        },
        'email_2': {
            'subject': 'BE',
            'icebreaker': 'BF',
            'body': 'BG',
            'cta': 'BH'
        },
        'email_3': {
            'subject': 'BI',
            'icebreaker': 'BJ',
            'body': 'BK',
            'cta': 'BL'
        }
    }
}

# Research Quality Thresholds
RESEARCH_QUALITY_THRESHOLD = 6  # Minimum quality score to generate emails

# Prompt Templates (can be customized per client)
RESEARCH_PROMPTS = {
    'company': """Research {company_name} ({website}) and provide a concise summary including:
1. Company description and mission
2. Main products and services  
3. Company size and scale
4. Industry focus
5. Key business areas that might use dairy ingredients

Return a clean, actionable summary in 3-4 sentences.""",
    
    'contact': """Research {contact_name} at {company_name} and provide insights on:
1. Professional background and role
2. Current responsibilities
3. Industry expertise
4. Potential pain points in their role
5. How they might benefit from heat-stable cream cheese solutions

Focus on actionable insights for cold email personalization.""",
    
    'pain_points': """Research the food industry pain points that {company_name} likely faces:
1. Supply chain challenges
2. Quality control issues
3. Cost management pressures
4. Regulatory compliance challenges
5. Production efficiency concerns

Provide specific, industry-relevant pain points that can be addressed in cold emails.""",
    
    'opportunity': """Based on {company_name}'s business model and {contact_name}'s role, explain:
1. How heat-stable cream cheese could benefit their operations
2. Specific use cases for their products
3. Potential cost savings or efficiency gains
4. Competitive advantages they could gain
5. Why this contact would be interested

Make it specific to their business needs."""
}

# Email Generation Prompts
EMAIL_PROMPTS = {
    'master_prompt': """You are an expert cold email copywriter for Schreiber Foods, a leading manufacturer of heat-stable cream cheese for the food industry.

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
Schreiber Opportunity: {opportunity_match}

Create THREE completely different, HIGHLY PERSONALIZED cold emails for {first_name} at {company_name}.
Each email MUST reference specific details from the research data above.

CRITICAL REQUIREMENTS:
- MENTION {company_name} specifically in each email
- REFERENCE {first_name}'s role as {contact_title}
- USE specific pain points from column AX (Industry Pain Points) in the body
- REFERENCE their industry ({industry}) and specific challenges
- MENTION their website ({website}) or LinkedIn if relevant
- NO greetings (don't start with "Hi [Name]" or "Hello")
- Total word count for icebreaker + body + CTA: MAX 150 words
- Subject lines must be original and non-spammy (avoid "Elevate" and generic terms)
- Icebreakers cannot contain the person's name but should reference their role/company

EMAIL STRUCTURE (CRITICAL):
- Email 1: Subject + Icebreaker + Body + CTA (reply to email)
- Email 2: Subject + Icebreaker + Body + CTA (visit website)
- Email 3: Subject + Icebreaker + Body + CTA (request sample)

CTA REQUIREMENTS (CRITICAL):
- Email 1: "Simply reply to this email" (but make it compelling, not generic)
- Email 2: "Visit our website" BUT make it specific about what they'll discover
- Email 3: "Request a free sample" BUT make it compelling about what they'll get
- CTAs must be SEPARATE from the body content - do NOT embed the CTA in the body
- Each CTA should be 1-2 sentences max and drive specific action

PERSONALIZATION REQUIREMENTS:
- Reference their specific products (cookies, crackers, etc. from research)
- INCORPORATE SPECIFIC PAIN POINTS from column AX (Industry Pain Points) in the body
- Reference their company's mission/values if mentioned in research
- Use their industry-specific language and terminology
- Make it clear you've researched THEIR company specifically

PSYCHOLOGICAL ELEMENTS TO INCLUDE:
- Subject: Curiosity gaps, numbers, questions, urgency
- Icebreaker: Industry trends, recent events, specific observations about THEIR company
- Body: Social proof, specific benefits, problem-solution alignment for THEIR needs, PAIN POINTS from column AX
- CTA: Low-commitment, urgency, FOMO, clear next steps with specific benefits

FORMAT YOUR RESPONSE EXACTLY LIKE THIS:
EMAIL 1:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from column AX]
CTA: [CTA content - separate from body]

EMAIL 2:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from column AX]
CTA: [CTA content - separate from body]

EMAIL 3:
Subject: [subject line]
Icebreaker: [icebreaker content]
Body: [body content - include pain points from column AX]
CTA: [CTA content - separate from body]

Focus on promoting Schreiber Foods' heat-stable cream cheese solutions and addressing {company_name}'s specific pain points with OUR product benefits. Make each email feel like it was written specifically for {first_name} and {company_name}, not a generic template. The CTAs must be compelling and specific to their business needs, but SEPARATE from the body content."""
}

# File Paths
DATA_DIR = "data"
OUTPUTS_DIR = "outputs"
SCRIPTS_DIR = "scripts"
CONFIG_DIR = "config"

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "schreiber_pipeline.log"
