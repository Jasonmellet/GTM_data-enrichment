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
CLIENT_NAME = "Camp Broadway MyWay (Broadway Education Alliance)"
CLIENT_INDUSTRY = "Arts & Education – Youth Musical Theatre Enrichment"
CLIENT_PRODUCT = "Online, customizable experiential learning program teaching fundamentals of musical theater, character development, and life skills"
CLIENT_WEBSITE = "https://campbwaymyway.com/"

# Related/affiliated organizations (for research context)
AFFILIATE_URLS = [
    "https://campbwaymyway.com/",   # Primary program
    "https://campbroadway.com/",    # Camp Broadway
    "https://stagenotes.org/",      # StageNotes (study guides, classroom resources)
    "https://bealliance.org/",      # Broadway Education Alliance (parent 501c3)
    "https://rogerreesawards.com/"  # Roger Rees Awards (recognition program)
]

# ============================================================================
# API CONFIGURATION - UPDATE WITH YOUR API KEYS
# ============================================================================

# API Keys (prefer Broadway-specific keys; fall back to globals)
PERPLEXITY_API_KEY = os.getenv('BROADWAY_PERPLEXITY_API_KEY') or os.getenv('PERPLEXITY_API_KEY')
OPENAI_API_KEY = os.getenv('BROADWAY_OPENAI_API_KEY') or os.getenv('OPENAI_API_KEY')

# Google Sheets not used for Broadway (local CSV workflow only)
GOOGLE_SHEET_URL = None
GOOGLE_SHEET_NAME = None

# ============================================================================
# RESEARCH CONFIGURATION - CUSTOMIZE FOR YOUR INDUSTRY
# ============================================================================

# Research Fields (customized for arts education/enrichment)
RESEARCH_FIELDS = {
    'company_research': 'Program overview, mission, audience (ages/educators), delivery model, distribution/partners',
    'contact_research': 'Contact background and role in arts education/program operations/partnerships',
    'industry_pain_points': 'Arts education & enrichment challenges (curriculum, access, licensing, funding, scale)',
    'opportunity_match': 'How Broadway’s enrichment assets map to partner needs (schools, camps, licensing orgs)',
    'research_quality': 'Quality score (1-10)'
}

# ============================================================================
# EMAIL CONFIGURATION – Not applicable for Broadway (research‑only)
EMAIL_STRUCTURE = None

# ============================================================================
# Column mapping – not using Google Sheets; CSV columns are handled in pipeline
SHEET_COLUMNS = None

# ============================================================================
# QUALITY THRESHOLDS - CUSTOMIZE BASED ON YOUR STANDARDS
# ============================================================================

# Research Quality Thresholds
RESEARCH_QUALITY_THRESHOLD = 6  # Minimum quality score to generate emails

# ============================================================================
# RESEARCH PROMPTS - CUSTOMIZE FOR YOUR INDUSTRY AND PRODUCT
# ============================================================================

# Research Prompts (tailored to Broadway Education Alliance programs)
RESEARCH_PROMPTS = {
    'company': """Research {company_name} ({website}) and summarize in 3–5 sentences:
1) Program purpose/mission and primary audience (students, teachers, camps)
2) Delivery model (online modules, experiential activities), distribution partners (e.g., MTI)
3) Core offerings (theater fundamentals, character development, life skills) and notable advisors/affiliations
4) Launch timeline, scale, and any credential/recognition components
""",
    
    'contact': """Research {contact_name} at {company_name} and provide:
1) Background and current role (education programs, partnerships, curriculum, licensing, operations)
2) Responsibilities tied to content, distribution, fundraising, educator success, or program adoption
3) Likely pain points (resource constraints, curriculum alignment, school onboarding, measurement)
4) How online enrichment content and study guides can help (scalable delivery, standards alignment, turnkey lesson plans)
""",
    
    'pain_points': """List arts education/enrichment pain points relevant to {company_name}:
1) Curriculum alignment (national/state standards) and assessment
2) Access & equity (cost, technology, teacher training)
3) Licensing & rights (content use with partners like MTI), attribution, compliance
4) Scale & distribution (school onboarding, PD for educators, asynchronous content)
5) Funding & sustainability (grants, sponsorships, institutional partnerships)
Provide concrete, program-operations examples, not generic statements.""",
    
    'opportunity': """Given {company_name}'s enrichment focus, explain succinctly:
1) Where online modules + study guides fit (classroom, after‑school, camps, homeschool)
2) How partnerships (MTI, StageNotes, BEA) accelerate distribution and standards alignment
3) Ways to measure outcomes (participation, completion, teacher feedback)
4) What supports adoption (turnkey curriculum, PD resources, flexible licensing)
5) Immediate next step that reduces friction for {contact_name}'s role
"""
}

# ============================================================================
# Email generation prompts – not applicable for Broadway
EMAIL_PROMPTS = None

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
