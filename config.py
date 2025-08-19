import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

# Google Sheets API Configuration
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv('GOOGLE_SHEETS_CREDENTIALS_FILE', 'credentials.json')
GOOGLE_SHEETS_TOKEN_FILE = os.getenv('GOOGLE_SHEETS_TOKEN_FILE', 'token.json')

# Government API Configuration
CENSUS_API_KEY = os.getenv('CENSUS_API_KEY', '')
CENSUS_BASE_URL = 'https://api.census.gov/data/2020/cbp'

# AI Service API Keys
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY', '')
GOOGLE_AI_API_KEY = os.getenv('GOOGLE_AI_API_KEY', '')
PERPLEXITY_API_KEY = os.getenv('PERPLEXITY_API_KEY', '')

# Broadway-specific API Keys
BROADWAY_OPENAI_API_KEY = os.getenv('BROADWAY_OPENAI_API_KEY', '')
BROADWAY_ANTHROPIC_API_KEY = os.getenv('BROADWAY_ANTHROPIC_API_KEY', '')
BROADWAY_PERPLEXITY_API_KEY = os.getenv('BROADWAY_PERPLEXITY_API_KEY', '')
BROADWAY_XAI_API_KEY = os.getenv('BROADWAY_XAI_API_KEY', '')
BROADWAY_GOOGLE_MAPS_API_KEY = os.getenv('BROADWAY_GOOGLE_MAPS_API_KEY', '')

# Database Configuration
SUMMER_CAMPS_DB_HOST = os.getenv('SUMMER_CAMPS_DB_HOST', 'localhost')
SUMMER_CAMPS_DB_PORT = os.getenv('SUMMER_CAMPS_DB_PORT', '5432')
SUMMER_CAMPS_DB_NAME = os.getenv('SUMMER_CAMPS_DB_NAME', 'summer_camps_db')
SUMMER_CAMPS_DB_USER = os.getenv('SUMMER_CAMPS_DB_USER', 'summer_camps_user')
SUMMER_CAMPS_DB_PASSWORD = os.getenv('SUMMER_CAMPS_DB_PASSWORD', '')
SUMMER_CAMPS_DB_SSLMODE = os.getenv('SUMMER_CAMPS_DB_SSLMODE', 'disable')

def get_db_connection():
    """Get database connection for Summer Camps database."""
    return psycopg.connect(
        host=SUMMER_CAMPS_DB_HOST,
        port=SUMMER_CAMPS_DB_PORT,
        dbname=SUMMER_CAMPS_DB_NAME,
        user=SUMMER_CAMPS_DB_USER,
        password=SUMMER_CAMPS_DB_PASSWORD,
        sslmode=SUMMER_CAMPS_DB_SSLMODE
    )

# NAICS Code Mapping (Common business types)
COMMON_NAICS_MAPPING = {
    'restaurant': '722511',
    'retail': '441000',
    'healthcare': '621000',
    'construction': '230000',
    'manufacturing': '310000',
    'technology': '511200',
    'finance': '522000',
    'education': '611000',
    'transportation': '480000',
    'real estate': '531000'
}
