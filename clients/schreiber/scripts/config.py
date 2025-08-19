import os
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
