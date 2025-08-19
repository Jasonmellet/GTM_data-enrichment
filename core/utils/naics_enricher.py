import requests
import json
import re
from typing import Dict, List, Optional, Tuple
import openai
import anthropic
import google.generativeai as genai
from config import (
    CENSUS_API_KEY, CENSUS_BASE_URL, OPENAI_API_KEY, 
    ANTHROPIC_API_KEY, GOOGLE_AI_API_KEY, COMMON_NAICS_MAPPING
)

class NAICSEnricher:
    """Enriches business data with NAICS codes using government APIs and AI services."""
    
    def __init__(self):
        """Initialize the NAICS enricher with available AI services."""
        self.anthropic_client = None
        self.gemini_model = None
        
        # Initialize API clients and track usage
        self.usage_stats = {
            'openai': 0,
            'anthropic': 0,
            'gemini': 0,
            'perplexity': 0,
            'census_api': 0,
            'common_mapping': 0
        }
        
        # Initialize Anthropic Claude
        if ANTHROPIC_API_KEY:
            try:
                self.anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
                print(f"✅ Anthropic Claude initialized")
            except Exception as e:
                print(f"❌ Failed to initialize Anthropic: {e}")
        
        # Initialize Google Gemini
        if GOOGLE_AI_API_KEY:
            try:
                import google.generativeai as genai
                genai.configure(api_key=GOOGLE_AI_API_KEY)
                self.gemini_model = genai.GenerativeModel('gemini-1.5-flash')
                print(f"✅ Google Gemini initialized")
            except Exception as e:
                print(f"❌ Failed to initialize Gemini: {e}")
        
        print(f"🔧 AI Services Available: {sum(1 for v in self.usage_stats.values() if v == 0)} services ready")
    
    def setup_ai_clients(self):
        """Initialize AI service clients if API keys are available."""
        if OPENAI_API_KEY:
            openai.api_key = OPENAI_API_KEY
        
        if ANTHROPIC_API_KEY:
            self.anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        
        if GOOGLE_AI_API_KEY:
            genai.configure(api_key=GOOGLE_AI_API_KEY)
            self.gemini_model = genai.GenerativeModel('gemini-pro')
    
    def get_naics_from_census_api(self, business_name: str, business_type: str = None) -> Optional[str]:
        """
        Try to get NAICS code from Census Bureau API.
        This is the preferred method as it's free and authoritative.
        """
        if not CENSUS_API_KEY:
            return None
        
        try:
            # Search for business establishments by name/type
            params = {
                'key': CENSUS_API_KEY,
                'get': 'NAICS2017,ESTAB,EMP,PAYANN',
                'for': 'us:*',
                'NAICS2017': '*'
            }
            
            if business_type:
                # Try to find businesses of similar type
                params['NAICS2017'] = self._get_naics_range_for_type(business_type)
            
            response = requests.get(f"{CENSUS_BASE_URL}", params=params)
            
            if response.status_code == 200:
                data = response.json()
                # Parse response and find best match
                return self._parse_census_response(data, business_name, business_type)
        
        except Exception as e:
            print(f"Error calling Census API: {e}")
        
        return None
    
    def _get_naics_range_for_type(self, business_type: str) -> str:
        """Get NAICS code range for a business type."""
        business_type_lower = business_type.lower()
        
        for key, naics in COMMON_NAICS_MAPPING.items():
            if key in business_type_lower:
                return f"{naics[:2]}*"  # Return 2-digit level
        
        return "*"
    
    def _parse_census_response(self, data: List, business_name: str, business_type: str) -> Optional[str]:
        """Parse Census API response to find relevant NAICS code."""
        if not data or len(data) < 2:
            return None
        
        # Skip header row
        for row in data[1:]:
            if len(row) >= 4:
                naics_code = row[0]
                estab_count = int(row[1]) if row[1].isdigit() else 0
                
                # Prefer codes with more establishments (more common)
                # Filter out "00" codes and other invalid codes
                if (estab_count > 0 and 
                    naics_code != "0" and 
                    naics_code != "00" and 
                    len(naics_code) >= 2 and
                    naics_code.isdigit()):
                    return naics_code
        
        return None
    
    def get_naics_from_ai(self, business_name: str, business_type: str = None, 
                          business_description: str = None) -> Optional[str]:
        """
        Use AI services to determine NAICS code based on business information.
        Falls back through multiple AI services if available.
        """
        prompt = self._create_naics_prompt(business_name, business_type, business_description)
        
        # Try OpenAI first
        if OPENAI_API_KEY:
            try:
                client = openai.OpenAI(api_key=OPENAI_API_KEY)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a business classification expert. Return only the 6-digit NAICS code, nothing else."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=10,
                    temperature=0
                )
                naics_code = response.choices[0].message.content.strip()
                if self._validate_naics_code(naics_code):
                    self.usage_stats['openai'] += 1
                    return naics_code
            except Exception as e:
                print(f"OpenAI API error: {e}")
        
        # Try Anthropic Claude
        if hasattr(self, 'anthropic_client'):
            try:
                response = self.anthropic_client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=10,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                naics_code = response.content[0].text.strip()
                if self._validate_naics_code(naics_code):
                    self.usage_stats['anthropic'] += 1
                    return naics_code
            except Exception as e:
                print(f"Anthropic API error: {e}")
        
        # Try Google Gemini
        if hasattr(self, 'gemini_model'):
            try:
                response = self.gemini_model.generate_content(prompt)
                naics_code = response.text.strip()
                if self._validate_naics_code(naics_code):
                    self.usage_stats['gemini'] += 1
                    return naics_code
            except Exception as e:
                print(f"Google AI API error: {e}")
        
        return None
    
    def _create_naics_prompt(self, business_name: str, business_type: str = None, 
                            business_description: str = None) -> str:
        """Create a prompt for AI services to determine NAICS code."""
        prompt = f"Business Name: {business_name}"
        
        if business_type:
            prompt += f"\nBusiness Type: {business_type}"
        
        if business_description:
            prompt += f"\nDescription: {business_description}"
        
        prompt += "\n\nBased on this business information, what is the most appropriate 6-digit NAICS code? Return only the code."
        
        return prompt
    
    def _validate_naics_code(self, naics_code: str) -> bool:
        """Validate that a string is a valid NAICS code format."""
        if not naics_code:
            return False
        
        # Remove any extra text, keep only digits
        digits_only = re.sub(r'\D', '', naics_code)
        
        # NAICS codes are typically 2, 3, 4, 5, or 6 digits
        if len(digits_only) in [2, 3, 4, 5, 6]:
            return True
        
        return False
    
    def analyze_cream_cheese_potential(self, business_name: str, naics_code: str, business_type: str = None) -> str:
        """
        Analyze if a company is likely to buy heat-stable cream cheese in bulk (100K+ lbs/year).
        Returns 'Yes' or 'No'.
        """
        prompt = f"""Business Analysis: {business_name}
NAICS Code: {naics_code}
Business Type: {business_type or 'Not specified'}

Based on this business information, determine if this company is LIKELY to purchase heat-stable cream cheese in bulk quantities (100,000+ pounds per year) from Schreiber Foods.

Consider:
- Food manufacturing companies that use cream cheese as an ingredient
- Large-scale food processors
- Companies that make products requiring heat-stable dairy ingredients
- Bulk food manufacturers
- Companies that would need significant quantities for production

Return ONLY 'Yes' or 'No' - nothing else."""

        # Try OpenAI first
        if OPENAI_API_KEY:
            try:
                client = openai.OpenAI(api_key=OPENAI_API_KEY)
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are a food industry expert. Analyze if companies are likely to buy bulk cream cheese. Return only 'Yes' or 'No'."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=5,
                    temperature=0
                )
                result = response.choices[0].message.content.strip().lower()
                if result in ['yes', 'no']:
                    self.usage_stats['openai'] += 1
                    return result.capitalize()
            except Exception as e:
                print(f"OpenAI API error: {e}")
        
        # Try Anthropic Claude
        if hasattr(self, 'anthropic_client'):
            try:
                response = self.anthropic_client.messages.create(
                    model="claude-3-haiku-20240307",
                    max_tokens=5,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                result = response.content[0].text.strip().lower()
                if result in ['yes', 'no']:
                    self.usage_stats['anthropic'] += 1
                    return result.capitalize()
            except Exception as e:
                print(f"Anthropic API error: {e}")
        
        # Try Google Gemini
        if hasattr(self, 'gemini_model'):
            try:
                response = self.gemini_model.generate_content(prompt)
                result = response.text.strip().lower()
                if result in ['yes', 'no']:
                    self.usage_stats['gemini'] += 1
                    return result.capitalize()
            except Exception as e:
                print(f"Google AI API error: {e}")
        
        # Default fallback based on NAICS code
        food_manufacturing_codes = ['311', '312', '445', '722']  # Food manufacturing, food stores, restaurants
        if any(naics_code.startswith(code) for code in food_manufacturing_codes):
            self.usage_stats['common_mapping'] += 1
            return "Yes"
        self.usage_stats['common_mapping'] += 1
        return "No"

    def enrich_business_data(self, business_name: str, business_type: str = None, 
                           business_description: str = None) -> Dict:
        """
        Enrich business data with NAICS code and cream cheese buying potential.
        """
        result = {
            'business_name': business_name,
            'business_type': business_type,
            'business_description': business_description,
            'naics_code': None,
            'likely_to_buy': 'No',
            'source': None
        }
        
        # First try government API (free and authoritative)
        naics_code = self.get_naics_from_census_api(business_name, business_type)
        if naics_code and len(naics_code) >= 4:  # Only use Census if we get a specific code (4+ digits)
            result['naics_code'] = naics_code
            result['source'] = 'Census Bureau API'
        else:
            # Fall back to AI services
            naics_code = self.get_naics_from_ai(business_name, business_type, business_description)
            if naics_code:
                result['naics_code'] = naics_code
                result['source'] = 'AI Service'
        
        # Analyze cream cheese buying potential if we have a NAICS code
        if result['naics_code']:
            result['likely_to_buy'] = self.analyze_cream_cheese_potential(
                business_name, result['naics_code'], business_type
            )
        
        return result

    def get_usage_stats(self) -> Dict[str, int]:
        """Get the usage statistics for all AI services."""
        return self.usage_stats.copy()
    
    def print_usage_stats(self):
        """Print a formatted summary of AI service usage."""
        print("\n🤖 AI Service Usage Breakdown:")
        print("=" * 40)
        
        total_calls = sum(self.usage_stats.values())
        if total_calls == 0:
            print("No AI service calls made yet.")
            return
        
        for service, count in self.usage_stats.items():
            if count > 0:
                percentage = (count / total_calls) * 100
                service_name = service.replace('_', ' ').title()
                print(f"  {service_name}: {count} calls ({percentage:.1f}%)")
        
        print(f"\n📊 Total AI Service Calls: {total_calls}")
        
        # Cost analysis
        print("\n💰 Estimated Cost Analysis:")
        print("  OpenAI GPT-4o-mini: ~$0.00015 per 1K tokens")
        print("  Anthropic Claude Haiku: ~$0.00025 per 1K tokens") 
        print("  Google Gemini Flash: ~$0.000075 per 1K tokens")
        print("  (Costs are minimal due to short prompts/responses)")
