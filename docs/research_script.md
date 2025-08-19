# How to Build a Research Script Using Perplexity

## Overview
This guide covers how to create an automated research script using Perplexity's Sonar API to collect detailed company and contact information for cold email campaigns. Based on our work with the AGT Data Enrichment project, this approach has proven highly effective for gathering actionable insights.

## Prerequisites

### 1. API Setup
- **Perplexity API Key**: Sign up at [perplexity.ai](https://perplexity.ai) and get your API key
- **Google Sheets API**: For storing and managing research data
- **Python Environment**: Python 3.7+ with required packages

### 2. Required Python Packages
```bash
pip install requests pandas google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client python-dotenv
```

## Core Components

### 1. API Configuration
```python
import requests
from dotenv import load_dotenv
import os

load_dotenv()
perplexity_api_key = os.getenv('PERPLEXITY_API_KEY')

# Perplexity Sonar API endpoint
url = "https://api.perplexity.ai/chat/completions"
headers = {
    "Authorization": f"Bearer {perplexity_api_key}",
    "Content-Type": "application/json"
}
```

### 2. Research Data Structure
Define the research fields you want to collect:
```python
research_data = {
    'company_research': '',        # Company overview and mission
    'contact_research': '',        # Contact background and role
    'industry_pain_points': '',    # Industry challenges and pain points
    'schreiber_opportunity': '',   # How your product fits
    'research_quality': 0          # Quality score (1-10)
}
```

## Research Prompts

### 1. Company Research
```python
company_prompt = f"""Research {company_name} ({website}) and provide a concise summary including:
1. Company description and mission
2. Main products and services  
3. Company size and scale
4. Industry focus
5. Key business areas that might use dairy ingredients

Return a clean, actionable summary in 3-4 sentences."""
```

### 2. Contact Research
```python
contact_prompt = f"""Research {contact_name} at {company_name} and provide insights on:
1. Professional background and role
2. Current responsibilities
3. Industry expertise
4. Potential pain points in their role
5. How they might benefit from heat-stable cream cheese solutions

Focus on actionable insights for cold email personalization."""
```

### 3. Pain Points Research
```python
pain_points_prompt = f"""Research the food industry pain points that {company_name} likely faces:
1. Supply chain challenges
2. Quality control issues
3. Cost management pressures
4. Regulatory compliance challenges
5. Production efficiency concerns

Provide specific, industry-relevant pain points that can be addressed in cold emails."""
```

### 4. Opportunity Research
```python
opportunity_prompt = f"""Based on {company_name}'s business model and {contact_name}'s role, explain:
1. How heat-stable cream cheese could benefit their operations
2. Specific use cases for their products
3. Potential cost savings or efficiency gains
4. Competitive advantages they could gain
5. Why this contact would be interested

Make it specific to their business needs."""
```

## API Call Implementation

### 1. Making the API Call
```python
def research_with_perplexity(prompt, max_tokens=400):
    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers=headers,
            json={
                "model": "sonar-pro",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.1  # Low temperature for consistent results
            }
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                choice = result['choices'][0]
                message = choice.get('message', {})
                
                if 'content' in message:
                    return message['content'].strip()
                else:
                    return "No research data available"
            else:
                return "API response format error"
        else:
            return f"API error: {response.status_code}"
            
    except Exception as e:
        return f"API call failed: {str(e)}"
```

### 2. Research Quality Scoring
```python
def score_research_quality(company_research, contact_research, pain_points, opportunity):
    """Score research quality from 1-10 based on completeness and relevance"""
    score = 0
    
    # Company research quality
    if len(company_research) > 200 and "company" in company_research.lower():
        score += 2
    
    # Contact research quality
    if len(contact_research) > 200 and contact_research != "No research data available":
        score += 2
    
    # Pain points quality
    if len(pain_points) > 300 and any(word in pain_points.lower() for word in ["challenge", "issue", "problem", "pressure"]):
        score += 2
    
    # Opportunity quality
    if len(opportunity) > 200 and "cream cheese" in opportunity.lower():
        score += 2
    
    # Overall relevance
    if score >= 6:
        score += 2  # Bonus for comprehensive research
    
    return min(score, 10)
```

## Data Storage and Management

### 1. Google Sheets Integration
```python
def write_research_to_sheet(sheets_handler, sheet_url, sheet_name, sheet_row, research_data):
    """Write research data to specific columns in Google Sheets"""
    
    # Define column mapping (AV-AZ)
    columns = {
        'company_research': 'AV',
        'contact_research': 'AW', 
        'industry_pain_points': 'AX',
        'schreiber_opportunity': 'AY',
        'research_quality': 'AZ'
    }
    
    for field, column in columns.items():
        value = research_data.get(field, '')
        cell_range = f"{sheet_name}!{column}{sheet_row}"
        
        try:
            sheets_handler.update_cell(cell_range, value)
            print(f"    📝 Column {column} (row {sheet_row}): {value[:50]}...")
        except Exception as e:
            print(f"    ❌ Error writing to {column}: {e}")
```

### 2. Error Handling and Fallbacks
```python
def robust_research_pipeline(company_name, website, contact_name, job_title):
    """Robust research pipeline with error handling and fallbacks"""
    
    research_data = {}
    
    # Company research with retry logic
    company_research = research_with_perplexity(company_prompt)
    if company_research == "No research data available":
        # Fallback: Use basic company info
        company_research = f"{company_name} is a company in the food industry with website {website}."
    
    # Contact research with fallback
    contact_research = research_with_perplexity(contact_prompt)
    if contact_research == "No research data available":
        contact_research = f"{contact_name} is a {job_title} at {company_name}."
    
    # Continue with other research fields...
    
    return research_data
```

## Best Practices

### 1. Prompt Engineering
- **Be Specific**: Include company name, website, and contact details in prompts
- **Set Clear Expectations**: Specify desired output format and length
- **Use Low Temperature**: 0.1 for consistent, factual responses
- **Limit Tokens**: 400-600 tokens per research field for concise results

### 2. Rate Limiting and API Management
```python
import time

def research_with_rate_limiting(prompts, delay=1):
    """Research multiple fields with rate limiting"""
    results = {}
    
    for field, prompt in prompts.items():
        results[field] = research_with_perplexity(prompt)
        time.sleep(delay)  # Respect API rate limits
    
    return results
```

### 3. Data Validation
```python
def validate_research_data(research_data):
    """Validate research data before writing to sheets"""
    
    validation_errors = []
    
    for field, value in research_data.items():
        if not value or value == "No research data available":
            validation_errors.append(f"Missing {field}")
        elif len(value) < 50:
            validation_errors.append(f"{field} too short: {len(value)} chars")
    
    return validation_errors
```

## Complete Research Pipeline Example

```python
def run_research_pipeline(company_data):
    """Complete research pipeline for a company"""
    
    print(f"🔍 Researching {company_data['company_name']}...")
    
    # Phase 1: Research Collection
    research_data = {}
    
    # Company research
    company_prompt = f"Research {company_data['company_name']} ({company_data['website']})..."
    research_data['company_research'] = research_with_perplexity(company_prompt)
    
    # Contact research  
    contact_prompt = f"Research {company_data['contact_name']} at {company_data['company_name']}..."
    research_data['contact_research'] = research_with_perplexity(contact_prompt)
    
    # Pain points research
    pain_points_prompt = f"Research food industry pain points for {company_data['company_name']}..."
    research_data['industry_pain_points'] = research_with_perplexity(pain_points_prompt)
    
    # Opportunity research
    opportunity_prompt = f"Explain how heat-stable cream cheese could benefit {company_data['company_name']}..."
    research_data['schreiber_opportunity'] = research_with_perplexity(opportunity_prompt)
    
    # Quality scoring
    research_data['research_quality'] = score_research_quality(
        research_data['company_research'],
        research_data['contact_research'], 
        research_data['industry_pain_points'],
        research_data['schreiber_opportunity']
    )
    
    # Phase 2: Data Storage
    write_research_to_sheet(sheets_handler, sheet_url, sheet_name, sheet_row, research_data)
    
    print(f"✅ Research complete! Quality score: {research_data['research_quality']}/10")
    
    return research_data
```

## Troubleshooting Common Issues

### 1. API Rate Limits
- Implement delays between calls (1-2 seconds)
- Use exponential backoff for failed requests
- Monitor API usage and implement queuing if needed

### 2. Poor Research Quality
- Refine prompts to be more specific
- Increase max_tokens for more detailed responses
- Add validation checks for research completeness

### 3. Data Consistency Issues
- Implement data cleaning and formatting
- Add fallback values for missing data
- Validate data before writing to sheets

## Advanced Features

### 1. Batch Processing
```python
def process_multiple_companies(company_list):
    """Process multiple companies in sequence"""
    
    for i, company in enumerate(company_list):
        print(f"Processing company {i+1}/{len(company_list)}: {company['name']}")
        
        try:
            research_data = run_research_pipeline(company)
            print(f"✅ {company['name']} research complete")
        except Exception as e:
            print(f"❌ Error researching {company['name']}: {e}")
            continue
        
        time.sleep(2)  # Rate limiting
```

### 2. Research Analytics
```python
def analyze_research_quality(research_results):
    """Analyze research quality across multiple companies"""
    
    quality_scores = [r['research_quality'] for r in research_results]
    
    print(f"📊 Research Quality Analysis:")
    print(f"   Average Score: {sum(quality_scores)/len(quality_scores):.1f}/10")
    print(f"   High Quality (8-10): {sum(1 for s in quality_scores if s >= 8)}")
    print(f"   Medium Quality (5-7): {sum(1 for s in quality_scores if 5 <= s <= 7)}")
    print(f"   Low Quality (1-4): {sum(1 for s in quality_scores if s <= 4)}")
```

## Conclusion

This research script approach using Perplexity's Sonar API provides:
- **High-quality research data** for cold email personalization
- **Automated data collection** at scale
- **Consistent data structure** for downstream processing
- **Quality scoring** to ensure research meets standards
- **Robust error handling** for production use

The key to success is crafting specific, actionable prompts and implementing proper error handling and validation throughout the pipeline.