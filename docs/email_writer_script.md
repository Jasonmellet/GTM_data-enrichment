# How to Create Personalized Cold Email Outreach Using Research Data

## Overview
This guide covers how to transform research data collected from a research bot into highly personalized, effective cold email campaigns. Based on our work with the AGT Data Enrichment project, this approach has proven highly effective for creating emails that convert.

## The Research-to-Email Pipeline

### 1. Data Flow
```
Research Bot → Research Data → Email Generator → Personalized Emails → Google Sheets
     ↓              ↓              ↓              ↓              ↓
Company Info → Pain Points → AI Analysis → Email Content → Campaign Ready
```

### 2. Required Research Data Fields
```python
research_data = {
    'company_name': 'Company Name',
    'website': 'www.company.com',
    'contact_name': 'First Last',
    'job_title': 'Job Title',
    'company_research': 'Company overview and mission...',
    'contact_research': 'Contact background and role...',
    'industry_pain_points': 'Specific challenges and issues...',
    'schreiber_opportunity': 'How your product fits...',
    'research_quality': 8  # Quality score 1-10
}
```

## Email Structure and Components

### 1. Email Anatomy
```
Subject Line (50 chars max)
    ↓
Icebreaker (1-2 sentences, no greeting)
    ↓
Body (Value proposition + pain point alignment)
    ↓
Call to Action (Specific, compelling next step)
```

### 2. Three Email Variations
Create three distinct emails for each contact:
- **Email 1**: Reply-focused CTA
- **Email 2**: Website visit CTA  
- **Email 3**: Sample request CTA

## Prompt Engineering for Email Generation

### 1. Master Email Prompt Structure
```python
email_prompt = f"""You are an expert cold email copywriter for {your_company}, a leading manufacturer of {your_product}.

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
- REFERENCE their industry and specific challenges
- MENTION their website or LinkedIn if relevant
- NO greetings (don't start with "Hi [Name]" or "Hello")
- Total word count for icebreaker + body + CTA: MAX 150 words
- Subject lines must be original and non-spammy
- Icebreakers cannot contain the person's name but should reference their role/company

EMAIL STRUCTURE:
- Email 1: Subject + Icebreaker + Body + CTA (reply to email)
- Email 2: Subject + Icebreaker + Body + CTA (visit website)
- Email 3: Subject + Icebreaker + Body + CTA (request sample)

CTA REQUIREMENTS:
- Email 1: "Simply reply to this email" (but make it compelling, not generic)
- Email 2: "Visit our website" BUT make it specific about what they'll discover
- Email 3: "Request a free sample" BUT make it compelling about what they'll get
- CTAs must be SEPARATE from the body content
- Each CTA should be 1-2 sentences max and drive specific action

PERSONALIZATION REQUIREMENTS:
- Reference their specific products (from research)
- INCORPORATE SPECIFIC PAIN POINTS from research data
- Reference their company's mission/values if mentioned
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

Focus on promoting {your_product} and addressing {company_name}'s specific pain points with YOUR product benefits."""
```

## Email Generation Implementation

### 1. OpenAI Integration
```python
from openai import OpenAI

def generate_three_emails(company_name, company_url, contact_name, contact_title, 
                         company_summary, contact_summary, pain_points, opportunity_match,
                         first_name, last_name, industry, company_linkedin, contact_linkedin):
    """Generate three distinct cold emails using OpenAI."""
    
    client = OpenAI(api_key=openai_api_key)
    
    prompt = email_prompt  # Use the master prompt above
    
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1500,
            temperature=0.7  # Slightly higher for creativity
        )
        
        if response.choices and len(response.choices) > 0:
            content = response.choices[0].message.content
            return parse_three_emails(content)
        else:
            print("    ❌ No response from OpenAI")
            return None
            
    except Exception as e:
        print(f"    ❌ OpenAI API error: {e}")
        return None
```

### 2. Email Parsing Function
```python
def parse_three_emails(content):
    """Parse the OpenAI response into structured email data."""
    emails = []
    
    try:
        # Split by email sections
        email_sections = content.split("EMAIL ")
        
        for section in email_sections[1:]:  # Skip first empty section
            if not section.strip():
                continue
                
            email_data = {}
            
            # Extract subject
            if "Subject:" in section:
                subject_start = section.find("Subject:") + 8
                subject_end = section.find("\n", subject_start)
                if subject_end != -1:
                    email_data['subject'] = section[subject_start:subject_end].strip()
            
            # Extract icebreaker
            if "Icebreaker:" in section:
                icebreaker_start = section.find("Icebreaker:") + 11
                icebreaker_end = section.find("\n", icebreaker_start)
                if icebreaker_end != -1:
                    email_data['icebreaker'] = section[icebreaker_start:icebreaker_end].strip()
            
            # Extract body
            if "Body:" in section:
                body_start = section.find("Body:") + 5
                body_end = section.find("\n", body_start)
                if body_end != -1:
                    email_data['body'] = section[body_start:body_end].strip()
            
            # Extract CTA
            if "CTA:" in section:
                cta_start = section.find("CTA:") + 4
                cta_end = section.find("\n", cta_start)
                if cta_end != -1:
                    cta_text = section[cta_start:cta_end].strip()
                    
                    # Fix any "No CTA" responses
                    if cta_text.lower() in ["no cta", "no cta.", "n/a", "none"]:
                        cta_text = "Request a free sample to see how our product can transform your business."
                    
                    email_data['cta'] = cta_text
                else:
                    # If no newline found, take everything after CTA:
                    cta_text = section[cta_start:].strip()
                    
                    # Fix any "No CTA" responses
                    if cta_text.lower() in ["no cta", "no cta.", "n/a", "none"]:
                        cta_text = "Request a free sample to see how our product can transform your business."
                    
                    email_data['cta'] = cta_text
            
            if email_data:
                emails.append(email_data)
        
        return emails
        
    except Exception as e:
        print(f"    ❌ Error parsing emails: {e}")
        return None
```

## Data Storage and Column Management

### 1. Google Sheets Column Mapping
```python
def get_email_column_index(email_num, section):
    """Get the column index for a specific email section."""
    base_indices = {
        1: {'subject': 53, 'icebreaker': 54, 'body': 55, 'cta': 56},      # BA-BD
        2: {'subject': 57, 'icebreaker': 58, 'body': 59, 'cta': 60},      # BE-BH
        3: {'subject': 61, 'icebreaker': 62, 'body': 63, 'cta': 64}       # BI-BL
    }
    
    if section in base_indices[email_num]:
        return base_indices[email_num][section]
    return None
```

### 2. Writing Emails to Sheets
```python
def write_emails_to_sheet(sheets_handler, sheet_url, sheet_name, sheet_row, emails):
    """Write emails to the correct columns in Google Sheets."""
    
    # Write emails to columns BA-BL (indices 53-64) for the correct sheet row
    for i, email in enumerate(emails, 1):
        print(f"    📝 Writing Email {i}...")
        
        # Get column indices for this email
        subject_col = get_email_column_index(i, 'subject')
        icebreaker_col = get_email_column_index(i, 'icebreaker')
        body_col = get_email_column_index(i, 'body')
        cta_col = get_email_column_index(i, 'cta')
        
        # Write each section
        try:
            # Subject
            if subject_col:
                cell_range = f"{sheet_name}!{chr(64 + subject_col)}{sheet_row}"
                sheets_handler.update_cell(cell_range, email.get('subject', ''))
            
            # Icebreaker
            if icebreaker_col:
                cell_range = f"{sheet_name}!{chr(64 + icebreaker_col)}{sheet_row}"
                sheets_handler.update_cell(cell_range, email.get('icebreaker', ''))
            
            # Body
            if body_col:
                cell_range = f"{sheet_name}!{chr(64 + body_col)}{sheet_row}"
                sheets_handler.update_cell(cell_range, email.get('body', ''))
            
            # CTA
            if cta_col:
                cell_range = f"{sheet_name}!{chr(64 + cta_col)}{sheet_row}"
                sheets_handler.update_cell(cell_range, email.get('cta', ''))
                
        except Exception as e:
            print(f"    ❌ Error writing email {i}: {e}")
    
    print(f"    ✅ Emails written to correct columns (BA-BD, BE-BH, BI-BL)")
    print(f"    🔒 ONLY wrote to the specified email columns")
    print(f"    💡 CTA hyperlink variables in BI & BN are preserved for your use")
```

## Quality Control and Validation

### 1. Email Quality Checks
```python
def validate_email_quality(emails):
    """Validate email quality before writing to sheets."""
    
    validation_errors = []
    
    for i, email in enumerate(emails, 1):
        # Check required fields
        required_fields = ['subject', 'icebreaker', 'body', 'cta']
        for field in required_fields:
            if field not in email or not email[field]:
                validation_errors.append(f"Email {i}: Missing {field}")
        
        # Check word count
        total_words = len(email.get('icebreaker', '').split()) + \
                     len(email.get('body', '').split()) + \
                     len(email.get('cta', '').split())
        
        if total_words > 150:
            validation_errors.append(f"Email {i}: Too long ({total_words} words, max 150)")
        
        # Check subject line length
        subject = email.get('subject', '')
        if len(subject) > 50:
            validation_errors.append(f"Email {i}: Subject too long ({len(subject)} chars, max 50)")
        
        # Check for personalization
        if not any(word in email.get('body', '').lower() for word in ['company', 'role', 'industry']):
            validation_errors.append(f"Email {i}: Missing company-specific personalization")
    
    return validation_errors
```

### 2. CTA Validation
```python
def validate_ctas(emails):
    """Ensure CTAs are compelling and specific."""
    
    cta_issues = []
    
    for i, email in enumerate(emails, 1):
        cta = email.get('cta', '').lower()
        
        # Check for generic CTAs
        generic_phrases = ['visit our website', 'contact us', 'learn more', 'get in touch']
        if any(phrase in cta for phrase in generic_phrases):
            cta_issues.append(f"Email {i}: Generic CTA detected")
        
        # Check CTA length
        if len(email.get('cta', '')) < 20:
            cta_issues.append(f"Email {i}: CTA too short")
        
        # Check for specific action words
        action_words = ['reply', 'visit', 'request', 'schedule', 'book', 'download']
        if not any(word in cta for word in action_words):
            cta_issues.append(f"Email {i}: Missing clear action in CTA")
    
    return cta_issues
```

## Advanced Personalization Techniques

### 1. Dynamic Content Insertion
```python
def enhance_personalization(email_content, research_data):
    """Enhance email content with dynamic personalization."""
    
    # Replace placeholders with actual data
    enhanced_content = email_content
    
    # Company-specific references
    enhanced_content = enhanced_content.replace(
        '[COMPANY_NAME]', research_data.get('company_name', '')
    )
    
    # Contact-specific references
    enhanced_content = enhanced_content.replace(
        '[CONTACT_NAME]', research_data.get('contact_name', '')
    )
    
    # Industry-specific references
    enhanced_content = enhanced_content.replace(
        '[INDUSTRY]', research_data.get('industry', '')
    )
    
    # Pain point integration
    pain_points = research_data.get('industry_pain_points', '')
    if pain_points:
        # Extract key pain points for dynamic insertion
        key_pain_points = extract_key_pain_points(pain_points)
        enhanced_content = enhanced_content.replace(
            '[KEY_PAIN_POINT]', key_pain_points[0] if key_pain_points else ''
        )
    
    return enhanced_content
```

### 2. Pain Point Integration
```python
def extract_key_pain_points(pain_points_text):
    """Extract key pain points from research text."""
    
    # Common pain point keywords
    pain_keywords = [
        'challenge', 'issue', 'problem', 'pressure', 'difficulty',
        'struggle', 'concern', 'hurdle', 'obstacle', 'bottleneck'
    ]
    
    sentences = pain_points_text.split('.')
    pain_point_sentences = []
    
    for sentence in sentences:
        if any(keyword in sentence.lower() for keyword in pain_keywords):
            pain_point_sentences.append(sentence.strip())
    
    return pain_point_sentences[:3]  # Return top 3 pain points
```

## Complete Email Generation Pipeline

### 1. Main Pipeline Function
```python
def run_email_generation_pipeline(company_data, research_data):
    """Complete email generation pipeline for a company."""
    
    print(f"📧 Generating emails for {company_data['company_name']}...")
    
    # Validate research data quality
    if research_data.get('research_quality', 0) < 6:
        print(f"    ⚠️ Research quality too low ({research_data['research_quality']}/10)")
        print(f"    📧 Skipping email generation")
        return None
    
    # Generate emails
    emails = generate_three_emails(
        company_name=company_data['company_name'],
        company_url=company_data['website'],
        contact_name=f"{company_data['first_name']} {company_data['last_name']}",
        contact_title=company_data['job_title'],
        company_summary=research_data['company_research'],
        contact_summary=research_data['contact_research'],
        pain_points=research_data['industry_pain_points'],
        opportunity_match=research_data['schreiber_opportunity'],
        first_name=company_data['first_name'],
        last_name=company_data['last_name'],
        industry=company_data.get('industry', ''),
        company_linkedin=company_data.get('company_linkedin', ''),
        contact_linkedin=company_data.get('contact_linkedin', '')
    )
    
    if not emails:
        print(f"    ❌ Failed to generate emails")
        return None
    
    # Validate email quality
    validation_errors = validate_email_quality(emails)
    if validation_errors:
        print(f"    ⚠️ Email validation issues:")
        for error in validation_errors:
            print(f"       - {error}")
    
    # Validate CTAs
    cta_issues = validate_ctas(emails)
    if cta_issues:
        print(f"    ⚠️ CTA validation issues:")
        for issue in cta_issues:
            print(f"       - {issue}")
    
    print(f"    ✅ Generated {len(emails)} emails")
    
    return emails
```

## Best Practices for Email Generation

### 1. Prompt Engineering
- **Be Specific**: Include all relevant company and contact details
- **Set Clear Format**: Specify exact output structure
- **Use Research Data**: Reference specific pain points and opportunities
- **Avoid Generic Language**: Ensure personalization throughout

### 2. Quality Control
- **Validate Research Quality**: Only generate emails for high-quality research (6+/10)
- **Check Personalization**: Ensure company-specific references
- **Validate CTAs**: Ensure compelling, specific calls to action
- **Word Count Limits**: Enforce 150-word maximum for main content

### 3. Error Handling
- **API Failures**: Implement retry logic and fallbacks
- **Parsing Errors**: Handle malformed AI responses gracefully
- **Data Validation**: Check all required fields before writing
- **Sheet Writing Errors**: Handle Google Sheets API failures

## Troubleshooting Common Issues

### 1. Poor Email Quality
- **Refine Prompts**: Make prompts more specific and detailed
- **Increase Temperature**: Use higher temperature (0.7-0.8) for creativity
- **Add Examples**: Include example emails in prompts
- **Validate Research**: Ensure research data quality is high

### 2. Generic Content
- **Enhance Prompts**: Add more personalization requirements
- **Use Research Data**: Ensure pain points are specifically referenced
- **Company References**: Require company name mentions throughout
- **Role Integration**: Include job title and responsibilities

### 3. CTA Issues
- **Specific Instructions**: Make CTA requirements more explicit
- **Action Words**: Require specific action verbs
- **Benefit Focus**: Ensure CTAs include clear benefits
- **Length Requirements**: Set minimum CTA length requirements

## Conclusion

This email generation approach provides:
- **Highly Personalized Content**: Based on actual research data
- **Consistent Quality**: Through validation and quality checks
- **Scalable Process**: Automated generation for multiple companies
- **Professional Results**: Ready-to-use cold email campaigns

The key to success is combining high-quality research data with well-crafted prompts and robust validation systems. This ensures every email is personalized, compelling, and ready for successful cold outreach campaigns.
