# Quick Start: Adding a New Client

## Overview
This guide walks you through adding a new client to the AGT Data Enrichment platform in under 10 minutes.

## 🚀 Step-by-Step Process

### 1. Create Client Directory Structure
```bash
# From the project root
mkdir -p clients/[CLIENT_NAME]/{data,scripts,outputs,config}
```

**Example:**
```bash
mkdir -p clients/acme_corp/{data,scripts,outputs,config}
```

### 2. Copy the Onboarding Template
```bash
cp templates/config_templates/client_onboarding_template.py clients/[CLIENT_NAME]/config/client_config.py
```

**Example:**
```bash
cp templates/config_templates/client_onboarding_template.py clients/acme_corp/config/client_config.py
```

### 3. Customize the Configuration File
Edit `clients/[CLIENT_NAME]/config/client_config.py` and update these key sections:

#### Basic Client Information
```python
CLIENT_NAME = "Acme Corporation"                    # Your client's name
CLIENT_INDUSTRY = "Software Development"            # Their industry
CLIENT_PRODUCT = "AI-Powered Analytics Platform"    # Your product/service
CLIENT_WEBSITE = "https://www.acmecorp.com"         # Your website
```

#### Email Structure (Customize CTAs)
```python
EMAIL_STRUCTURE = {
    'email_1': {
        'cta_type': 'reply_to_email',
        'cta_description': 'Simply reply to this email'
    },
    'email_2': {
        'cta_type': 'visit_website',
        'cta_description': 'Visit our website',
        'website_url': 'https://www.acmecorp.com/demo'  # Update this
    },
    'email_3': {
        'cta_type': 'request_demo',                    # Customize this
        'cta_description': 'Request a demo',           # Customize this
        'demo_url': 'https://www.acmecorp.com/demo'    # Update this
    }
}
```

#### Google Sheets Column Mapping
Update these to match your Google Sheet structure:
```python
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
        # ... continue for email_2 and email_3
    }
}
```

#### Research Prompts (Industry-Specific)
Customize these prompts for your industry:
```python
RESEARCH_PROMPTS = {
    'company': """Research {company_name} ({website}) and provide a concise summary including:
1. Company description and mission
2. Main products and services  
3. Company size and scale
4. Industry focus
5. Key business areas that might use [YOUR_PRODUCT]  # Update this

Return a clean, actionable summary in 3-4 sentences.""",
    
    'pain_points': """Research the [INDUSTRY] pain points that {company_name} likely faces:  # Update industry
1. [INDUSTRY_SPECIFIC_CHALLENGE_1]  # Update these
2. [INDUSTRY_SPECIFIC_CHALLENGE_2]  # Update these
3. [INDUSTRY_SPECIFIC_CHALLENGE_3]  # Update these
4. [INDUSTRY_SPECIFIC_CHALLENGE_4]  # Update these
5. [INDUSTRY_SPECIFIC_CHALLENGE_5]  # Update these

Provide specific, industry-relevant pain points that can be addressed in cold emails.""",
    
    # ... continue for other prompts
}
```

### 4. Copy and Customize Scripts
```bash
# Copy the Schreiber scripts as a starting point
cp clients/schreiber/scripts/*.py clients/[CLIENT_NAME]/scripts/

# Update the scripts to use your client config
# In each script, change the import from:
# from config.client_config import *
# to:
# import sys
# sys.path.append('../config')
# from client_config import *
```

### 5. Create Client README
```bash
# Copy and customize the Schreiber README
cp clients/schreiber/README.md clients/[CLIENT_NAME]/README.md

# Edit the README with client-specific information
```

### 6. Add Your Data
```bash
# Place your CSV file in the client's data folder
cp your_contacts.csv clients/[CLIENT_NAME]/data/
```

### 7. Test the Setup
```bash
# Navigate to the client scripts directory
cd clients/[CLIENT_NAME]/scripts

# Test with a small dataset first
python super_pipeline.py
```

## 🔧 Configuration Checklist

Before running, ensure you've completed:

- [ ] **Client Information**: Name, industry, product, website
- [ ] **Email Structure**: CTAs and URLs customized for your business
- [ ] **Column Mapping**: Matches your Google Sheet structure
- [ ] **Research Prompts**: Industry-specific and relevant to your product
- [ ] **API Keys**: Perplexity and OpenAI keys in your `.env` file
- [ ] **Google Sheets**: URL and permissions set up correctly

## 📊 Example Customization

### For a Software Company:
```python
CLIENT_NAME = "TechFlow Solutions"
CLIENT_INDUSTRY = "Software Development"
CLIENT_PRODUCT = "Project Management Platform"
RESEARCH_PROMPTS = {
    'pain_points': """Research the software development pain points that {company_name} likely faces:
1. Project timeline management
2. Team collaboration challenges
3. Resource allocation issues
4. Client communication bottlenecks
5. Quality assurance processes

Provide specific, industry-relevant pain points that can be addressed in cold emails."""
}
```

### For a Marketing Agency:
```python
CLIENT_NAME = "Growth Marketing Pro"
CLIENT_INDUSTRY = "Digital Marketing"
CLIENT_PRODUCT = "Marketing Automation Services"
RESEARCH_PROMPTS = {
    'pain_points': """Research the digital marketing pain points that {company_name} likely faces:
1. Lead generation challenges
2. Marketing ROI measurement
3. Customer acquisition costs
4. Campaign optimization
5. Marketing technology integration

Provide specific, industry-relevant pain points that can be addressed in cold emails."""
}
```

## 🚨 Common Pitfalls

1. **Column Mapping Mismatch**: Ensure your Google Sheet columns match the configuration
2. **Generic Prompts**: Customize research prompts for your specific industry and product
3. **Missing API Keys**: Verify all required API keys are in your `.env` file
4. **Wrong File Paths**: Double-check directory structure and file locations
5. **Uncustomized CTAs**: Make sure CTAs are specific to your business model

## ✅ Success Indicators

Your setup is ready when:
- [ ] Scripts run without import errors
- [ ] Research data is collected and stored correctly
- [ ] Emails are generated with proper personalization
- [ ] Data flows correctly to Google Sheets
- [ ] Quality scores are reasonable (6+/10)

## 🔄 Next Steps

After successful setup:
1. **Scale Up**: Process larger datasets
2. **Optimize**: Refine prompts based on results
3. **Analyze**: Review research quality and email performance
4. **Iterate**: Improve based on feedback and results

## 📞 Need Help?

- Check the [main documentation](README.md)
- Review [research script guide](research_script.md)
- Review [email writer guide](email_writer_script.md)
- Use the test scripts for troubleshooting
- Check the onboarding template for reference
