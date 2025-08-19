# AGT Data Enrichment - Working Scripts

This document describes the main working scripts for the AGT Data Enrichment pipeline.

## Working Scripts

### 01_research_pipeline.py
**Purpose**: Collects research data for companies and contacts using Perplexity Sonar API
**Status**: ⚠️ Has indentation issues - needs fixing
**What it does**:
- Reads company data from Google Sheets
- Researches companies, contacts, industry pain points, and opportunities
- Writes research data to columns AV-AZ

### 02_email_generator.py
**Purpose**: Generates 3 personalized cold emails using OpenAI API based on collected research data
**Status**: ✅ **WORKING PERFECTLY**
**What it does**:
- Reads research data from columns AV-AZ
- Generates 3 distinct emails with different CTAs:
  - Email 1: Reply to email
  - Email 2: Visit website  
  - Email 3: Request sample
- Writes emails to columns BA-BL (Subject, Icebreaker, Body, CTA for each email)

**Usage**:
```bash
python3 02_email_generator.py
```

**Note**: Currently configured to process row 5. Edit the `sheet_row = 5` line to change which row to process.

## Current Status

- ✅ **Row 2**: 8th Avenue Food and Provisions - Research + Emails ✅
- ✅ **Row 3**: AbiMar Foods - Research + Emails ✅  
- ✅ **Row 4**: ACH Food Companies - Research + Emails ✅
- ✅ **Row 5**: Annie's Homegrown - Research + Emails ✅

## Column Structure

- **AV-AZ**: Research data (Company Research, Contact Research, Industry Pain Points, Schreiber Opportunity, Research Quality Score)
- **BA-BD**: Email 1 (Subject, Icebreaker, Body, CTA)
- **BE-BH**: Email 2 (Subject, Icebreaker, Body, CTA)
- **BI-BL**: Email 3 (Subject, Icebreaker, Body, CTA)
- **BM-BN**: Reserved for hyperlink variables (DO NOT OVERWRITE)

## Next Steps

1. **Fix the research script** (`01_research_pipeline.py`) - resolve indentation issues
2. **Continue processing** additional rows as needed
3. **Maintain the working system** - the email generator is working perfectly!

## Dependencies

- `google_sheets_handler.py`
- `config.py` 
- `.env` file with API keys
- Required Python packages (see requirements.txt)
