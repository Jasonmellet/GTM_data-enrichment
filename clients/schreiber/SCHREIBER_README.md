# Schreiber Foods - Data Enrichment Project

## Overview

This is the client-specific project folder for Schreiber Foods. The project enriches contact data with research and generates three compliant, personalized cold emails per contact.

## Leadership Overview

- **What we built**: An automated workflow that researches each contact and drafts three short, personalized email options.
- **How it works**:
  - **Research**: An AI research API gathers company/role insights, likely pain points, and where Schreiber’s product fits.
  - **Writing**: An AI writing model uses that research to produce 3 distinct versions in clear, compliant language.
  - **Quality**: We score research, enforce length/tone rules, remove spammy elements, and keep subjects/CTAs unique.
  - **Scale**: Many API calls run in parallel with safe saves, enabling hundreds of unique emails quickly.
- **Why it works**: Each email is grounded in specific company/role insights, so it feels truly personalized at scale while guardrails keep quality consistent.
- **Outcome**: ~900 personalized emails generated quickly and reliably, ready to ship.

## Project Structure

```text
schreiber/
├── data/           # CSV imports and data files
├── scripts/        # Client-specific scripts
├── outputs/        # Generated emails, research data
├── config/         # Client-specific configurations
└── README.md       # This file
```

## Key Features

- **Research Pipeline**: Automated company and contact research using Perplexity Sonar API
- **Email Generation**: AI-powered personalized cold email creation using Anthropic (Claude)
- **Local CSV Workflow**: Works against a local CSV for speed and reliability
- **Quality Control**: Research quality scoring and email validation/guardrails

## Scripts

- `super_pipeline.py` - Complete research + email generation pipeline (supports `--concurrency`)
- `01_research_pipeline.py` - Research collection only
- `02_email_generator.py` - Email generation only

## Configuration

All client-specific settings are in `config/client_config.py`:

- API keys and endpoints
- Google Sheets configuration
- Research and email prompts
- Column mappings
- Quality thresholds

## Data Flow

1. **CSV Import** → `data/` folder
2. **Research Collection** → Perplexity API → CSV columns AV–AZ
3. **Email Generation** → Anthropic (Claude) → CSV columns BA–BL
4. Optional: export consolidated outputs to `outputs/`

## Usage

1. Place your CSV file in the `data/` folder
2. Update configuration in `config/client_config.py` if needed
3. From `scripts/`, run a batch with concurrency, e.g.:
   - `python3 super_pipeline.py --ids 21 22 23 --concurrency 6`
   - or process all: `python3 super_pipeline.py --all --concurrency 6`
4. Check outputs in the `outputs/` folder

## Research Quality Threshold

Emails are only generated for contacts with research quality scores of 6/10 or higher.

## Column Mappings

- **Research Data**: AV (Company), AW (Contact), AX (Pain Points), AY (Opportunity), AZ (Quality)
- **Email 1**: BA (Subject), BB (Icebreaker), BC (Body), BD (CTA)
- **Email 2**: BE (Subject), BF (Icebreaker), BG (Body), BH (CTA)
- **Email 3**: BI (Subject), BJ (Icebreaker), BK (Body), BL (CTA)

## Notes

- All scripts are configured for Schreiber Foods
- Prompts are tailored for the dairy/food industry
- CTAs mapping is enforced (reply, visit, sample)
- Research focuses on food manufacturing pain points

## See Also

- `LESSONS_LEARNED.md` – end‑to‑end approach, concurrency model, tips, and pitfalls
