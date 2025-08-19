# CSV Report Generation Guide

This document explains how to generate CSV reports for contact enrichment data in the Broadway SummerCampUSA project.

## Available CSV Reports

Two pre-generated CSV reports are available:

1. **Full CSV Report**: 
   - Location: `clients/Broadway/outputs/contact_enrichment_report.csv`
   - Contains all data fields for each contact
   - Includes enrichment summary at the top

2. **Compact CSV Report**:
   - Location: `clients/Broadway/outputs/contact_enrichment_report_compact.csv`
   - Contains only essential fields for quick analysis
   - Includes enrichment summary at the top

## Generating Custom CSV Reports

You can generate custom CSV reports using the simplified script that doesn't require database access:

```bash
python3 clients/Broadway/scripts/check_contact_enrichment_simple.py --csv [options]
```

### Options

- `--input FILE` - Specify input CSV file (default: contact_enrichment_report.csv)
- `--org "Name"` - Filter by organization name
- `--direct-only` - Show only contacts with direct emails
- `--compact` - Generate a simplified view with fewer columns
- `--output FILE` - Specify output file path (saved to clients/Broadway/outputs/)
- `--no-dedupe` - Skip deduplication of results (by default duplicates are removed)

### Examples

```bash
# Generate a CSV report for all camps
python3 clients/Broadway/scripts/check_contact_enrichment_simple.py --org "Camp" --csv --output "camps_report.csv"

# Generate a compact CSV report for camps with direct emails
python3 clients/Broadway/scripts/check_contact_enrichment_simple.py --org "Camp" --direct-only --compact --csv --output "direct_camps_compact.csv"

# Generate a report with duplicates included
python3 clients/Broadway/scripts/check_contact_enrichment_simple.py --org "Camp" --csv --no-dedupe --output "camps_with_duplicates.csv"
```

## CSV Report Structure

### Full Report Columns

The full CSV report includes all available data:

- Organization details (name, location, business status)
- Contact information (name, role, email, phone)
- Email quality indicators (is_direct_email, source)
- Location verification (maps_verified_address, maps_verified_phone)
- Website information (URL, status code)
- Scoring (fit_score, outreach_readiness)
- Categories and data sources

### Compact Report Columns

The compact CSV report includes only these essential columns:

- organization_name
- contact_name
- email
- is_direct_email
- phone_number
- city
- state
- business_status
- website_status
- fit_score
- outreach_readiness

## Enrichment Summary

Each CSV report includes an enrichment summary at the top showing:

- Total contacts analyzed
- Percentage with direct emails
- Percentage with any email
- Percentage with phone numbers
- Percentage with verified addresses
- Percentage with verified phones

This summary helps you quickly assess the quality of your contact data.

## Deduplication Logic

By default, the script removes duplicate entries based on organization name and contact name. When duplicates are found:

1. Only one entry per unique (organization_name, contact_name) pair is kept
2. If multiple entries exist for the same contact, the one with a direct email is preferred
3. Use `--no-dedupe` to preserve all entries if needed