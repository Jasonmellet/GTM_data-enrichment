# Contact Enrichment Status Guide

This document explains how to check the enrichment status of contacts in the Broadway SummerCampUSA database.

## Available Reports

Two contact enrichment reports have been generated for you in CSV format:

1. **Detailed CSV Report**: 
   - Location: `clients/Broadway/outputs/contact_enrichment_report.csv`
   - Contains all available data fields for each contact
   - Provides comprehensive view of enrichment status
   - Includes enrichment summary at the top

2. **Compact CSV Report**:
   - Location: `clients/Broadway/outputs/contact_enrichment_report_compact.csv`
   - Contains only the most essential fields for quick review
   - More readable format for quick analysis
   - Includes enrichment summary at the top

## Enrichment Summary

The reports include an enrichment summary at the top showing:
- Total contacts analyzed
- Percentage with direct emails
- Percentage with any email
- Percentage with phone numbers
- Percentage with verified addresses
- Percentage with verified phone numbers

## Key Data Tables

The contact enrichment data comes from these database tables:

1. **silver.organizations** - Base organization information
2. **silver.contacts** - Contact details (name, role)
3. **silver.emails** - Email addresses with direct/generic status
4. **silver.phones** - Phone numbers
5. **silver.locations** - Address and location data
6. **silver.scoring** - Fit score and outreach readiness
7. **silver.provenance** - Data source tracking

## Generating Custom CSV Reports

You can generate custom CSV reports using the `check_contact_enrichment.py` script:

```bash
python3 clients/Broadway/scripts/check_contact_enrichment.py --csv [options]
```

Options:
- `--org "Name"` - Filter by organization name
- `--contact-id 123` - Filter by contact ID
- `--direct-only` - Show only contacts with direct emails
- `--compact` - Show a simplified view with fewer columns
- `--limit 20` - Change the number of results (default: 10)
- `--output "path/to/file.csv"` - Specify custom output file path

Examples:
```bash
# Show only contacts with direct emails in CSV format
python3 clients/Broadway/scripts/check_contact_enrichment.py --direct-only --csv

# Filter by organization name and save to custom file
python3 clients/Broadway/scripts/check_contact_enrichment.py --org "Camp" --csv --output "my_report.csv"

# Generate compact CSV for a specific organization
python3 clients/Broadway/scripts/check_contact_enrichment.py --org "Chateaugay" --compact --csv
```

## Interpreting the Data

- **is_direct_email**: `True` means the email is a direct personal email, not a generic one
- **business_status**: Current status of the business (open, closed, etc.)
- **website_status**: HTTP status code from the last website check
- **fit_score**: How well the organization fits your target criteria
- **outreach_readiness**: How ready the contact is for outreach

## Improving Enrichment Status

To improve the enrichment status:
1. Focus on contacts without direct emails
2. Verify addresses and phone numbers
3. Update business status for organizations with unknown status
4. Fix website issues for organizations with non-200 status codes