# Archive Manifest

This directory contains archived scripts from the Broadway data enrichment project. These scripts were part of the development process but have been superseded by newer implementations.

## Archived Scripts

### Development Scripts

- `add_columns.py`: Early script to add columns to the CSV
- `add_contact_ids.py`: Script to add Contact ID column to the CSV
- `cleanup_csv.py`: Initial CSV cleaning script
- `enrich_contact_details.py`: Early web scraping implementation
- `maps_lookup.py`: Test script for Google Maps API integration
- `perplexity_taxonomy_test.py`: Test script for taxonomy classification
- `reorder_columns.py`: Script to standardize column order in CSV
- `research_only_pipeline.py`: Research-only pipeline without email generation
- `super_pipeline.py`: Adapted from Schreiber client pipeline

### Superseded Pipeline Scripts

- `crawler_enrich_contact.py`: Original crawler script, replaced by `Broadway_site_crawler_module.py`
- `broadway_full_pipeline.py`: Earlier orchestration script, replaced by `full_pipeline.py` and `full_enrichment_pipeline.py`

## Current Pipeline

The current pipeline consists of:

1. `Broadway_site_crawler_module.py`: Web crawling and data extraction
2. `full_pipeline.py`: Core pipeline orchestration
3. `full_enrichment_pipeline.py`: Complete pipeline with all data sources
4. `update_scoring_v3.py`: Scoring and gold view generation
5. `export_complete_dataset.py`: Dataset export
6. `apollo_email_lookup.py`: Apollo API integration
7. `yelp_business_lookup.py`: Yelp API integration
8. `targeted_email_enrichment.py`: Direct email discovery
9. `identify_missing_emails.py`: Email status analysis
10. Database scripts: `db_bootstrap.sql`, `db_extend.sql`, `db_persist_enrichment.py`

## Archive Date

This archive was created on 2025-08-18.
