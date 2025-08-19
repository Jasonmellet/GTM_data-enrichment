# Broadway Data Enrichment Pipeline Documentation

## Overview

The Broadway Data Enrichment Pipeline is a comprehensive system for collecting, enriching, and managing data about summer camps and educational programs. The pipeline includes:

1. **Web Crawling & Scraping**: Extracting data from websites, including contact details, social media links, and business attributes
2. **API Integrations**: Using Google Maps, Perplexity, Apollo, and Yelp for data verification and enrichment
3. **Database Storage**: Structured PostgreSQL database with raw, bronze, silver, and gold layers
4. **Deduplication & Entity Resolution**: Ensuring data quality and removing duplicates
5. **Scoring & Classification**: Evaluating fit and outreach readiness
6. **Export & Reporting**: Generating comprehensive datasets and identifying missing data

## Architecture

### Database Schema

The database follows a medallion architecture:

- **Raw**: Original, unmodified data
- **Bronze**: Cleaned, normalized data
- **Silver**: Enriched data with relationships
- **Gold**: Analytics-ready views

### Core Tables

- `silver.organizations`: Company information
- `silver.locations`: Address and geographic data
- `silver.contacts`: People associated with organizations
- `silver.emails`: Email addresses (with source and verification)
- `silver.phones`: Phone numbers
- `silver.websites`: Website URLs and status
- `silver.socials`: Social media links
- `silver.categories`: Taxonomy of camp types
- `silver.org_categories`: Organization-category relationships
- `silver.scoring`: Fit and outreach readiness scores
- `silver.provenance`: Data source tracking
- `silver.api_usage`: API call tracking and costs

### Views

- `gold.outreach_today_v`: Outreach-ready contacts with direct emails

## Pipeline Components

### 1. Data Ingestion & Mapping

The pipeline starts by loading data from CSV files and mapping it to the database schema:

```python
# Load CSV data into silver tables
python clients/Broadway/scripts/db_load_csv_to_silver.py --csv "/path/to/csv"
```

### 2. Web Crawling & Scraping

The crawler module visits websites, extracts data, and persists it to the database:

```python
# Run crawler for specific contact
python clients/Broadway/scripts/Broadway_site_crawler_module.py --id 123 --csv "/path/to/csv" --verify-with-maps
```

Features:
- Sitemap discovery
- Social media link extraction
- Email and phone extraction
- Business attribute extraction
- JSON-LD structured data parsing
- Robots.txt analysis
- JavaScript rendering (optional)
- Google Maps verification

### 3. Email Enrichment

Multiple methods for finding direct email addresses:

```python
# Targeted email enrichment
python clients/Broadway/scripts/targeted_email_enrichment.py --org-id 123 --contact-id 456

# Apollo API fallback
python clients/Broadway/scripts/apollo_email_lookup.py --org-id 123 --contact-id 456 --first-name "John" --last-name "Doe" --company "Camp Example"
```

### 4. Business Verification

Additional data sources for business verification:

```python
# Yelp business lookup
python clients/Broadway/scripts/yelp_business_lookup.py --org-id 123 --company "Camp Example" --location "New York, NY"
```

### 5. Scoring & Classification

Scoring system evaluates fit and outreach readiness:

```python
# Update scoring and gold view
python clients/Broadway/scripts/update_scoring_v3.py
```

Scoring factors:
- Direct vs. generic emails
- Phone verification
- Business status
- Maps verification
- Category count

### 6. Export & Reporting

Generate comprehensive datasets and reports:

```python
# Export complete dataset
python clients/Broadway/scripts/export_complete_dataset.py

# Generate null report
python clients/Broadway/scripts/full_pipeline.py --null-report-only
```

## Full Pipeline Orchestration

The full enrichment pipeline combines all components:

```python
# Run full pipeline for specific contacts
python clients/Broadway/scripts/full_enrichment_pipeline.py --csv "/path/to/csv" --ids 1,2,3

# Run for all contacts
python clients/Broadway/scripts/full_enrichment_pipeline.py --csv "/path/to/csv" --all

# Skip specific API integrations
python clients/Broadway/scripts/full_enrichment_pipeline.py --ids 1,2,3 --no-apollo --no-yelp
```

## Data Quality & Diagnostics

The pipeline includes several diagnostic tools:

1. **Email Status Report**: Identifies direct, generic, and missing emails
2. **Null Report**: Lists missing fields with reasons and next actions
3. **Dataset Summary**: Provides statistics on the complete dataset

## API Cost Management

The pipeline tracks API usage and costs:

- Google Maps: ~$0.034 per lookup (Find Place + Place Details)
- Perplexity: ~$0.005 per query
- Apollo: ~$0.10 per lookup
- Yelp: ~$0.05 per lookup

## Best Practices

1. **Direct Emails**: Prioritize finding direct email addresses over generic ones
2. **Deduplication**: Use `DISTINCT ON` in SQL queries to avoid duplicates
3. **Incremental Processing**: Process contacts in small batches
4. **API Fallbacks**: Use multiple data sources with fallback logic
5. **Cost Control**: Set maximum API call limits

## Troubleshooting

Common issues and solutions:

1. **Missing Emails**: Run `targeted_email_enrichment.py` or `apollo_email_lookup.py`
2. **Missing Addresses**: Run crawler with `--verify-with-maps` or use `yelp_business_lookup.py`
3. **Missing Categories**: Run with `--force-taxonomy` flag
4. **Database Errors**: Check constraints and schema compatibility
5. **Duplicate Contacts**: Verify the `gold.outreach_today_v` view is using `DISTINCT ON`

## Future Improvements

Potential enhancements:

1. **SMTP Verification**: Add email deliverability checking
2. **Batch Processing**: Improve concurrency for large datasets
3. **Machine Learning**: Enhance fit scoring with ML models
4. **UI Dashboard**: Create a web interface for pipeline monitoring
5. **API Rate Limiting**: Implement adaptive throttling based on API responses
