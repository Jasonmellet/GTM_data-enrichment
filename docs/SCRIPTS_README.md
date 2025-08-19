# Scripts Overview (Standardized Names)

This repo contains two client workspaces; Broadway naming is standardized to clarity.

## Broadway Active Scripts

- Data cleanup: `clients/Broadway/scripts/data_cleanup.py`
- DB loader: `clients/Broadway/scripts/db_loader.py`
- Web crawler: `clients/Broadway/scripts/web_crawler.py`
- Perplexity enricher: `clients/Broadway/scripts/perplexity_enricher.py`
- Apollo enricher: `clients/Broadway/scripts/apollo_enricher.py`
- Email discovery: `clients/Broadway/scripts/email_discovery.py` (renamed from `enhanced_email_discovery.py`)
- Catch-all migrator: `clients/Broadway/scripts/email_catchall_migrator.py` (renamed from `move_catchall_contacts.py`)

### Run examples

- Email discovery (batch):
  `python3 clients/Broadway/scripts/email_discovery.py --all-contacts --limit 50 --workers 6`
- Catch-all migrator (dry-run):
  `python3 clients/Broadway/scripts/email_catchall_migrator.py --limit 50 --dry-run`

## Notes

- Secret values are placeholders only (`env_example.txt`). Use a private `.env` for real keys.
- Archived tests/legacy scripts are under `archive/2025-08-19/`.
