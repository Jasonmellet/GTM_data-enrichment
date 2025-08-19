# Scripts Overview (Standardized Names)

This repo contains two client workspaces; Broadway naming is standardized to clarity.

## Broadway Active Scripts (ordered A→H)

- A: `clients/Broadway/scripts/A_data_cleanup.py`
- B: `clients/Broadway/scripts/B_db_loader.py`
- C: `clients/Broadway/scripts/C_web_crawler.py`
- D: `clients/Broadway/scripts/D_perplexity_enricher.py`
- E: `clients/Broadway/scripts/E_apollo_enricher.py`
- F: `clients/Broadway/scripts/F_email_discovery.py`
- G: `clients/Broadway/scripts/G_email_catchall_migrator.py`

### Run examples

- Email discovery (batch):
  `python3 clients/Broadway/scripts/F_email_discovery.py --all-contacts --limit 50 --workers 6`
- Catch-all migrator (dry-run):
  `python3 clients/Broadway/scripts/G_email_catchall_migrator.py --limit 50 --dry-run`

## Notes

- Secret values are placeholders only (`env_example.txt`). Use a private `.env` for real keys.
- Archived tests/legacy scripts are under `archive/2025-08-19/`.
