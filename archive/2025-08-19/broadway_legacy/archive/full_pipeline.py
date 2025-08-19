#!/usr/bin/env python3
"""
One-command full pipeline for Broadway (SummerCampUSA):
- Idempotent upserts from CSV (organizations, contacts, locations) with mapping
- Enrichment per row: crawl + Maps + staged Perplexity (name->email)
- Taxonomy classification (Definitive Categories) persisted to silver.categories/org_categories
- Scoring refresh and export
- Null report for any residual empty core fields

Usage:
  python3 full_pipeline.py --csv "/abs/path/to.csv" --ids 251,252,253
  python3 full_pipeline.py --csv "/abs/path/to.csv" --all
"""

import argparse
import csv
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# project root
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(ROOT)

from config import get_db_connection

CSV_DEFAULT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/Summer Camp Enrichment Sample Test.expanded.csv"))

CORE_EMPTY_FIELDS = [
    "website_domain", "email", "phone_e164", "street", "city", "state", "postal_code", "country",
    "Definitive Categories", "Fit Score", "First Name", "Last Name", "Job Title"
]


def normalize_domain(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    if not url.startswith("http"):
        url = "https://" + url
    host = url.split("//", 1)[-1].split("/", 1)[0].lower()
    if host.startswith("www."):
        host = host[4:]
    host = host.split(":")[0]
    return host


def ensure_indexes() -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # unique-ish on website domain
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_orgs_domain
                ON silver.organizations ((NULLIF(website_domain,'')))
                WHERE website_domain IS NOT NULL;
            """)
            # contacts unique per org by full_name when present
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_contacts_org_name
                ON silver.contacts (org_id, full_name)
                WHERE COALESCE(full_name,'') <> '';
            """)
            # csv mapping table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS silver.csv_contact_map (
                  csv_contact_id BIGINT PRIMARY KEY,
                  org_id BIGINT REFERENCES silver.organizations(org_id),
                  contact_id BIGINT REFERENCES silver.contacts(contact_id)
                );
            """)
            conn.commit()


def upsert_org_and_contact(row: Dict[str, str]) -> Tuple[int, int]:
    """Return (org_id, contact_id)."""
    name = (row.get("legal_name") or "").strip()
    website = (row.get("website_domain") or "").strip()
    domain = normalize_domain(website)
    if not name:
        core = domain.split(".")[0] if domain else ""
        name = core.capitalize() if core else ""
    first = (row.get("First Name") or "").strip()
    last = (row.get("Last Name") or "").strip()
    full = (first + (" " if last else "") + last).strip()
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # organizations upsert by domain
            cur.execute("SELECT org_id FROM silver.organizations WHERE website_domain = %s", (website,))
            r = cur.fetchone()
            if r:
                org_id = r[0]
                cur.execute("UPDATE silver.organizations SET display_name = COALESCE(%s, display_name) WHERE org_id = %s", (name or None, org_id))
            else:
                cur.execute(
                    "INSERT INTO silver.organizations (legal_name, display_name, website_domain) VALUES (%s,%s,%s) RETURNING org_id",
                    (name or domain or "", name or domain or "", website),
                )
                org_id = cur.fetchone()[0]
            # contacts upsert by org_id, full_name (when available)
            contact_id: Optional[int] = None
            if full:
                cur.execute("SELECT contact_id FROM silver.contacts WHERE org_id = %s AND full_name = %s", (org_id, full))
                r = cur.fetchone()
                if r:
                    contact_id = r[0]
                    cur.execute(
                        "UPDATE silver.contacts SET role_title = COALESCE(%s, role_title) WHERE contact_id = %s",
                        ((row.get("Job Title") or row.get("Designation") or None), contact_id),
                    )
            if contact_id is None:
                cur.execute(
                    "INSERT INTO silver.contacts (org_id, full_name, first_name, last_name, role_title) VALUES (%s,%s,%s,%s,%s) RETURNING contact_id",
                    (org_id, full, first, last, row.get("Job Title") or row.get("Designation") or ""),
                )
                contact_id = cur.fetchone()[0]
            # ensure a locations stub
            cur.execute("SELECT 1 FROM silver.locations WHERE org_id = %s", (org_id,))
            if cur.fetchone() is None:
                cur.execute(
                    "INSERT INTO silver.locations (org_id, city, region, country, business_status) VALUES (%s,%s,%s,%s,%s)",
                    (org_id, row.get("City") or "", row.get("State/Region*") or row.get("State/Region") or "", row.get("Country/Region") or "", row.get("Business Status") or "open"),
                )
            # map csv id to db ids
            csv_id = int(row.get("org_id") or 0)
            if csv_id:
                cur.execute(
                    "INSERT INTO silver.csv_contact_map (csv_contact_id, org_id, contact_id) VALUES (%s,%s,%s) ON CONFLICT (csv_contact_id) DO UPDATE SET org_id=EXCLUDED.org_id, contact_id=EXCLUDED.contact_id",
                    (csv_id, org_id, contact_id),
                )
            conn.commit()
            return org_id, contact_id


def run_crawler(csv_path: str, csv_contact_id: int) -> None:
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(__file__), "Broadway_site_crawler_module.py"),
        "--id", str(csv_contact_id),
        "--csv", csv_path,
        "--verify-with-maps",
        "--maps-max-calls", "1",
        "--show-usage",
        "--update-csv",
    ]
    subprocess.run(cmd, check=False)


def persist_categories_for_org(org_id: int, definitive_cats: str) -> None:
    if not definitive_cats:
        return
    cats = [c.strip() for c in definitive_cats.split(",") if c.strip()]
    if not cats:
        return
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for label in cats:
                slug = label.lower().replace("/", "-").replace(" ", "-")
                cur.execute(
                    "INSERT INTO silver.categories (slug, label) VALUES (%s,%s) ON CONFLICT (slug) DO UPDATE SET label=EXCLUDED.label RETURNING category_id",
                    (slug, label),
                )
                try:
                    category_id = cur.fetchone()[0]
                except Exception:
                    cur.execute("SELECT category_id FROM silver.categories WHERE slug=%s", (slug,))
                    category_id = cur.fetchone()[0]
                cur.execute(
                    "INSERT INTO silver.org_categories (org_id, category_id, confidence) VALUES (%s,%s,%s) ON CONFLICT (org_id, category_id) DO UPDATE SET confidence=EXCLUDED.confidence",
                    (org_id, category_id, 90),
                )
            conn.commit()


def run_taxonomy_and_persist(csv_path: str, csv_contact_id: int) -> None:
    # read website URL for this row
    website = ""
    with open(csv_path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            if str(row.get("org_id", "")).strip() == str(csv_contact_id):
                website = row.get("website_domain", "")
                break
    if not website:
        return
    # call perplexity_lookup via a small inline runner to reuse existing code
    code = f"""
import json, os
from clients.Broadway.scripts.crawler_enrich_contact import perplexity_lookup
res = perplexity_lookup('{website}', verbose=False, stats=None)
print(json.dumps(res))
"""
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    try:
        data = out.stdout.strip()
        res = {}
        if data:
            import json as _json
            res = _json.loads(data)
    except Exception:
        res = {}
    definitive = res.get("Definitive Categories") if isinstance(res, dict) else ""
    if not definitive:
        return
    # fetch org_id from mapping
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT org_id FROM silver.csv_contact_map WHERE csv_contact_id = %s", (csv_contact_id,))
            r = cur.fetchone()
            if not r:
                return
            org_id = r[0]
    persist_categories_for_org(org_id, definitive)


def refresh_scoring_and_export() -> None:
    subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "update_scoring_v3.py")], check=False)
    subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), "export_complete_dataset.py")], check=False)


def build_null_report(export_csv: str, out_csv: str) -> None:
    rows: List[Dict[str, str]] = []
    with open(export_csv, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            empties = [c for c in CORE_EMPTY_FIELDS if (row.get(c, "") or "").strip() == ""]
            if empties:
                missing_reasons = []
                next_actions = []
                
                for field in empties:
                    reason = "Unknown"
                    action = "Manual review required"
                    
                    # Determine reason and action based on field type
                    if field == "Email":
                        reason = "No email found during crawl or enrichment"
                        action = "Run targeted_email_enrichment.py or apollo_email_lookup.py"
                    elif field == "website_domain":
                        reason = "No website URL in original data"
                        action = "Manual research or use perplexity_lookup"
                    elif field == "Company Phone":
                        reason = "No phone found during crawl or Maps lookup"
                        action = "Run yelp_business_lookup.py or manual research"
                    elif field == "Street Address" or field == "City" or field == "State/Region" or field == "Postal Code":
                        reason = "No address found during crawl or Maps lookup"
                        action = "Run crawler with --verify-with-maps or use yelp_business_lookup.py"
                    elif field == "Definitive Categories":
                        reason = "Taxonomy classification not run or failed"
                        action = "Run perplexity_taxonomy_test.py with --force-taxonomy"
                    elif field == "First Name" or field == "Last Name" or field == "Job Title":
                        reason = "Contact details not found during enrichment"
                        action = "Run perplexity_person_email_lookup for primary contact discovery"
                    
                    missing_reasons.append(f"{field}: {reason}")
                    next_actions.append(f"{field}: {action}")
                
                rows.append({
                    "org_id": row.get("org_id", ""),
                    "display_name": row.get("display_name", ""),
                    "missing_fields": ", ".join(empties),
                    "missing_reason": "; ".join(missing_reasons),
                    "next_action": "; ".join(next_actions)
                })
    if rows:
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
        with open(out_csv, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["org_id", "display_name", "missing_fields", "missing_reason", "next_action"])
            w.writeheader(); w.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description="Full pipeline orchestrator")
    parser.add_argument("--csv", default=CSV_DEFAULT, help="Path to expanded CSV")
    parser.add_argument("--ids", default="", help="Comma-separated CSV org_ids to process; blank means none")
    parser.add_argument("--all", action="store_true", help="Process all rows in CSV")
    parser.add_argument("--null-report-only", action="store_true", help="Only generate null report without running pipeline")
    parser.add_argument("--export-path", help="Path to export CSV for null report")
    parser.add_argument("--null-path", help="Path to output null report CSV")
    args = parser.parse_args()

    ensure_indexes()

    # Handle null report only mode
    if args.null_report_only:
        export_csv = args.export_path or os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/complete_enriched_dataset.csv"))
        null_csv = args.null_path or os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/null_report.csv"))
        build_null_report(export_csv, null_csv)
        print(f"\n✅ Null report generated: {null_csv}")
        return

    # select rows
    target_ids: List[int] = []
    if args.all:
        with open(args.csv, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            target_ids = [int(row.get("org_id") or 0) for row in r if (row.get("website_domain") or "").strip()]
    elif args.ids:
        target_ids = [int(s.strip()) for s in args.ids.split(",") if s.strip()]

    if not target_ids:
        print("No targets specified; use --ids or --all")
        sys.exit(1)

    # load and map, enrich, taxonomy per id
    with open(args.csv, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = {row["org_id"]: row for row in r}
    for cid in target_ids:
        row = rows.get(str(cid))
        if not row:
            print(f"Skip id {cid} (not in CSV)")
            continue
        org_id, contact_id = upsert_org_and_contact(row)
        print(f"Upserted id {cid} -> org_id {org_id}, contact_id {contact_id}")
        run_crawler(args.csv, cid)
        run_taxonomy_and_persist(args.csv, cid)

    # finalize
    refresh_scoring_and_export()
    export_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/complete_enriched_dataset.csv"))
    null_csv = os.path.abspath(os.path.join(os.path.dirname(__file__), "../outputs/null_report.csv"))
    build_null_report(export_csv, null_csv)
    print("\n✅ Full pipeline complete.")
    print(f"Export: {export_csv}")
    print(f"Null report (if any missing fields): {null_csv}")


if __name__ == "__main__":
    main()
