#!/usr/bin/env python3
"""
Broadway Research-Only Pipeline
 - Reads local CSV
 - Collects research via Perplexity (company, contact, pain points, opportunity)
 - Writes results back into CSV columns
 - Supports concurrency
"""

import os
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time


load_dotenv()

# Perplexity API
PERPLEXITY_API_KEY = os.getenv('BROADWAY_PERPLEXITY_API_KEY') or os.getenv('PERPLEXITY_API_KEY')
PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"

# CSV file path (expanded schema)
CSV_FILE_PATH = "../data/Summer Camp Enrichment Sample Test.expanded.csv"

# Robust CSV read kwargs
CSV_READ_KWARGS = {
    'engine': 'python',
    'quotechar': '"',
    'escapechar': '\\',
    'on_bad_lines': 'skip',
    'dtype': str,
    'keep_default_na': False
}


def _clean_text(value):
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\r", " ").replace("\n", " ")
    return " ".join(s.split())


def collect_research_data(company_name, website, job_title, first_name, last_name, person_linkedin_url, company_linkedin_url):
    print(f"🔍 Collecting research for {company_name}...")

    company_prompt = f"""
    Research {company_name} ({website}) and provide a concise summary focusing on:
    1. Program/mission and primary audience
    2. Delivery model and distribution partners
    3. Core offerings (theater fundamentals, character development, life skills) and notable affiliations
    4. Launch timeline/scale
    """

    contact_prompt = f"""
    Research {first_name} {last_name}, {job_title} at {company_name}.
    Provide: background, responsibilities tied to education/program ops, and likely pain points.
    """

    pain_points_prompt = f"""
    List arts education/enrichment pain points relevant to {company_name}:
    - Curriculum alignment/assessment
    - Access & equity, teacher training
    - Licensing/rights and compliance
    - Scale & distribution (onboarding, PD, async content)
    - Funding & sustainability
    Provide concrete, program-operations examples.
    """

    opportunity_prompt = f"""
    Given {company_name}, explain succinctly where online modules + study guides fit (classroom/after‑school/camps),
    how partnerships accelerate distribution, how to measure outcomes, supports for adoption, and the lowest-friction next step.
    """

    research_data = {}

    def ask(prompt):
        try:
            resp = requests.post(
                PERPLEXITY_URL,
                headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
                json={"model": "sonar-pro", "messages": [{"role": "user", "content": prompt}]}
            )
            if resp.status_code == 200:
                return resp.json()["choices"][0]["message"]["content"].strip()
            return f"Research collection failed ({resp.status_code})"
        except Exception as e:
            return f"Error: {e}"

    research_data['company_summary'] = ask(company_prompt)
    research_data['contact_summary'] = ask(contact_prompt)
    research_data['pain_points'] = ask(pain_points_prompt)
    research_data['opportunity_match'] = ask(opportunity_prompt)

    # simple quality score
    quality_score = 10
    for v in research_data.values():
        if isinstance(v, str) and ("failed" in v.lower() or "error:" in v.lower()):
            quality_score -= 2
        elif len(v or "") < 50:
            quality_score -= 1
    research_data['quality_score'] = max(1, quality_score)
    return research_data


def classify_fit(company_name: str, website: str, raw_category: str, camp_type: str, description: str):
    """Classify if the business is a fit for Broadway MyWay via MTI.
    Return dict with: normalized_category, fit_decision (Yes/No/Maybe), fit_score (0-100), fit_reason, exclude_reason (optional),
    notes, recommended_segment (e.g., day camp / sleepaway / after-school), and taxonomy_decision (keep/replace/merge).
    """
    prompt = f"""
    You are evaluating whether a business is a good fit for the Camp Broadway MyWay program (Broadway Education Alliance),
    licensed to summer camps via MTI. Fit guidance:
    - Good fit: general summer camps (day or sleepaway) not focused on theater; organizations with kids attending venue-based programs.
    - Not ideal: dedicated theater camps with well-established theater programs; pure tech/academic businesses (e.g., Mathnasium, iCode).
    Input:
    - Company: {company_name}
    - Website: {website}
    - Given Category: {raw_category}
    - Camp Type: {camp_type}
    - Description: {description}

    1) Normalize to one category from: [General Camp, Sleepaway Camp, Day Camp, Sports Camp, Arts (non-theater), Theater Camp, STEM/Tech, Academic/Tutoring, Faith-based, Outdoor/Adventure, Equestrian, Other].
    2) Decide fit: Yes / No / Maybe. Score 0-100 where 100 = perfect fit.
    3) Provide short reason (1-2 sentences). If No, include concise exclude_reason.
    4) Decide taxonomy action: keep existing categories OR replace with normalized OR merge; return taxonomy_decision with a brief note.
    5) Recommend segment (day camp / sleepaway / after-school / other) based on signals.

    Respond as compact JSON with keys: normalized_category, fit_decision, fit_score, fit_reason, exclude_reason, taxonomy_decision, recommended_segment, notes.
    """
    try:
        resp = requests.post(
            PERPLEXITY_URL,
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
            json={"model": "sonar-pro", "messages": [{"role": "user", "content": prompt}]}
        )
        if resp.status_code == 200:
            content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "{}")
            # best-effort parse
            import json
            try:
                data = json.loads(content)
            except Exception:
                data = {"raw": content}
            return data
        return {"error": f"classification failed ({resp.status_code})"}
    except Exception as e:
        return {"error": str(e)}


def update_csv_with_research_only(csv_path, target_row_index: int, research_data):
    print(f"📝 Updating CSV row index {target_row_index}...")
    try:
        df = pd.read_csv(csv_path, **CSV_READ_KWARGS)
        if target_row_index < 0 or target_row_index >= len(df):
            print(f"❌ Row index {target_row_index} out of range")
            return

        research_columns = [
            'Company Research Summary',
            'Contact Research Summary',
            'Industry Pain Points',
            'Opportunity Match',
            'Research Quality Score'
        ]
        research_values = [
            _clean_text(research_data.get('company_summary', '')),
            _clean_text(research_data.get('contact_summary', '')),
            _clean_text(research_data.get('pain_points', '')),
            _clean_text(research_data.get('opportunity_match', '')),
            research_data.get('quality_score', 0)
        ]

        # Ensure classification columns
        class_columns = [
            'Normalized Category',
            'Fit Decision',
            'Fit Score',
            'Fit Reason',
            'Exclude Reason',
            'Taxonomy Decision',
            'Recommended Segment'
        ]

        for i, col in enumerate(research_columns):
            if col not in df.columns:
                df[col] = ""
            if str(df[col].dtype) != 'object':
                df[col] = df[col].astype(str)
            df.at[target_row_index, col] = research_values[i]
            print(f"  ✅ Updated {col}")

        for col in class_columns:
            if col not in df.columns:
                df[col] = ""

        df.to_csv(csv_path, index=False)
        print(f"✅ CSV updated (row {target_row_index})")
    except Exception as e:
        print(f"❌ Error updating CSV: {e}")


def process_row(df: pd.DataFrame, row_index: int):
    r = df.iloc[row_index]
    company_name = r.get('Company Name', '')
    website = r.get('Website URL', '')
    job_title = r.get('Job Title', '') or r.get('Designation', '')
    first_name = r.get('First Name', '')
    last_name = r.get('Last Name', '')
    person_linkedin_url = r.get('Contact Linkedin Url', '')
    company_linkedin_url = r.get('Company Linkedin Url', '')

    research_data = collect_research_data(
        company_name=company_name,
        website=website,
        job_title=job_title,
        first_name=first_name,
        last_name=last_name,
        person_linkedin_url=person_linkedin_url,
        company_linkedin_url=company_linkedin_url,
    )
    # classification
    raw_category = r.get('Business Category', '') or r.get('App Search Categories', '') or r.get('Camp Type', '')
    camp_type = r.get('Camp Type', '')
    description = r.get('Camp Description', '')
    class_data = classify_fit(company_name, website, raw_category, camp_type, description)
    research_data['classification'] = class_data
    return row_index, research_data


def main():
    if not PERPLEXITY_API_KEY:
        print("❌ PERPLEXITY_API_KEY not configured")
        return
    parser = argparse.ArgumentParser(description="Broadway research-only pipeline")
    parser.add_argument("--ids", nargs="+", help="Zero-based row indices to process", default=None)
    parser.add_argument("--all", action="store_true", help="Process all rows")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent workers")
    parser.add_argument("--csv", type=str, default=CSV_FILE_PATH, help="Path to expanded CSV")
    args, _ = parser.parse_known_args()

    csv_path = args.csv
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found: {csv_path}")
        return

    print("🚀 BROADWAY RESEARCH-ONLY PIPELINE")
    print("=" * 70)
    print(f"📖 Reading CSV file: {csv_path}")
    try:
        df = pd.read_csv(csv_path, **CSV_READ_KWARGS)
        print(f"📊 Found {len(df)} rows of data")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    if args.all:
        target_indices = list(range(len(df)))
    else:
        target_indices = [int(i) for i in (args.ids or [])]

    print(f"⚙️  Concurrency: {args.concurrency}")
    results = {}
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as ex:
        fut_to_idx = {ex.submit(process_row, df, i): i for i in target_indices}
        for fut in as_completed(fut_to_idx):
            idx = fut_to_idx[fut]
            try:
                row_index, research_data = fut.result()
                results[row_index] = research_data
            except Exception as e:
                print(f"❌ Error on row {idx}: {e}")
            time.sleep(random.uniform(0.05, 0.15))

    # Write sequentially
    for idx in sorted(results.keys()):
        # write research
        update_csv_with_research_only(csv_path, idx, results[idx])
        # write classification
        try:
            df = pd.read_csv(csv_path, **CSV_READ_KWARGS)
            cls = results[idx].get('classification', {}) or {}
            mapping = {
                'Normalized Category': cls.get('normalized_category', ''),
                'Fit Decision': cls.get('fit_decision', ''),
                'Fit Score': str(cls.get('fit_score', '')),
                'Fit Reason': cls.get('fit_reason', ''),
                'Exclude Reason': cls.get('exclude_reason', ''),
                'Taxonomy Decision': cls.get('taxonomy_decision', ''),
                'Recommended Segment': cls.get('recommended_segment', ''),
            }
            for col, val in mapping.items():
                if col not in df.columns:
                    df[col] = ""
                df.at[idx, col] = _clean_text(val)
            df.to_csv(csv_path, index=False)
            print(f"  ✅ Classification written for row {idx}")
        except Exception as e:
            print(f"  ⚠️ Classification write failed for row {idx}: {e}")
        print(f"🎉 COMPLETE for row {idx}")

    print("\n✅ Batch complete")


if __name__ == "__main__":
    main()


