# Schreiber – How We Did It / Lessons Learned

## Goal

Enrich a client CSV with research (company, contact, industry pains, opportunity match) and generate 3 compliant, personalized emails per contact. Complete end‑to‑end for the full file reliably and fast.

## Leadership Overview

- **What we built**: An automated system that researches each contact’s company and role, then drafts three personalized email options per person.
- **How it works**:
  - **Research**: An AI research API pulls concise company/role insights, pain points, and where Schreiber fits.
  - **Writing**: An AI writing model turns that into three distinct, compliant emails.
  - **Quality**: Research is scored, messages follow clear length/tone rules, and we remove anything that looks spammy; subjects/CTAs are kept unique.
  - **Scale**: We run many API calls in parallel while saving safely, enabling fast throughput.
- **Why it works**: Each email is grounded in specific insights rather than a generic template, so it reads as truly personalized at scale.
- **Outcome**: ~900 personalized emails produced quickly and reliably.

## What We Built

- A single, local pipeline (`scripts/super_pipeline.py`) that:
  - Reads the client CSV once with robust parsing (line breaks, quoting, dtype=str)
  - Collects research via Perplexity Sonar API
  - Generates 3 emails via Anthropic (Claude) with guardrails
  - Writes results back to the CSV (AV–AZ for research, BA–BL for emails)
  - Supports parallelized API work with `--concurrency` and single‑writer CSV updates

## Why Local CSV (vs Sheets)

- Immediate feedback and faster I/O
- No network flakiness or Sheets rate limits
- Explicit column mapping control; safer edits

## Concurrency Design

- I/O bound tasks (HTTP calls) parallelized with a thread pool; CSV writes remain sequential.
- Flag: `--concurrency N` (default 1). We used 6 for stable 2–4× throughput.
- Jittered completion handling to avoid API bursts.

## Prompts and Guardrails (Email Quality)

- Emails use a single master prompt and strict format requirements:
  - Subject ≤ 50 chars, outcome/role oriented, no spam words
  - No greeting; concise icebreaker; body ties pain→benefit→company anchor
  - CTA mapping enforced (reply/website/sample)
- Post‑processing guardrails:
  - Remove URLs from bodies (keep CTAs text‑only)
  - Decouple plant‑based terms from “cream cheese” in the same sentence
  - Normalize subjects and ensure uniqueness across the dataset

## Column Mapping

- Research: AV (Company), AW (Contact), AX (Pain Points), AY (Opportunity), AZ (Quality Score)
- Email 1: BA (Subject), BB (Icebreaker), BC (Body), BD (CTA)
- Email 2: BE (Subject), BF (Icebreaker), BG (Body), BH (CTA)
- Email 3: BI (Subject), BJ (Icebreaker), BK (Body), BL (CTA)

## Robust CSV Read/Write

- `engine='python'`, explicit `quotechar`, `escapechar`, `on_bad_lines='skip'`, `dtype=str`, `keep_default_na=False`
- All text cleaned to one‑line to prevent CSV structure issues
- Single save per contact update to minimize corruption risk

## Error Handling & Quality Gates

- Research quality scored 1–10; emails generated only for ≥6/10
- If any research stage fails, the error string is written (to avoid rework); low scores block email generation
- Each contact handled independently; partial progress is safe

## Execution Patterns That Worked

- Process by Contact IDs (primary key) to avoid matching on names/emails
- Batch in chunks of 10–30 for easy spot checks; then scale to full file
- Use `--concurrency 6` locally; dial up/down based on API behavior

## Performance

- Single‑thread baseline → 1×
- Threaded at 6 workers → ~2–3× end‑to‑end speedup consistently (I/O bound)
- Full file (1–287) completed with no write conflicts

## Operational Playbook

- Prereqs: set `.env` with `PERPLEXITY_API_KEY` and `ANTHROPIC_API_KEY`
- CSV path is set in `super_pipeline.py`: `CSV_FILE_PATH = "../data/Schreiber Sheet 5_11 test - Sheet1 (2).fixed.csv"`
- Run from `clients/schreiber/scripts/`:
  - Target a batch: `python3 super_pipeline.py --ids 61 62 63 --concurrency 6`
  - Run all: `python3 super_pipeline.py --all --concurrency 6`
- Recovery:
  - Safe to re‑run any IDs; existing rows will be overwritten deterministically
  - If an API hiccup happens, re‑run just those IDs

## Common Pitfalls (and Fixes)

- CSV line breaks/newlines: normalize to spaces prior to writing
- Duplicated or missing contact emails: prefer `Contact id` column for targeting
- API rate limiting: small random jitter on completion and reasonable `concurrency`
- Link leakage in bodies: strip URLs; CTAs hold the action text

## What We’d Improve Next

- Add retry/backoff wrappers with capped exponential delay
- Optional output to `outputs/` as XLSX and JSONL for BI and audit
- Pluggable prompt “themes” by persona/industry segment
- Dry‑run mode to preview subject/CTA uniqueness at scale

## Hand‑Off Checklist

- `.env` present with API keys (Perplexity, Anthropic)
- CSV verified in `data/` (utf‑8, headers intact)
- Run `super_pipeline.py` in batches with `--concurrency 6`
- Spot check a few rows (research coherence, CTA mapping, subject length)
- Archive the final CSV in `outputs/` and/or publish to Sheets

---
If you need to port this to another client, copy the folder, update `CSV_FILE_PATH`, prompts, and any client‑specific CTAs. The concurrency pattern and guardrails are reusable as‑is.
