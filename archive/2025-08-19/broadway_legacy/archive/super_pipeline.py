#!/usr/bin/env python3
"""
Super Pipeline: Research + Email Generation
Combines research collection and email generation into a single workflow.
Works with local CSV files instead of Google Sheets.
"""

import os
import hashlib
import random
import argparse
import requests
import pandas as pd
from dotenv import load_dotenv
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Load environment variables
load_dotenv()

# Initialize Anthropic client for email generation (prefer Broadway-specific keys)
ANTHROPIC_API_KEY = os.getenv('BROADWAY_ANTHROPIC_API_KEY') or os.getenv('ANTHROPIC_API_KEY')
anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
DEFAULT_ANTHROPIC_MODEL = os.getenv('ANTHROPIC_MODEL', 'claude-3-5-sonnet-latest')
DEFAULT_ANTHROPIC_TEMPERATURE = float(os.getenv('ANTHROPIC_TEMPERATURE', '0.8'))

# Perplexity API configuration (prefer Broadway-specific keys)
PERPLEXITY_API_KEY = os.getenv('BROADWAY_PERPLEXITY_API_KEY') or os.getenv('PERPLEXITY_API_KEY')
PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"

# CSV file path
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
    # Replace newlines and carriage returns with spaces and collapse multiple spaces
    s = s.replace("\r", " ").replace("\n", " ")
    s = " ".join(s.split())
    return s


def collect_research_data(company_name, website, job_title, first_name, last_name, person_linkedin_url, company_linkedin_url):
    """Collect research data using Perplexity Sonar API"""
    
    print(f"🔍 Collecting research for {company_name}...")
    
    # Company Research Prompt
    company_prompt = f"""
    Research {company_name} ({website}) and provide a comprehensive summary focusing on:
    1. Company overview, mission, and main products/services
    2. Industry position and market presence
    3. Business areas that might use dairy ingredients
    4. Company size, revenue, and operations scale
    
    Format as a concise paragraph with key details.
    """
    
    # Contact Research Prompt
    contact_prompt = f"""
    Research {first_name} {last_name}, {job_title} at {company_name}.
    Focus on:
    1. Professional background and role responsibilities
    2. How their role relates to food manufacturing/ingredients
    3. Potential pain points they might face
    4. How heat-stable cream cheese solutions could help them
    
    Format as a concise paragraph with actionable insights.
    """
    
    # Industry Pain Points Prompt
    pain_points_prompt = f"""
    Research the food manufacturing industry challenges that {company_name} likely faces.
    Focus on:
    1. Ingredient sourcing and quality control issues
    2. Production efficiency challenges
    3. Cost management pressures
    4. Regulatory compliance issues
    5. Supply chain disruptions
    
    Format as a structured list with specific examples.
    """
    
    # Opportunity Match Prompt
    opportunity_prompt = f"""
    Based on {company_name}'s business profile, explain how Schreiber Foods' heat-stable cream cheese can help them:
    1. Address specific industry challenges
    2. Improve product offerings
    3. Gain competitive advantages
    4. Achieve business objectives
    
    Format as a compelling value proposition paragraph.
    """
    
    research_data = {}
    
    # Collect company research
    try:
        response = requests.post(
            PERPLEXITY_URL,
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
            json={"model": "sonar-pro", "messages": [{"role": "user", "content": company_prompt}]}
        )
        if response.status_code == 200:
            research_data['company_summary'] = response.json()["choices"][0]['message']['content'].strip()
            print("✅ Company research collected")
        else:
            research_data['company_summary'] = "Research collection failed"
            print("❌ Company research failed")
    except Exception as e:
        research_data['company_summary'] = f"Error: {str(e)}"
        print(f"❌ Company research error: {e}")
    
    # Collect contact research
    try:
        response = requests.post(
            PERPLEXITY_URL,
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
            json={"model": "sonar-pro", "messages": [{"role": "user", "content": contact_prompt}]}
        )
        if response.status_code == 200:
            research_data['contact_summary'] = response.json()["choices"][0]['message']['content'].strip()
            print("✅ Contact research collected")
        else:
            research_data['contact_summary'] = "Research collection failed"
            print("❌ Contact research failed")
    except Exception as e:
        research_data['contact_summary'] = f"Error: {str(e)}"
        print(f"❌ Contact research error: {e}")
    
    # Collect pain points
    try:
        response = requests.post(
            PERPLEXITY_URL,
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
            json={"model": "sonar-pro", "messages": [{"role": "user", "content": pain_points_prompt}]}
        )
        if response.status_code == 200:
            research_data['pain_points'] = response.json()["choices"][0]['message']['content'].strip()
            print("✅ Pain points collected")
        else:
            research_data['pain_points'] = "Research collection failed"
            print("❌ Pain points failed")
    except Exception as e:
        research_data['pain_points'] = f"Error: {str(e)}"
        print(f"❌ Pain points error: {e}")
    
    # Collect opportunity match
    try:
        response = requests.post(
            PERPLEXITY_URL,
            headers={"Authorization": f"Bearer {PERPLEXITY_API_KEY}", "Content-Type": "application/json"},
            json={"model": "sonar-pro", "messages": [{"role": "user", "content": opportunity_prompt}]}
        )
        if response.status_code == 200:
            research_data['opportunity_match'] = response.json()["choices"][0]['message']['content'].strip()
            print("✅ Opportunity match collected")
        else:
            research_data['opportunity_match'] = "Research collection failed"
            print("❌ Opportunity match failed")
    except Exception as e:
        research_data['opportunity_match'] = f"Error: {str(e)}"
        print(f"❌ Opportunity match error: {e}")
    
    # Calculate research quality score (1-10)
    quality_score = 10
    for key, value in research_data.items():
        if "failed" in value.lower() or "error" in value.lower():
            quality_score -= 2
        elif len(value) < 50:
            quality_score -= 1
    
    research_data['quality_score'] = max(1, quality_score)
    
    return research_data

def generate_three_emails(company_name, company_url, contact_name, contact_title, 
                          company_summary, contact_summary, pain_points, opportunity_match,
                          first_name, last_name, industry, company_linkedin, contact_linkedin, *, anthropic_model: str | None = None):
    """Generate three personalized emails using OpenAI"""
    
    print(f"🤖 Generating emails for {company_name}...")
    
    # Master email prompt
    master_prompt = f"""
    Create 3 highly personalized cold emails for {first_name} {last_name}, {contact_title} at {company_name}.
    
    COMPANY CONTEXT (use specifics):
    - Company: {company_name} ({company_url})
    - Industry: {industry}
    - Company Summary: {company_summary}
    - Pain Points (verbatim list): {pain_points}
    - Opportunity: {opportunity_match}
    
    CONTACT CONTEXT (use specifics):
    - Name: {first_name} {last_name}
    - Title: {contact_title}
    - Company LinkedIn: {company_linkedin}
    - Contact LinkedIn: {contact_linkedin}
    
    STRICT PERSONALIZATION REQUIREMENTS:
    - NO GREETING. Start directly with the icebreaker.
    - MAX 150 words total for icebreaker + body + CTA per email.
    - Icebreaker must reference a concrete, role-relevant detail from CONTACT CONTEXT or COMPANY CONTEXT, but do NOT include the person's name.
    - Body must include:
      1) One explicit pain point quoted or paraphrased from the Pain Points list (e.g., "temperature stability in aseptic filling" if present), and
      2) One role-specific benefit for {contact_title} (how this helps their daily responsibilities), and
      3) One company-specific anchor (brand, facility, product line, customer type, or website reference {company_url}).
    - Tie Schreiber’s heat-stable cream cheese directly to the pain point with a clear outcome (e.g., reduce rework, maintain texture post-bake, improve compliance readiness).
    - Keep language human, concrete, and non-generic. Avoid buzzwords and vague claims.
    - Each email must end with a clear CTA (no placeholders) as specified below.

    RULES (DO NOT VIOLATE):
    - Do NOT imply dairy usage in the exact same sentence as plant-based/vegan-only positioning. If plant-based is mentioned in context, keep the dairy suggestion separate and conditional.
    - In Email 2 body, reference the prospect’s own website or public presence (e.g., {company_url}) before the CTA to visit our site.
    - CTA mapping must be exact:
      • Email 1 CTA: "Simply reply to the email"
      • Email 2 CTA: "Visit our website" (we will append the link later)
      • Email 3 CTA: "Request a free sample" (we will append the link later)
    - Subject lines must be:
      • <= 50 characters
      • Distinct across the 3 emails
      • Outcome- or role-oriented (e.g., fewer reworks, faster QA release)
      • Avoid spammy words (free, boost, elevate) and clichés
    
    EMAIL 1:
    - Subject: Compelling, original, non-spammy (<= 50 chars)
    - Icebreaker: Role/operation insight tied to COMPANY CONTEXT (no name)
    - Body: Include pain point + role benefit + company anchor as specified
    - CTA: "Simply reply to the email"
    
    EMAIL 2:
    - Subject: Different angle, original, non-spammy
    - Icebreaker: Different role/operation insight (no name)
    - Body: Include pain point + role benefit + company anchor as specified
    - CTA: "Visit our website" (https://www.schreiberfoodsproducts.com/about/) with supportive reason to click
    
    EMAIL 3:
    - Subject: Third unique angle, original, non-spammy
    - Icebreaker: Another operation/quality insight (no name)
    - Body: Include pain point + role benefit + company anchor as specified
    - CTA: "Request a free sample" (https://www.schreiberfoodsproducts.com/request-sample/) with supportive reason
    
    FORMAT EXACTLY:
    Subject: [subject]
    Icebreaker: [icebreaker]
    Body: [body]
    CTA: [cta]
    
    Separate emails with "---". Do not include any extra sections.
    """
    
    try:
        resp = anthropic_client.messages.create(
            model=(anthropic_model or DEFAULT_ANTHROPIC_MODEL),
            max_tokens=1200,
            temperature=DEFAULT_ANTHROPIC_TEMPERATURE,
            messages=[{"role": "user", "content": master_prompt}]
        )
        content_text = ""
        if hasattr(resp, 'content') and isinstance(resp.content, list):
            for part in resp.content:
                if getattr(part, 'type', None) == 'text' or hasattr(part, 'text'):
                    content_text += getattr(part, 'text', '')
        if not content_text:
            # Some SDKs return .content as string
            content_text = getattr(resp, 'content', '') or ''
        content_text = str(content_text).strip()
        if content_text:
            print("✅ Emails generated successfully")
            emails = parse_three_emails(content_text)
            return validate_and_adjust_emails(
                emails,
                company_url,
                company_summary,
                company_name=company_name,
                role_title=contact_title,
                pain_points_text=pain_points,
            )
        print("❌ No email content generated")
        return None
    except Exception as e:
        print(f"❌ Error generating emails (Anthropic): {e}")
        return None

def parse_three_emails(email_content):
    """Parse the generated email content into structured format"""
    
    print("🔍 Parsing generated emails...")
    
    # Split by email separator
    email_sections = email_content.split("---")
    
    if len(email_sections) < 3:
        print(f"⚠️  Expected 3 emails, found {len(email_sections)}")
        return None
    
    emails = []
    
    for i, section in enumerate(email_sections[:3], 1):
        section = section.strip()
        if not section:
            continue
            
        email_data = {
            'subject': '',
            'icebreaker': '',
            'body': '',
            'cta': ''
        }
        
        # Parse each section
        if "Subject:" in section:
            subject_start = section.find("Subject:") + 8
            subject_end = section.find("\n", subject_start)
            if subject_end != -1:
                email_data['subject'] = section[subject_start:subject_end].strip()
            else:
                email_data['subject'] = section[subject_start:].strip()
        
        if "Icebreaker:" in section:
            icebreaker_start = section.find("Icebreaker:") + 11
            icebreaker_end = section.find("\n", icebreaker_start)
            if icebreaker_end != -1:
                email_data['icebreaker'] = section[icebreaker_start:icebreaker_end].strip()
            else:
                email_data['icebreaker'] = section[icebreaker_start:].strip()
        
        if "Body:" in section:
            body_start = section.find("Body:") + 5
            body_end = section.find("\n", body_start)
            if body_end != -1:
                email_data['body'] = section[body_start:body_end].strip()
            else:
                email_data['body'] = section[body_start:].strip()
        
        if "CTA:" in section:
            cta_start = section.find("CTA:") + 4
            cta_end = section.find("\n", cta_start)
            if cta_end != -1:
                cta_text = section[cta_start:cta_end].strip()
                # Fix any "No CTA" responses
                if cta_text.lower() in ["no cta", "no cta.", "n/a", "none", "no cta", "no cta text"]:
                    cta_text = "Request a free sample to see how our heat-stable cream cheese can transform your products."
                email_data['cta'] = cta_text
            else:
                # If no newline found, take everything after CTA:
                cta_text = section[cta_start:].strip()
                # Fix any "No CTA" responses
                if cta_text.lower() in ["no cta", "no cta.", "n/a", "none", "no cta", "no cta text"]:
                    cta_text = "Request a free sample to see how our heat-stable cream cheese can transform your products."
                email_data['cta'] = cta_text
        
        emails.append(email_data)
        print(f"  ✅ Email {i} parsed")
    
    return emails

def validate_and_adjust_emails(emails, company_url: str, company_summary: str, *, company_name: str = "", role_title: str = "", pain_points_text: str = "", existing_subjects: set | None = None):
    """Apply guardrails:
    - A: Avoid dairy+plant-based in same sentence
    - B: Ensure Email 2 body references prospect website before CTA
    - C: Enforce CTA mapping text only (no links; user will append)
    """
    if not emails:
        return emails
    plant_terms = ["plant-based", "vegan", "dairy-free"]
    def has_plant_context(text: str) -> bool:
        t = (text or "").lower()
        return any(term in t for term in plant_terms)
    def fix_conflict(text: str) -> str:
        if not text:
            return text
        sentences = [s.strip() for s in text.split('.')]
        out = []
        for s in sentences:
            s_low = s.lower()
            if any(pt in s_low for pt in plant_terms) and "cream cheese" in s_low:
                # decouple: remove cream cheese from this sentence; we’ll handle benefit in another sentence
                s = s.replace("cream cheese", "ingredient solutions")
            out.append(s)
        cleaned = '. '.join([s for s in out if s]).strip()
        if cleaned and not cleaned.endswith('.'):
            cleaned += '.'
        return cleaned

    # Helpers for subjects
    def clean_subject(s: str, max_len: int = 50) -> str:
        s = (s or "").strip()
        # Remove em/en dashes and hyphens entirely
        for ch in ["—", "–", "-"]:
            s = s.replace(ch, " ")
        # Normalize spaces
        s = " ".join(s.split())
        blacklist = ["free", "boost", "elevate", "unlock", "transform"]
        for w in blacklist:
            s = s.replace(w, "").replace(w.title(), "")
        # Sentence case: capitalize first letter, keep rest as written
        if s:
            s = s[0].upper() + s[1:]
        # Capitalize common acronyms
        s = s.replace(" qa ", " QA ").replace(" r&d ", " R&D ")
        if s.lower().startswith("qa "):
            s = "QA " + s[3:]
        if len(s) <= max_len:
            return s
        # Build token-by-token to avoid awkward cuts
        out = []
        for token in s.split():
            next_s = (" ".join(out + [token])).strip()
            if len(next_s) <= max_len:
                out.append(token)
            else:
                break
        s = " ".join(out).strip()
        # Remove trailing punctuation artifacts
        while s and s[-1] in [":", ",", ";", "-", "—", "–"]:
            s = s[:-1].rstrip()
        return s

    role_outcomes = [
        "fewer reworks", "faster QA release", "stable texture", "audit-ready", "less downtime", "reduced scrap", "consistent bake"
    ]

    # Extract a compact pain keyword from pain_points_text
    import re
    pp_text_low = (pain_points_text or "").lower()
    pain_vocab = [
        ("rework", ["rework", "re-work", "reworks", "redo"]),
        ("qa release", ["qa", "quality release", "release"]),
        ("efficiency", ["efficiency", "downtime", "throughput", "changeover"]),
        ("compliance", ["compliance", "regulatory", "audit"]),
        ("stability", ["stability", "temperature", "heat-stable", "bake"]),
        ("texture", ["texture", "mouthfeel"]),
        ("shelf life", ["shelf", "life"]),
        ("costs", ["cost", "costs", "margin"]),
        ("supply chain", ["supply", "logistics"]),
    ]
    chosen_pain = "stability"
    for key, triggers in pain_vocab:
        if any(t in pp_text_low for t in triggers):
            chosen_pain = key
            break

    def seeded_choice(options: list[str], seed_key: str) -> str:
        h = int(hashlib.md5(seed_key.encode('utf-8')).hexdigest(), 16)
        return options[h % len(options)] if options else ""

    # Compose diversified subjects by email index with seeded templates
    def compose_subject(idx: int) -> str:
        # Avoid role mentions in subjects; use second person and concrete anchors
        company_short = (company_name or "your operation").split(' / ')[0].strip()
        # Normalize pain term for subject phrasing
        pain_for_subject = chosen_pain
        if pain_for_subject.lower() == "efficiency":
            pain_for_subject = "rework"
        if pain_for_subject.lower() == "qa release":
            pain_for_subject = "QA release"
        templates = [
            "Reduce {Pain} at {Company}?",
            "Cut {Pain} in your {Process}",
            "Fewer reworks in your {Process}",
            "Better {Process} this quarter",
            "Scale {Process} without rework",
            "Faster {Process} for your team",
            "Is {Pain} slowing your {Process}?",
        ]
        processes = ["baking", "filling", "QA release", "changeovers", "aseptic runs", "scale up"]
        qualifiers = ["ready faster", "fewer reworks", "audit ready", "stable texture"]
        outcome = seeded_choice(role_outcomes, f"outcome-{company_short}-{idx}")
        process = seeded_choice(processes, f"process-{company_short}-{idx}")
        qualifier = seeded_choice(qualifiers, f"qual-{company_short}-{idx}")
        tpl = seeded_choice(templates, f"tpl-{company_short}-{idx}")
        base = tpl.format(Outcome=outcome.capitalize(), Pain=pain_for_subject, Company=company_short, Process=process, Qualifier=qualifier)
        base = base[0].upper() + base[1:] if base else base
        subj = clean_subject(base)
        # ensure contains an anchor (prefer process anchor to avoid clunky company insertions)
        anchors = [process.lower()]
        if not any(a in subj.lower() for a in anchors):
            subj = clean_subject(f"{subj} {process}")
        # Ban stale phrases
        banned_pairs = ["streamline sourcing", "fewer reworks"]
        subj_low = subj.lower()
        if all(bp in subj_low for bp in banned_pairs):
            subj = clean_subject(f"Reduce {pain_for_subject} in your {process}")
        # Fix any accidental 'cut efficiency' phrasing
        subj = subj.replace("cut efficiency", "improve efficiency").replace("Cut efficiency", "Improve efficiency")
        return subj

    def compose_cta(idx: int) -> str:
        pain = (chosen_pain or '').strip().lower()
        # Normalize phrasing for efficiency vs others
        is_eff = (pain == 'efficiency')
        pain_reduce = 'rework' if is_eff else pain
        reply_reduce = [
            "If a one‑shift pilot checklist would help you reduce {pain} this week, just reply and I’ll send it over.",
            "If a quick spec pack would help you reduce {pain} now, just reply and I’ll share it today.",
            "If it would help your team reduce {pain} this week, just reply and I’ll forward a one‑shift pilot plan."
        ]
        reply_improve_eff = [
            "If a one‑shift pilot checklist would help you improve efficiency this week, just reply and I’ll send it over.",
            "If a quick spec pack would help you improve efficiency now, just reply and I’ll share it today.",
            "If it would help your team improve efficiency this week, just reply and I’ll forward a one‑shift pilot plan."
        ]
        visit_generic = [
            "Take 60 seconds to see how teams like yours improve efficiency—visit our website.",
            "See a concise walkthrough on improving efficiency—visit our website.",
            "Want practical steps toward {outcome}? Visit our website for a quick rundown tailored to your team."
        ]
        visit_pain = [
            "Take 60 seconds to see how teams like yours solve {pain}—visit our website.",
            "See a concise walkthrough on solving {pain}—visit our website.",
            "Want practical steps toward {outcome}? Visit our website for a quick rundown tailored to your team."
        ]
        sample_eff = [
            "Want to validate an improvement in efficiency on your line? Request a free sample and see {outcome} in one shift.",
            "Ready to improve efficiency during scale up? Request a free sample and prove out {outcome} fast.",
            "Curious how this improves efficiency at your hottest step? Request a free sample and validate {outcome}."
        ]
        sample_pain = [
            "Want to pressure‑test {pain} on your line? Request a free sample and see {outcome} in one shift.",
            "Ready to de‑risk {pain} during scale up? Request a free sample and prove out {outcome} fast.",
            "Curious how this handles {pain} at your hottest step? Request a free sample and validate {outcome}."
        ]
        if idx == 1:
            t = seeded_choice(reply_improve_eff if is_eff else reply_reduce, f"cta1-{company_name}")
            return t.format(pain=pain_reduce)
        elif idx == 2:
            t = seeded_choice(visit_generic if is_eff else visit_pain, f"cta2-{company_name}")
            return t.format(pain=pain_reduce, outcome=seeded_choice(role_outcomes, f"out2-{company_name}"))
        else:
            t = seeded_choice(sample_eff if is_eff else sample_pain, f"cta3-{company_name}")
            return t.format(pain=pain_reduce, outcome=seeded_choice(role_outcomes, f"out3-{company_name}"))

    seen_subjects = set(s.lower() for s in (existing_subjects or set()))

    for idx, email in enumerate(emails, start=1):
        # A: conflict fix in body
        body = fix_conflict(email.get('body', ''))
        # Normalize accidental spaced URLs like 'www. baf. com'
        body = body.replace('www. ', 'www.').replace('. com', '.com').replace('. org', '.org')
        # Remove Schreiber links from body (CTAs handle links)
        for sch in ["schreiberfoodsproducts.com", "schreiberfoods.com"]:
            body = body.replace(sch, "schreiber site")
        # If body references prospect URL, avoid possessive 'our' phrasing
        if company_url:
            for phr in ["our website at ", "our solutions at ", "explore at "]:
                body = body.replace(f"{phr}{company_url}", f"as outlined on {company_url}")
            # Also generic case: 'our ... at www.company.com' -> neutral phrasing
            body = body.replace("our solutions at www.", "as outlined on www.")
        # Remove any CTA-like phrasing from body; keep CTAs only in CTA field
        import re
        cta_markers = [
            r"^\s*visit our website\b",
            r"^\s*request a free sample\b",
            r"^\s*simply reply\b",
            r"^\s*learn more\b",
        ]
        # Split into sentences (simple split by '.') and filter out lines containing CTA markers or URLs
        sentences = [s.strip() for s in re.split(r"(?<=[\.!?])\s+", body) if s.strip()]
        cleaned_sentences = []
        for s in sentences:
            s_low = s.lower()
            if any(re.search(p, s_low) for p in cta_markers):
                continue
            if re.search(r"https?://\S+|www\.\S+", s_low):
                continue
            cleaned_sentences.append(s)
        body = " ".join(cleaned_sentences).strip()
        email['body'] = body
        # B: enforce no URLs in body (no prospect or Schreiber URLs here)
        import re
        body = email.get('body', '') or ''
        # remove http/https and www references
        body = re.sub(r"https?://\S+", "", body)
        body = re.sub(r"www\.\S+", "", body)
        # collapse extra spaces after removals
        body = " ".join(body.split())
        email['body'] = body
        # C: enforce CTA mapping and text only (role/pain-aware phrasing)
        email['cta'] = compose_cta(idx)

        # Subject tuning: keep concise, outcome/role oriented, unique
        # Deterministic subject override for consistency and distinctness
        sub = compose_subject(idx)
        if not sub:
            # Default fallbacks per email index
            defaults = {
                1: "Streamline sourcing — fewer reworks",
                2: "Enhance production efficiency — faster QA",
                3: "Improve compliance readiness — stable texture"
            }
            sub = defaults.get(idx, "QA outcome, faster release")
        # Add outcome cue if missing
        if not any(k in sub.lower() for k in ["qa", "rework", "stable", "audit", "downtime", "efficiency", "texture"]):
            sub = f"{sub} – {role_outcomes[min(idx-1, len(role_outcomes)-1)]}"
        sub = clean_subject(sub)
        # Detect dangling endings and force robust fallback
        dangling_tokens = {"fewer","with","for","now","today","–","-"}
        last_token = (sub.split()[-1].lower() if sub.split() else "")
        if (not sub) or last_token in dangling_tokens or sub.endswith("–") or sub.endswith("-"):
            forced = {
                1: "Streamline sourcing — fewer reworks",
                2: "Enhance production efficiency — faster QA",
                3: "Improve compliance readiness — stable texture"
            }
            sub = clean_subject(forced.get(idx, "Faster QA — fewer reworks"))
        # Ensure uniqueness (global across dataset)
        base_sub = sub
        n = 2
        while sub.lower() in seen_subjects:
            sub = clean_subject(f"{base_sub} #{n}")
            n += 1
        seen_subjects.add(sub.lower())
        email['subject'] = sub
    return emails

def update_csv_with_research_and_emails(csv_path, target_company, target_email, research_data, emails, contact_id: str = '', contact_id_header: str | None = None):
    """Update the CSV file with research data and emails"""
    
    print(f"📝 Updating CSV file for {target_company}...")
    
    try:
        # Read the CSV with robust settings and string dtype
        df = pd.read_csv(csv_path, **CSV_READ_KWARGS)
        
        # Find row by Contact id first (precise key), else by company + email
        target_row = None
        
        # Prefer Contact id
        if contact_id_header and str(contact_id or '').strip():
            for idx, row in df.iterrows():
                row_cid = str(row.get(contact_id_header, '')).strip()
                target_cid = str(contact_id or '').strip()
                if row_cid == target_cid:
                    target_row = idx
                    break
        else:
            for idx, row in df.iterrows():
                row_email = row.get('Email Address', '') or row.get('Email', '')
                if (row.get('Company Name') == target_company and row_email == (target_email or '')):
                    target_row = idx
                    break
        
        if target_row is None:
            key_desc = f"Contact ID '{contact_id}'" if (contact_id_header and contact_id) else f"email '{target_email}'"
            print(f"❌ Company '{target_company}' with {key_desc} not found in CSV during update")
            return
        
        if contact_id_header and contact_id:
            print(f"✅ Found {target_company} (Contact ID {contact_id}) at row index {target_row} for update")
        else:
            print(f"✅ Found {target_company} ({target_email}) at row index {target_row} for update")
        
        # Update research data (Broadway: generic opportunity column)
        research_columns = ['Company Research Summary', 'Contact Research Summary', 'Industry Pain Points', 'Opportunity Match', 'Research Quality Score']
        research_values = [
            _clean_text(research_data.get('company_summary', '')),
            _clean_text(research_data.get('contact_summary', '')),
            _clean_text(research_data.get('pain_points', '')),
            _clean_text(research_data.get('opportunity_match', '')),
            research_data.get('quality_score', 0)
        ]
        
        for i, col in enumerate(research_columns):
            if col in df.columns:
                if str(df[col].dtype) != 'object':
                    df[col] = df[col].astype(str)
                df.at[target_row, col] = research_values[i]
                print(f"  ✅ Updated {col}")
        
        # Update email data (columns BA-BL)
        if emails and len(emails) >= 3:
            email_columns = [
                'Email 1 Subject', 'Email 1 Icebreaker', 'Email 1 Body', 'Email 1 CTA',
                'Email 2 Subject', 'Email 2 Icebreaker', 'Email 2 Body', 'Email 2 CTA Text',
                'Email 3 Subject', 'Email 3 Icebreaker', 'Email 3 Body', 'Email 3 CTA Text'
            ]
            
            email_values = [
                _clean_text(emails[0].get('subject', '')),
                _clean_text(emails[0].get('icebreaker', '')),
                _clean_text(emails[0].get('body', '')),
                _clean_text(emails[0].get('cta', '')),
                _clean_text(emails[1].get('subject', '')),
                _clean_text(emails[1].get('icebreaker', '')),
                _clean_text(emails[1].get('body', '')),
                _clean_text(emails[1].get('cta', '')),
                _clean_text(emails[2].get('subject', '')),
                _clean_text(emails[2].get('icebreaker', '')),
                _clean_text(emails[2].get('body', '')),
                _clean_text(emails[2].get('cta', ''))
            ]
            
            for i, col in enumerate(email_columns):
                if col in df.columns:
                    if str(df[col].dtype) != 'object':
                        df[col] = df[col].astype(str)
                    df.at[target_row, col] = email_values[i]
                    print(f"  ✅ Updated {col}")
        
        # Save the updated CSV
        df.to_csv(csv_path, index=False)
        if contact_id_header and contact_id:
            print(f"✅ CSV file updated successfully for {target_company} (Contact ID {contact_id}) at row {target_row}")
        else:
            print(f"✅ CSV file updated successfully for {target_company} ({target_email}) at row {target_row}")
        
    except Exception as e:
        print(f"❌ Error updating CSV: {e}")

def _process_contact_row(df: pd.DataFrame, contact_id_header: str | None, target_contact_id: str, *, research_only: bool = False):
    """Run research + email generation for a single Contact ID and return data for CSV update."""
    try:
        target_row = None
        if contact_id_header:
            for idx, r in df.iterrows():
                if (r.get(contact_id_header) or '').strip() == target_contact_id:
                    target_row = idx
                    break
        else:
            # When there is no explicit Contact ID column, treat target_contact_id as a row index
            try:
                target_row = int(str(target_contact_id))
            except Exception:
                target_row = None
        if target_row is None:
            print(f"❌ Contact ID {target_contact_id} not found; skipping")
            return False, {"contact_id": target_contact_id}

        row = df.iloc[target_row]
        company_name = row['Company Name']
        website = row['Website']
        job_title = row['Job Title']
        first_name = row['First Name']
        last_name = row['Last Name']
        contact_email = row.get('Email Address', '') or row.get('Email', '')
        contact_id_value = (row.get(contact_id_header) if contact_id_header else '') or ''
        person_linkedin_url = row.get('Person Linkedin Url', '')
        company_linkedin_url = row.get('Company Linkedin Url', '')

        print(f"\n{'='*80}")
        print(f"🎯 TARGET Contact ID: {target_contact_id}")
        print(f"{'='*80}")
        print(f"📊 Company: {company_name}")
        print(f"🌐 Website: {website}")
        print(f"👤 Contact: {first_name} {last_name} - {job_title}")
        print(f"📧 Email: {contact_email if contact_email else 'Not available'}")
        if contact_id_header:
            print(f"🆔 Contact ID: {contact_id_value}")
        print(f"🔗 Person LinkedIn: {person_linkedin_url if person_linkedin_url else 'Not available'}")
        print(f"🏢 Company LinkedIn: {company_linkedin_url if company_linkedin_url else 'Not available'}")

        print(f"\n🔍 PHASE 1: RESEARCH COLLECTION")
        print("-" * 40)
        research_data = collect_research_data(
            company_name=company_name,
            website=website,
            job_title=job_title,
            first_name=first_name,
            last_name=last_name,
            person_linkedin_url=person_linkedin_url,
            company_linkedin_url=company_linkedin_url
        )
        print(f"\n📋 Research Results:")
        print(f"  Company Summary: {research_data.get('company_summary', 'N/A')[:100]}...")
        print(f"  Contact Summary: {research_data.get('contact_summary', 'N/A')[:100]}...")
        print(f"  Pain Points: {research_data.get('pain_points', 'N/A')[:100]}...")
        print(f"  Opportunity Match: {research_data.get('opportunity_match', 'N/A')[:100]}...")
        print(f"  Quality Score: {research_data.get('quality_score', 0)}/10")

        # Optionally skip email generation for research-only workflows
        if research_only:
            print("🛈 Research-only mode: skipping email generation")
            return True, {
                "contact_id": contact_id_value,
                "company_name": company_name,
                "contact_email": contact_email,
                "research_data": research_data,
                "emails": None,
            }

        print(f"\n📧 PHASE 2: EMAIL GENERATION")
        print("-" * 40)
        if research_data.get('quality_score', 0) < 6:
            print(f"⚠️  Research quality too low ({research_data.get('quality_score', 0)}/10) for email generation")
            return True, {
                "contact_id": contact_id_value,
                "company_name": company_name,
                "contact_email": contact_email,
                "research_data": research_data,
                "emails": None,
            }
        print(f"🤖 Generating emails with OpenAI...")
        emails = generate_three_emails(
            company_name=company_name,
            company_url=website or "Not available",
            contact_name=f"{first_name} {last_name}",
            contact_title=job_title or "Not available",
            company_summary=research_data.get('company_summary', ''),
            contact_summary=research_data.get('contact_summary', ''),
            pain_points=research_data.get('pain_points', ''),
            opportunity_match=research_data.get('opportunity_match', ''),
            first_name=first_name or "Contact",
            last_name=last_name or "Person",
            industry="food production",
            company_linkedin=company_linkedin_url if company_linkedin_url else "Not available",
            contact_linkedin=person_linkedin_url if person_linkedin_url else "Not available"
        )
        if not emails:
            print("❌ Failed to generate emails")
            return True, {
                "contact_id": contact_id_value,
                "company_name": company_name,
                "contact_email": contact_email,
                "research_data": research_data,
                "emails": None,
            }
        print(f"✅ Generated {len(emails)} emails")
        return True, {
            "contact_id": contact_id_value,
            "company_name": company_name,
            "contact_email": contact_email,
            "research_data": research_data,
            "emails": emails,
        }
    except Exception as e:
        print(f"❌ Error processing Contact ID {target_contact_id}: {e}")
        return False, {"contact_id": target_contact_id, "error": str(e)}

def main():
    """Main function to run the super pipeline"""
    
    print("🚀 SUPER PIPELINE: Research + Email Generation")
    print("=" * 70)
    print("📁 Working with local CSV file")
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"❌ CSV file not found: {CSV_FILE_PATH}")
        return
    
    # Load environment variables
    if not PERPLEXITY_API_KEY:
        print("❌ PERPLEXITY_API_KEY not found in .env file")
        return
    
    if not ANTHROPIC_API_KEY:
        print("❌ ANTHROPIC_API_KEY not found in .env file")
        return
    
    print(f"✅ Perplexity API Key loaded: {PERPLEXITY_API_KEY[:20]}...")
    print(f"✅ Anthropic API Key loaded: {ANTHROPIC_API_KEY[:20]}...")
    
    # Read CSV data
    print(f"📖 Reading CSV file: {CSV_FILE_PATH}")
    try:
        import pandas as pd  # ensure pandas in local scope
        df = pd.read_csv(CSV_FILE_PATH, **CSV_READ_KWARGS)
        print(f"📊 Found {len(df)} rows of data")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return
    
    # Primary selection: Contact id
    contact_id_header = None
    for col in df.columns:
        if col.strip().lower() == 'contact id' or col.strip().lower() == 'contact id':
            contact_id_header = col
            break
    
    # Parse CLI for target Contact IDs
    parser = argparse.ArgumentParser(description="Run super pipeline for specific Contact IDs")
    parser.add_argument("--ids", nargs="+", help="One or more Contact IDs to process", default=None)
    parser.add_argument("--all", action="store_true", help="Process all Contact IDs found in the CSV")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent workers for API tasks (research + emails)")
    parser.add_argument("--research-only", action="store_true", help="Collect research only and skip email generation")
    args, _ = parser.parse_known_args()

    # Determine target Contact IDs
    if args.all:
        if contact_id_header:
            target_ids = [str(v).strip() for v in df[contact_id_header].tolist() if str(v).strip()]
        else:
            # No Contact ID column; use zero-based row indices as IDs
            target_ids = [str(i) for i in range(len(df))]
    else:
        target_ids = args.ids if args.ids else ["10", "11"]
    # Concurrent API phase
    concurrency = max(1, int(args.concurrency or 1))
    results_by_id = {}
    if concurrency > 1:
        print(f"⚙️  Concurrency enabled: {concurrency} workers")
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_id = {executor.submit(_process_contact_row, df, contact_id_header, target_contact_id, research_only=args.research_only): target_contact_id for target_contact_id in target_ids}
            for future in as_completed(future_to_id):
                cid = future_to_id[future]
                ok, data = future.result()
                results_by_id[cid] = {"ok": ok, **data}
                time.sleep(random.uniform(0.05, 0.15))
    else:
        for target_contact_id in target_ids:
            ok, data = _process_contact_row(df, contact_id_header, target_contact_id, research_only=args.research_only)
            results_by_id[target_contact_id] = {"ok": ok, **data}

    # Sequential CSV write phase
    for target_contact_id in target_ids:
        data = results_by_id.get(target_contact_id)
        if not data or not data.get("ok"):
            continue
        update_csv_with_research_and_emails(
            CSV_FILE_PATH,
            data.get("company_name", ""),
            data.get("contact_email", ""),
            data.get("research_data"),
            data.get("emails"),
            contact_id=target_contact_id,
            contact_id_header=contact_id_header,
        )
        print(f"\n🎉 COMPLETE for Contact ID {target_contact_id} — emails written to BA–BL")
    print(f"\n✅ Batch complete for Contact IDs: {', '.join(target_ids)}")

if __name__ == "__main__":
    main()
