#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
from typing import Dict, List
import requests
from dotenv import load_dotenv


CSV_DEFAULT = "../data/Summer Camp Enrichment Sample Test.expanded.csv"


TAXONOMY = (
    "Formats & Seasons: Summer day camps, Overnight sleepaway camps, Residential camps, Day-only commuter camps, Half-day camps, "
    "Weekend mini-camps, Weeklong intensives, Two-week sessions, Extended-session camps (3+ weeks), Spring break camps, Winter break camps, "
    "Fall camps, Holiday camps, Year-round enrichment camps, Family camps, Parent–child camps, Travel/expedition camps, International travel camps, Virtual/online camps, Hybrid (online + on-site) camps; "
    "Audience & Community: Pre-K camps, Kindergarten readiness camps, Elementary school camps, Middle school camps, High school camps, Teen leadership camps, College prep camps, Coed camps, Girls-only camps, Boys-only camps, Christian camps, Catholic camps, Jewish/JCC camps, Interfaith camps, Muslim youth camps, LGBTQ+ inclusive camps, BIPOC-centered camps, Military family camps, Foster youth camps, Refugee/immigrant youth camps; "
    "Performing Arts: Theater/drama camps, Musical theater camps, Acting technique camps, Improv comedy camps, Playwriting/scriptwriting camps, Stagecraft/technical theater camps, Costume & set design camps, Dance camps, Ballet camps, Jazz dance camps, Hip-hop dance camps, Tap dance camps, Contemporary/modern dance camps, Choreography camps, Music camps, Choir/voice camps, Band & orchestra camps, Piano/keyboard camps, Guitar/strings camps, Songwriting & music production camps; "
    "Visual, Media & Design: Visual arts camps, Drawing & painting camps, Sculpture & ceramics camps, Printmaking camps, Photography camps, Film photography/darkroom camps, Filmmaking & video production camps, Animation (2D/3D) camps, Stop-motion animation camps, Graphic design camps, Illustration & comics/manga camps, Fashion design & sewing camps, Textile & fiber arts camps, Architecture & design camps, Interior design camps, UX/UI design camps, Digital media/content creator camps, Podcasting & broadcasting camps, Journalism & media literacy camps, Advertising/creative strategy camps; "
    "STEM & Tech: STEM/STEAM camps, Coding/programming camps, Game design & development camps, Robotics camps, AI & machine learning camps, Data science & analytics camps, Cybersecurity camps, Web & app development camps, Electronics & circuits camps, Engineering design camps, Mechanical engineering camps, Electrical engineering camps, Aerospace/rocketry camps, Drone & UAV camps, 3D printing & CAD camps, Maker/makerspace camps, Science discovery camps, Biology & life sciences camps, Chemistry & physics camps, Astronomy & space science camps; "
    "Academics, Language & Leadership: Academic enrichment camps, Reading & literacy camps, Creative writing camps, Debate & public speaking camps, Model United Nations camps, Social studies & civics camps, Financial literacy & entrepreneurship camps, Business & startup camps, Math enrichment camps, Test prep (SAT/ACT) camps, Study skills & executive function camps, Foreign language immersion camps, Spanish immersion camps, French immersion camps, Mandarin Chinese immersion camps, American Sign Language (ASL) camps, History & museum camps, Law & mock trial camps, Medical/health science explorer camps, Leadership & service-learning camps; "
    "Outdoors, Nature & Animals: Outdoor adventure camps, Backpacking & camping skills camps, Hiking & trail exploration camps, Survival skills/bushcraft camps, Orienteering & navigation camps, Climbing & bouldering camps, Wilderness first aid camps, Environmental science & ecology camps, Conservation & stewardship camps, Sustainability/green living camps, Farm & agriculture camps, Ranch & horsemanship camps, Equestrian/horseback riding camps, Animal care & veterinary explorer camps, Marine biology & ocean camps, Sailing & seamanship camps, Kayaking & canoeing camps, Fishing & angling camps, River & whitewater adventure camps, National parks expedition camps; "
    "Sports & Movement: Multi-sport day camps, Soccer camps, Basketball camps, Baseball camps, Softball camps, Volleyball camps, Football (non-contact/flag) camps, Tennis camps, Pickleball camps, Golf camps, Gymnastics camps, Cheerleading & dance team camps, Track & field camps, Cross-country running camps, Swimming camps, Diving camps, Rowing/crew camps, Lacrosse camps, Field hockey camps, Ice hockey camps; "
    "Inclusion, Wellness & Support: Inclusive/special needs camps, Autism spectrum (ASD) camps, ADHD support camps, Learning differences (LD/Dyslexia) camps, Speech & language development camps, Social skills development camps, Sensory-friendly camps, Adaptive sports camps, Visually impaired/Blind camps, Deaf/Hard-of-hearing camps, Wheelchair-accessible camps, Diabetes camps, Cancer survivor camps, Cardiac/heart-healthy camps, Asthma & allergy-aware camps, Bereavement & grief support camps, Trauma-informed resilience camps, Mindfulness & yoga wellness camps, Nutrition & healthy habits camps, Independent living/life skills camps."
)


def ask_perplexity(company: str, website: str, api_key: str, verbose: bool = False) -> str:
    prompt = (
        f"Classify the business '{company}' ({website}) using only categories from this taxonomy: {TAXONOMY}. "
        f"Return JSON with key 'definitive_categories' as a comma-separated list (no commentary)."
    )
    resp = requests.post(
        "https://api.perplexity.ai/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": "sonar-pro",
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": "Classify strictly by the provided taxonomy. Return JSON only."},
                {"role": "user", "content": prompt},
            ],
        },
        timeout=30,
    )
    if resp.status_code != 200:
        if verbose:
            print(json.dumps({"debug": "perplexity_error", "status": resp.status_code, "text": resp.text[:400]}, indent=2))
        return ""
    content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    try:
        m = __import__("re").search(r"\{[\s\S]*\}", content)
        data = json.loads(m.group(0)) if m else json.loads(content)
        cats = (data.get("definitive_categories") or "").strip()
        return cats
    except Exception:
        if verbose:
            print(json.dumps({"debug": "perplexity_parse_fail", "content": content[:400]}, indent=2))
        return ""


def write_categories(csv_path: str, ids: List[int], api_key: str, verbose: bool = False) -> None:
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []
    if "Definitive Categories" not in fieldnames:
        fieldnames.append("Definitive Categories")

    id_set = {str(i) for i in ids}
    updated = 0
    for r in rows:
        if r.get("Contact id") in id_set:
            cats = ask_perplexity(r.get("Company Name", ""), r.get("Website URL", ""), api_key, verbose=verbose)
            if cats:
                r["Definitive Categories"] = cats
                updated += 1

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(json.dumps({"updated": updated, "ids": ids}, indent=2))


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Test Perplexity taxonomy classification for selected contact ids")
    parser.add_argument("--csv", type=str, default=CSV_DEFAULT)
    parser.add_argument("--ids", nargs="+", type=int, required=True)
    args = parser.parse_args()

    api_key = os.getenv("BROADWAY_PERPLEXITY_API_KEY") or os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        print("❌ Missing BROADWAY_PERPLEXITY_API_KEY / PERPLEXITY_API_KEY", file=sys.stderr)
        sys.exit(1)

    write_categories(args.csv, args.ids, api_key, verbose=True)


if __name__ == "__main__":
    main()


