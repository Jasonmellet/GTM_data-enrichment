import argparse
import csv
from typing import List


DEFAULT_IN = "../data/Summer Camp Enrichment Sample Test.expanded.csv"
DEFAULT_OUT = "../data/Summer Camp Enrichment Sample Test.cleaned.csv"


def build_target_order() -> List[str]:
    return [
        # Identifiers
        "Contact id",

        # Company core
        "Company Name",
        "Website URL",
        # Single definitive taxonomy
        "Definitive Categories",
        "Primary Location",

        # Location
        "Street Address",
        "City",
        "State/Region*",
        "Postal Code",
        "Country/Region",

        # Status
        "Website Status",
        "Business Status",

        # Attributes
        "Price Range",
        "Session Length",
        "Season",
        "Age Range",
        "Overnight/Day",
        "Founded Year",
        "Enrollment Size",
        "Accreditations",

        # Socials & org profiles
        "Company Linkedin Url",
        "Facebook Url",
        "Instagram Url",
        "YouTube Url",
        "TikTok Url",
        "Twitter/X Url",

        # Phones & emails
        "Company Phone",
        "Email",
        "Email Status",
        "Email Confidence",
        "Alternate Phone",

        # Primary contact (row’s person)
        "First Name",
        "Last Name",
        "Job Title",
        "Designation",
        "Mobile Phone Number",
        "Contact Linkedin Url",

        # Secondary contacts (in-row)
        "Contact 2 Name",
        "Contact 2 Title",
        "Contact 2 Email",
        "Contact 2 Phone",
        "Contact 2 Linkedin Url",
        "Contact 3 Name",
        "Contact 3 Title",
        "Contact 3 Email",
        "Contact 3 Phone",
        "Contact 3 Linkedin Url",

        # Verification (Maps)
        "Maps Place ID",
        "Maps Verified Phone",
        "Maps Verified Address",

        # Provenance
        "Source Verified URL",
        "Verified On",
        "Notes",

        # Classification (fit)
        "Fit Decision",
        "Fit Score",
        "Fit Reason",
        "Exclude Reason",
        "Taxonomy Decision",
        "Recommended Segment",

        # Research (optional at end so it doesn't clutter ops fields)
        "Company Research Summary",
        "Contact Research Summary",
        "Industry Pain Points",
        "Opportunity Match",
        "Research Quality Score",
        
        # Original context columns (kept late)
        # Keep legacy context if present (moved to end)
        "Business Category",
        "Camp Category",
        "Normalized Category",
        "App Search Categories",
        "Camp Type",
        "Camp Description",
        "Role Group",
    ]


DROP_EXACT = {
    # Known noise placeholders that slipped in
    "Twitter/X Url": "newsroom.fedex.com/newsroom",  # keep column, drop values handled elsewhere
}


def reorder_csv(input_path: str, output_path: str) -> None:
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        existing_cols = reader.fieldnames or []

    # Remove clearly empty columns (all rows empty) except identifiers
    non_empty_cols = set()
    for col in existing_cols:
        for r in rows:
            if (r.get(col) or "").strip():
                non_empty_cols.add(col)
                break

    # Build final ordered header: target order present + any remaining non-empty cols appended
    target = build_target_order()
    final_order: List[str] = []
    for col in target:
        if col in non_empty_cols:
            final_order.append(col)
    # Append remaining non-empty columns not already included
    for col in existing_cols:
        if col in non_empty_cols and col not in final_order:
            final_order.append(col)

    # Write cleaned CSV
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=final_order)
        writer.writeheader()
        for r in rows:
            # Optionally scrub known noisy values
            cleaned = dict(r)
            # Example: if a value contains an obvious share URL, keep but it will be visible for later QA
            writer.writerow({k: cleaned.get(k, "") for k in final_order})


def main():
    parser = argparse.ArgumentParser(description="Reorder and clean Broadway CSV columns for analysis")
    parser.add_argument("--input", type=str, default=DEFAULT_IN)
    parser.add_argument("--output", type=str, default=DEFAULT_OUT)
    args = parser.parse_args()

    reorder_csv(args.input, args.output)
    print(f"✅ Wrote cleaned CSV: {args.output}")


if __name__ == "__main__":
    main()


