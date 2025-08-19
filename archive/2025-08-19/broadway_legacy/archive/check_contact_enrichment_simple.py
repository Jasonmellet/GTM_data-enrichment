#!/usr/bin/env python3
"""
Simplified script to check contact enrichment status in the database.
This version doesn't rely on psycopg and directly reads from CSV files.
"""

import os
import sys
import argparse
import csv
from datetime import datetime

def read_csv_data(file_path):
    """Read data from a CSV file."""
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return [], []
    
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        # Read all rows
        all_rows = list(reader)
        
        # Find where the actual data starts (after the summary)
        data_start = 0
        for i, row in enumerate(all_rows):
            if row and row[0] == "org_id":
                data_start = i
                break
        
        # Extract headers and data
        if data_start > 0:
            headers = all_rows[data_start]
            data = all_rows[data_start+1:]
        else:
            headers = all_rows[0] if all_rows else []
            data = all_rows[1:] if len(all_rows) > 1 else []
    
    return headers, data

def calculate_enrichment_summary(data, headers):
    """Calculate summary statistics for the enrichment status."""
    total = len(data)
    if total == 0:
        return {
            "total": 0,
            "direct_emails": 0,
            "any_emails": 0,
            "with_phone": 0,
            "verified_address": 0,
            "verified_phone": 0
        }
    
    # Find column indices
    email_idx = headers.index("email") if "email" in headers else -1
    is_direct_idx = headers.index("is_direct_email") if "is_direct_email" in headers else -1
    phone_idx = headers.index("phone_number") if "phone_number" in headers else -1
    verified_addr_idx = headers.index("maps_verified_address") if "maps_verified_address" in headers else -1
    verified_phone_idx = headers.index("maps_verified_phone") if "maps_verified_phone" in headers else -1
    
    # Count values
    direct_emails = sum(1 for row in data if is_direct_idx >= 0 and row[is_direct_idx] == "True")
    any_emails = sum(1 for row in data if email_idx >= 0 and row[email_idx])
    with_phone = sum(1 for row in data if phone_idx >= 0 and row[phone_idx])
    verified_address = sum(1 for row in data if verified_addr_idx >= 0 and row[verified_addr_idx] == "True")
    verified_phone = sum(1 for row in data if verified_phone_idx >= 0 and row[verified_phone_idx] == "True")
    
    return {
        "total": total,
        "direct_emails": direct_emails,
        "direct_emails_pct": direct_emails/total*100 if total > 0 else 0,
        "any_emails": any_emails,
        "any_emails_pct": any_emails/total*100 if total > 0 else 0,
        "with_phone": with_phone,
        "with_phone_pct": with_phone/total*100 if total > 0 else 0,
        "verified_address": verified_address,
        "verified_address_pct": verified_address/total*100 if total > 0 else 0,
        "verified_phone": verified_phone,
        "verified_phone_pct": verified_phone/total*100 if total > 0 else 0
    }

def filter_data(data, headers, org_name=None, direct_only=False):
    """Filter data based on criteria."""
    if not data:
        return []
    
    # Find column indices
    org_idx = headers.index("organization_name") if "organization_name" in headers else -1
    is_direct_idx = headers.index("is_direct_email") if "is_direct_email" in headers else -1
    
    # Apply filters
    filtered_data = data
    
    if org_name and org_idx >= 0:
        # Case-insensitive search in organization name
        filtered_data = [row for row in filtered_data 
                        if org_idx < len(row) and 
                        (org_name.lower() in row[org_idx].lower() or  # Search anywhere in the name
                         any(org_name.lower() in field.lower() for field in row))]  # Search in all fields
    
    if direct_only and is_direct_idx >= 0:
        filtered_data = [row for row in filtered_data if is_direct_idx < len(row) and row[is_direct_idx] == "True"]
    
    return filtered_data

def deduplicate_data(data, headers):
    """Remove duplicate entries based on organization and contact name."""
    if not data or not headers:
        return data
    
    # Find column indices
    org_idx = headers.index("organization_name") if "organization_name" in headers else -1
    contact_idx = headers.index("contact_name") if "contact_name" in headers else -1
    email_idx = headers.index("email") if "email" in headers else -1
    is_direct_idx = headers.index("is_direct_email") if "is_direct_email" in headers else -1
    
    # If we can't find the key columns, return the original data
    if org_idx < 0 or contact_idx < 0:
        return data
    
    # Use a dictionary to track unique entries
    unique_entries = {}
    
    for row in data:
        if org_idx >= len(row) or contact_idx >= len(row):
            continue
            
        key = (row[org_idx], row[contact_idx])
        
        # If this is the first time we've seen this key, add it
        if key not in unique_entries:
            unique_entries[key] = row
        else:
            # If we already have this key, prefer the row with direct email
            existing_row = unique_entries[key]
            
            if (is_direct_idx >= 0 and 
                is_direct_idx < len(row) and 
                row[is_direct_idx] == "True" and 
                (is_direct_idx >= len(existing_row) or existing_row[is_direct_idx] != "True")):
                unique_entries[key] = row
    
    # Convert back to a list
    return list(unique_entries.values())

def get_compact_view(headers, data):
    """Create a compact view with fewer columns."""
    compact_columns = [
        "organization_name", "contact_name", "email", "is_direct_email",
        "phone_number", "city", "state", "business_status", "website_status",
        "fit_score", "outreach_readiness"
    ]
    
    # Get indices of compact columns
    indices = [headers.index(col) for col in compact_columns if col in headers]
    
    # Filter columns and data
    compact_headers = [headers[i] for i in indices]
    compact_data = [[row[i] if i < len(row) else "" for i in indices] for row in data]
    
    return compact_headers, compact_data

def write_csv(filename, headers, data, include_summary=True, summary=None):
    """Write data to a CSV file."""
    # Make sure the directory exists
    os.makedirs(os.path.dirname(os.path.abspath(filename)), exist_ok=True)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write summary if requested
        if include_summary and summary:
            writer.writerow(["ENRICHMENT SUMMARY"])
            writer.writerow([f"Total contacts: {summary['total']}"])
            writer.writerow([f"With direct emails: {summary['direct_emails']} ({summary['direct_emails_pct']:.1f}%)"])
            writer.writerow([f"With any email: {summary['any_emails']} ({summary['any_emails_pct']:.1f}%)"])
            writer.writerow([f"With phone numbers: {summary['with_phone']} ({summary['with_phone_pct']:.1f}%)"])
            writer.writerow([f"With verified addresses: {summary['verified_address']} ({summary['verified_address_pct']:.1f}%)"])
            writer.writerow([f"With verified phones: {summary['verified_phone']} ({summary['verified_phone_pct']:.1f}%)"])
            writer.writerow([])  # Empty row for separation
        
        # Write headers
        writer.writerow(headers)
        
        # Write data
        for row in data:
            writer.writerow(row)

def print_enrichment_summary(summary):
    """Print a summary of the enrichment status."""
    if summary["total"] == 0:
        print("No contacts found matching the criteria.")
        return
    
    print("\n=== ENRICHMENT SUMMARY ===")
    print(f"Total contacts: {summary['total']}")
    print(f"With direct emails: {summary['direct_emails']} ({summary['direct_emails_pct']:.1f}%)")
    print(f"With any email: {summary['any_emails']} ({summary['any_emails_pct']:.1f}%)")
    print(f"With phone numbers: {summary['with_phone']} ({summary['with_phone_pct']:.1f}%)")
    print(f"With verified addresses: {summary['verified_address']} ({summary['verified_address_pct']:.1f}%)")
    print(f"With verified phones: {summary['verified_phone']} ({summary['verified_phone_pct']:.1f}%)")
    print("========================\n")

def print_table(headers, data):
    """Print data as a table."""
    # Calculate column widths
    col_widths = [max(len(str(row[i])) if i < len(row) else 0 for row in [headers] + data) + 2 for i in range(len(headers))]
    
    # Print header
    header_row = "".join(f"{headers[i]:{col_widths[i]}}" for i in range(len(headers)))
    print(header_row)
    print("-" * len(header_row))
    
    # Print data
    for row in data:
        print("".join(f"{str(row[i]) if i < len(row) else '':{col_widths[i]}}" for i in range(len(headers))))

def main():
    parser = argparse.ArgumentParser(description="Check contact enrichment status from CSV file")
    parser.add_argument("--input", default="clients/Broadway/outputs/contact_enrichment_report.csv", 
                        help="Input CSV file (default: clients/Broadway/outputs/contact_enrichment_report.csv)")
    parser.add_argument("--org", help="Filter by organization name (partial match)")
    parser.add_argument("--direct-only", action="store_true", help="Show only contacts with direct emails")
    parser.add_argument("--limit", type=int, default=10, help="Limit the number of results (default: 10)")
    parser.add_argument("--compact", action="store_true", help="Show compact view with fewer columns")
    parser.add_argument("--csv", action="store_true", help="Output as CSV instead of table")
    parser.add_argument("--output", help="Output file path (default: filtered_contact_report.csv or filtered_contact_report_compact.csv)")
    parser.add_argument("--no-dedupe", action="store_true", help="Skip deduplication of results")
    parser.add_argument("--search-all", action="store_true", help="Search for term in all fields, not just organization name")
    args = parser.parse_args()
    
    try:
        # Read data from CSV
        headers, data = read_csv_data(args.input)
        
        if not headers or not data:
            print(f"Error: No data found in {args.input}")
            return 1
        
        # Filter data
        filtered_data = filter_data(data, headers, org_name=args.org, direct_only=args.direct_only)
        
        # Print debug info
        if args.org:
            print(f"Found {len(filtered_data)} entries matching '{args.org}'")
        
        # Deduplicate data (unless explicitly disabled)
        if not args.no_dedupe:
            deduped_data = deduplicate_data(filtered_data, headers)
            print(f"Removed {len(filtered_data) - len(deduped_data)} duplicate entries")
            filtered_data = deduped_data
        
        # Apply limit
        if args.limit > 0 and len(filtered_data) > args.limit:
            print(f"Limiting output to {args.limit} entries (from {len(filtered_data)} total)")
            filtered_data = filtered_data[:args.limit]
        
        # Calculate summary statistics
        summary = calculate_enrichment_summary(filtered_data, headers)
        
        # Apply compact view if requested
        if args.compact:
            headers, filtered_data = get_compact_view(headers, filtered_data)
        
        # Determine output mode and destination
        if args.csv:
            # Determine output file
            if args.output:
                # If output path doesn't start with clients/Broadway/outputs, prepend it
                output_file = args.output
                if not output_file.startswith("clients/Broadway/outputs/") and not os.path.isabs(output_file):
                    output_file = os.path.join("clients/Broadway/outputs", output_file)
            else:
                output_dir = os.path.join("clients", "Broadway", "outputs")
                if args.compact:
                    output_file = os.path.join(output_dir, "filtered_contact_report_compact.csv")
                else:
                    output_file = os.path.join(output_dir, "filtered_contact_report.csv")
            
            # Write CSV file
            write_csv(output_file, headers, filtered_data, include_summary=True, summary=summary)
            print(f"CSV report saved to: {output_file}")
        else:
            # Print to console
            print_enrichment_summary(summary)
            print_table(headers, filtered_data)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())