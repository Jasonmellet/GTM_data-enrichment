import argparse
import json
import os
import sys
import requests


def find_place(query: str, api_key: str, fields: str = "place_id,name,business_status,formatted_address"):
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": fields,
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=20)
    return resp.status_code, resp.json()


def place_details(place_id: str, api_key: str, fields: str = "name,formatted_phone_number,formatted_address,website,business_status,place_id"):
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": fields,
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=20)
    return resp.status_code, resp.json()


def main():
    parser = argparse.ArgumentParser(description="Google Maps quick lookup (ping + compare queries)")
    parser.add_argument("--query", type=str, required=True, help="Text query for FindPlaceFromText")
    parser.add_argument("--details", action="store_true", help="Fetch details for top candidate")
    args = parser.parse_args()

    api_key = os.getenv("BROADWAY_GOOGLE_MAPS_API_KEY")
    if not api_key:
        print("Missing BROADWAY_GOOGLE_MAPS_API_KEY in environment", file=sys.stderr)
        sys.exit(1)

    status, data = find_place(args.query, api_key)
    out = {
        "query": args.query,
        "status": status,
        "candidates_count": len((data or {}).get("candidates", [])),
        "candidates_sample": (data or {}).get("candidates", [])[:3],
    }
    if args.details and (data or {}).get("candidates"):
        top = data["candidates"][0]
        pid = top.get("place_id")
        if pid:
            d_status, d_json = place_details(pid, api_key)
            out["top_details_status"] = d_status
            out["top_details"] = d_json.get("result", {})

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()


