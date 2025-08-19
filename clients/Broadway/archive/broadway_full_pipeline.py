#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import sys
from typing import List, Dict, Any


CSV_DEFAULT = "../data/Summer Camp Enrichment Sample Test.expanded.csv"


def run_crawler(id_int: int, csv_path: str, verify_with_maps: bool, maps_max_calls: int, render: bool, show_usage: bool) -> Dict[str, Any]:
    script = os.path.join(os.path.dirname(__file__), "Broadway_site_crawler_module.py")
    cmd: List[str] = [sys.executable, script, "--id", str(id_int), "--csv", csv_path]
    if verify_with_maps:
        cmd.append("--verify-with-maps")
        cmd.extend(["--maps-max-calls", str(maps_max_calls)])
    if render:
        cmd.append("--render")
    if show_usage:
        cmd.append("--show-usage")

    # Stream output so user sees progress in real time while we parse JSON blocks
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    usage_block: Dict[str, Any] = {}
    updates_block: Dict[str, Any] = {}
    collected_lines: List[str] = []
    json_chunks: List[str] = []
    chunk_lines: List[str] = []
    brace_balance = 0
    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="")
        collected_lines.append(line.rstrip("\n"))
        s = line.strip()
        if s.startswith("{"):
            # start collecting JSON chunk
            brace_balance = 0
            chunk_lines = []
        # track braces
        brace_balance += line.count("{")
        brace_balance -= line.count("}")
        chunk_lines.append(line.rstrip("\n"))
        if brace_balance == 0 and s.endswith("}") and chunk_lines:
            json_chunks.append("\n".join(chunk_lines))
            chunk_lines = []
    proc.wait()
    # Parse chunks
    for j in json_chunks:
        try:
            obj = json.loads(j)
            if obj.get("debug") == "usage":
                usage_block = obj
            elif obj.get("contact_id") is not None and obj.get("updates") is not None:
                updates_block = obj
        except Exception:
            continue
    return {"raw": "\n".join(collected_lines), "usage": usage_block, "result": updates_block, "returncode": proc.returncode}


def main():
    parser = argparse.ArgumentParser(description="Broadway Full Pipeline (attributes + details + cost)")
    parser.add_argument("--ids", nargs="+", type=int, help="Contact ids to process (1-based)")
    parser.add_argument("--csv", type=str, default=CSV_DEFAULT, help="Path to expanded CSV")
    parser.add_argument("--maps", action="store_true", help="Enable Google Maps verification")
    parser.add_argument("--maps-max-calls", type=int, default=1, help="Max Maps calls per row")
    parser.add_argument("--render", action="store_true", help="Render pages when empty")
    parser.add_argument("--show-usage", action="store_true", help="Print usage/cost blocks")
    args = parser.parse_args()

    if not args.ids:
        print("Provide --ids list of Contact ids to process", file=sys.stderr)
        sys.exit(1)

    total_cost = 0.0
    rows_done = []
    total = len(args.ids)
    print(f"🚀 BROADWAY FULL PIPELINE | rows: {total} | maps: {args.maps} | render: {args.render}")
    for idx, cid in enumerate(args.ids, start=1):
        print(f"\n[{idx}/{total}] Processing Contact id {cid} …")
        res = run_crawler(
            id_int=cid,
            csv_path=args.csv,
            verify_with_maps=args.maps,
            maps_max_calls=args.maps_max_calls,
            render=args.render,
            show_usage=True,
        )
        usage = res.get("usage", {})
        costs = (usage.get("costs") or {})
        row_cost = float(costs.get("total_cost_est_usd", 0.0))
        total_cost += row_cost
        rows_done.append({"contact_id": cid, "cost": row_cost, "usage": usage.get("stats", {})})
        # Print the last updates block for quick visibility
        updates = res.get("result", {}).get("updates", {})
        if updates:
            print(json.dumps({"contact_id": cid, "updates": updates}, indent=2))
        else:
            print("No updates block parsed")

    print("\n=== Summary ===")
    print(json.dumps({"rows": rows_done, "total_cost_est_usd": round(total_cost, 4)}, indent=2))


if __name__ == "__main__":
    main()


