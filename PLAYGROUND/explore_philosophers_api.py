"""
explore_philosophers_api.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PURPOSE: Interactively explore the Philosophers API to understand:
  - What endpoints exist and what they return
  - The JSON schema / field names of each record
  - Pagination behaviour
  - Any edge cases (missing fields, null values, nested objects)

Run with:  python PLAYGROUND/explore_philosophers_api.py
No credentials required — the API is fully public.
"""

import json
import pprint
import requests

BASE_URL = "https://philosophersapi.com/api"

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: Discover available endpoints
# ─────────────────────────────────────────────────────────────────────────────
def probe_endpoints():
    """Try common REST endpoint patterns and print what's available."""
    candidates = [
        "/philosophers",
        "/philosophers/1",
        "/concepts",
        "/schools",
        "/eras",
    ]
    print("\n" + "═"*60)
    print("SECTION 1 — Endpoint Discovery")
    print("═"*60)
    for path in candidates:
        url = BASE_URL + path
        try:
            r = requests.get(url, timeout=10)
            print(f"\n{'─'*50}")
            print(f"  {r.request.method} {url}  →  HTTP {r.status_code}")
            if r.ok:
                data = r.json()
                if isinstance(data, list):1
                    print(f"  Type: LIST  |  Length: {len(data)}")
                    if data:
                        print(f"  First item keys: {list(data[0].keys())}")
                elif isinstance(data, dict):
                    print(f"  Type: DICT  |  Keys: {list(data.keys())}")
                    if "results" in data:
                        results = data["results"]
                        print(f"  results[]: {len(results)} items  |  next: {data.get('next')}")
                        if results:
                            print(f"  First result keys: {list(results[0].keys())}")
        except Exception as e:
            print(f"  ERROR: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: Full first-page dump with pretty-print
# ─────────────────────────────────────────────────────────────────────────────
def dump_first_page():
    print("\n" + "═"*60)
    print("SECTION 2 — Full first-page response (/philosophers)")
    print("═"*60)
    r = requests.get(f"{BASE_URL}/philosophers", timeout=15)
    r.raise_for_status()
    data = r.json()
    print(json.dumps(data, indent=2, ensure_ascii=False)[:4000])  # cap at 4 KB
    print("  ... (truncated to 4000 chars)")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: Single record in detail
# ─────────────────────────────────────────────────────────────────────────────
def inspect_single_record():
    print("\n" + "═"*60)
    print("SECTION 3 — Single philosopher record (id=1)")
    print("═"*60)
    r = requests.get(f"{BASE_URL}/philosophers/1", timeout=10)
    if not r.ok:
        # Some APIs don't support /id — fetch list and take first
        r = requests.get(f"{BASE_URL}/philosophers", timeout=10)
        r.raise_for_status()
        data = r.json()
        record = data[0] if isinstance(data, list) else data.get("results", [{}])[0]
    else:
        record = r.json()

    pprint.pprint(record, depth=4)

    print("\n  ── Field type summary ──")
    for key, val in record.items():
        print(f"  {key:30s}: {type(val).__name__}  →  {repr(val)[:80]}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: Pagination walk — count total records across all pages
# ─────────────────────────────────────────────────────────────────────────────
def walk_pagination():
    print("\n" + "═"*60)
    print("SECTION 4 — Pagination walk (count total records)")
    print("═"*60)
    url = f"{BASE_URL}/philosophers"
    total = 0
    page = 1
    while url:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            # No pagination wrapper — single flat list
            total = len(data)
            print(f"  Single page: {total} records (no pagination)")
            break
        items = data.get("results", [])
        total += len(items)
        print(f"  Page {page:3d}: {len(items):4d} items  (running total: {total})")
        url = data.get("next")
        page += 1
    print(f"\n  ✓ TOTAL RECORDS: {total}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5: Null / missing field analysis on first 20 records
# ─────────────────────────────────────────────────────────────────────────────
def null_analysis(sample_size: int = 20):
    print("\n" + "═"*60)
    print(f"SECTION 5 — Null/missing field analysis (first {sample_size} records)")
    print("═"*60)
    r = requests.get(f"{BASE_URL}/philosophers", timeout=15)
    r.raise_for_status()
    data = r.json()
    records = data if isinstance(data, list) else data.get("results", [])
    sample = records[:sample_size]

    all_keys = set()
    for rec in sample:
        all_keys.update(rec.keys())

    print(f"  All observed fields: {sorted(all_keys)}\n")
    for key in sorted(all_keys):
        missing = sum(1 for rec in sample if rec.get(key) in (None, "", [], {}))
        print(f"  {key:30s}: {missing}/{len(sample)} records have null/empty value")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    probe_endpoints()
    dump_first_page()
    inspect_single_record()
    walk_pagination()
    null_analysis()
    print("\n✓ Philosophers API exploration complete.\n")
