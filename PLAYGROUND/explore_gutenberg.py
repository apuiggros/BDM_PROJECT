"""
explore_gutenberg.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PURPOSE: Interactively explore Project Gutenberg / Gutendex to understand:
  - Gutendex API response structure (book catalog records)
  - Available text formats for each book (plain/utf-8, html, epub, …)
  - Size and structure of raw text downloads
  - The Gutenberg boilerplate header/footer pattern (what we'll strip later)

No credentials required.
Run with:  python PLAYGROUND/explore_gutenberg.py
"""

import json
import re
import textwrap

import requests

GUTENDEX_API = "https://gutendex.com/books"

# Authors and a known book ID to drill into
TEST_AUTHORS = ["Plato", "Nietzsche, Friedrich Wilhelm", "Kant, Immanuel"]
TEST_BOOK_ID = 1497  # Plato — "The Republic" (well-known stable ID)

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: Gutendex catalog record structure
# ─────────────────────────────────────────────────────────────────────────────
def inspect_catalog_record():
    print("\n" + "═"*60)
    print(f"SECTION 1 — Gutendex catalog record (book id={TEST_BOOK_ID})")
    print("═"*60)
    r = requests.get(f"{GUTENDEX_API}/{TEST_BOOK_ID}", timeout=15)
    if not r.ok:
        # Fallback: search for it
        r = requests.get(GUTENDEX_API, params={"ids": TEST_BOOK_ID}, timeout=15)
        r.raise_for_status()
        data = r.json().get("results", [{}])[0]
    else:
        data = r.json()

    print("\n  Top-level keys:", list(data.keys()))
    print(f"\n  id      : {data.get('id')}")
    print(f"  title   : {data.get('title')}")
    print(f"  subjects: {data.get('subjects', [])[:3]}")
    print(f"  authors :")
    for auth in data.get("authors", []):
        print(f"    ► {auth.get('name')}  (born {auth.get('birth_year')} — died {auth.get('death_year')})")

    print(f"\n  Available formats (media types → URL):")
    for fmt, url in data.get("formats", {}).items():
        print(f"    {fmt:45s}: {url}")

    print(f"\n  download_count    : {data.get('download_count')}")
    print(f"  languages         : {data.get('languages')}")
    print(f"  copyright         : {data.get('copyright')}")

    return data


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: Download the plain-text version and analyse structure
# ─────────────────────────────────────────────────────────────────────────────
def inspect_raw_text(book_data: dict):
    print("\n" + "═"*60)
    print("SECTION 2 — Raw text download analysis")
    print("═"*60)

    formats = book_data.get("formats", {})
    # Try to find a plain-text URL
    text_url = None
    for fmt_key in ["text/plain; charset=utf-8", "text/plain; charset=us-ascii", "text/plain"]:
        if fmt_key in formats:
            text_url = formats[fmt_key]
            print(f"\n  Text format selected: '{fmt_key}'")
            break

    if not text_url:
        # Fallback: any plain-text-like URL
        for fmt_key, url in formats.items():
            if "plain" in fmt_key.lower():
                text_url = url
                print(f"\n  Fallback text format: '{fmt_key}'")
                break

    if not text_url:
        print("  ❌  No plain-text format found for this book.")
        return

    print(f"  URL: {text_url}")
    print("  Downloading …", end=" ", flush=True)
    r = requests.get(text_url, timeout=60)
    r.raise_for_status()
    text = r.text
    print(f"done  ({len(text):,} chars  /  {len(text.encode('utf-8')):,} bytes)")

    # Character and line stats
    lines = text.splitlines()
    print(f"\n  Line count        : {len(lines):,}")
    print(f"  Word count (approx): {len(text.split()):,}")

    # Print the first 40 lines (Gutenberg header)
    print(f"\n  ── First 40 lines (Gutenberg header / boilerplate) ──────")
    for i, line in enumerate(lines[:40], 1):
        print(f"  {i:3d}: {line[:100]}")

    # Find START marker
    start_pattern = re.compile(r"\*\*\* ?start of (this|the) project gutenberg", re.IGNORECASE)
    start_idx = next((i for i, l in enumerate(lines) if start_pattern.search(l)), None)
    if start_idx:
        print(f"\n  ► Content starts at line {start_idx + 1}:  '{lines[start_idx]}'")

    # Find END marker
    end_pattern = re.compile(r"\*\*\* ?end of (this|the) project gutenberg", re.IGNORECASE)
    end_idx = next((i for i, l in enumerate(lines) if end_pattern.search(l)), None)
    if end_idx:
        print(f"  ► Content ends at line   {end_idx + 1}:  '{lines[end_idx]}'")

    if start_idx and end_idx:
        content_lines = lines[start_idx + 1 : end_idx]
        print(f"\n  Content-only lines  : {len(content_lines):,}")
        print(f"  Header lines skipped: {start_idx + 1}")
        print(f"  Footer lines skipped: {len(lines) - end_idx}")

    # Sample paragraph from the middle of the text
    mid = len(lines) // 2
    print(f"\n  ── 10 lines from the middle (line {mid}) ──────────────────")
    for line in lines[mid: mid + 10]:
        print(f"    {line[:100]}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: Search for multiple philosophers and see available books
# ─────────────────────────────────────────────────────────────────────────────
def search_philosophers():
    print("\n" + "═"*60)
    print("SECTION 3 — Gutendex search results for philosopher authors")
    print("═"*60)

    for author in TEST_AUTHORS:
        r = requests.get(GUTENDEX_API, params={"search": author, "languages": "en"}, timeout=15)
        r.raise_for_status()
        results = r.json().get("results", [])
        print(f"\n  Author: '{author}'  →  {len(results)} results")
        for book in results[:4]:
            has_text = any("plain" in k.lower() for k in book.get("formats", {}))
            print(f"    [{book.get('id'):6d}] {book.get('title', '?')[:55]:55s}  "
                  f"dl={book.get('download_count',0):5d}  "
                  f"text={'✓' if has_text else '✗'}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: Format availability survey across 20 philosophy books
# ─────────────────────────────────────────────────────────────────────────────
def format_survey(n: int = 20):
    print("\n" + "═"*60)
    print(f"SECTION 4 — Format availability survey ({n} books)")
    print("═"*60)
    r = requests.get(GUTENDEX_API, params={"topic": "philosophy", "languages": "en"}, timeout=15)
    r.raise_for_status()
    books = r.json().get("results", [])[:n]

    format_counts: dict[str, int] = {}
    for book in books:
        for fmt in book.get("formats", {}).keys():
            # Normalise: strip charset suffix
            base_fmt = fmt.split(";")[0].strip()
            format_counts[base_fmt] = format_counts.get(base_fmt, 0) + 1

    print(f"\n  Format                      Count / {len(books)} books")
    for fmt, cnt in sorted(format_counts.items(), key=lambda x: -x[1]):
        bar = "█" * cnt
        print(f"  {fmt:35s}: {cnt:2d}  {bar}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    book_data = inspect_catalog_record()
    inspect_raw_text(book_data)
    search_philosophers()
    format_survey()
    print("\n✓ Gutenberg exploration complete.\n")
