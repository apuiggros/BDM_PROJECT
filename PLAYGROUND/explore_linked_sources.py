"""
explore_linked_sources.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PURPOSE: Validate that the 5 target philosophers can be found consistently
across all three text-based sources:

  1. Philosophers API   → biographical records (name match)
  2. Project Gutenberg  → downloadable book texts (author search)
  3. Kaggle CSV         → philosophy quotes (author column filter)

Run with:  python PLAYGROUND/explore_linked_sources.py

No MinIO or Airflow needed. Kaggle section requires KAGGLE_USERNAME + KAGGLE_KEY
in .env; all other sections are credential-free.
"""

import os
import sys
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

# ── Allow importing from the sibling /ingestion/ package ────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
from philosopher_registry import TARGET_PHILOSOPHERS, ALL_API_NAMES

load_dotenv(Path(__file__).parent.parent / ".env")

PHILOSOPHERS_API = "https://philosophersapi.com/api/philosophers"
GUTENDEX_API     = "https://gutendex.com/books"
TMP_DIR          = Path("/tmp/bdm_playground")

# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — Philosophers API: find the 5 targets in the full list
# ════════════════════════════════════════════════════════════════════════════
def check_philosophers_api():
    print("\n" + "═"*65)
    print("SECTION 1 — Philosophers API: locate target philosophers")
    print("═"*65)

    all_records = requests.get(PHILOSOPHERS_API, timeout=15).json()
    print(f"\n  Total records in API: {len(all_records)}")

    # Build name → record map (lowercase for fuzzy matching)
    name_map = {r["name"].lower(): r for r in all_records}

    found, missing = [], []
    for target in TARGET_PHILOSOPHERS:
        api_name = target["api_name"]
        record   = name_map.get(api_name.lower())
        if record:
            found.append(record)
        else:
            missing.append(api_name)

    print(f"\n  ✓ FOUND ({len(found)}/{len(TARGET_PHILOSOPHERS)}):")
    for r in found:
        has_ebooks = "📚 has eBooks" if r.get("hasEBooks") else ""
        spe        = "✓ SEP" if r.get("speLink") else "✗ SEP"
        iep        = "✓ IEP" if r.get("iepLink") else "✗ IEP"
        print(f"    ► {r['name']:40s}  school={r.get('school','?'):25s}  "
              f"{spe}  {iep}  {has_ebooks}")

    if missing:
        print(f"\n  ✗ NOT FOUND ({len(missing)}) — check spelling in registry:")
        for name in missing:
            # Suggest close matches
            candidates = [r["name"] for r in all_records
                          if any(word in r["name"] for word in name.split())]
            print(f"    '{name}'  →  possible matches: {candidates[:3]}")

    return found


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — Gutenberg/Gutendex: find texts for each target philosopher
# ════════════════════════════════════════════════════════════════════════════
def check_gutenberg():
    print("\n" + "═"*65)
    print("SECTION 2 — Gutenberg (Gutendex): text availability")
    print("═"*65)

    TEXT_FORMATS = {"text/plain; charset=utf-8", "text/plain; charset=us-ascii", "text/plain"}

    for philo in TARGET_PHILOSOPHERS:
        search = philo["gutenberg_search"]
        try:
            r = requests.get(GUTENDEX_API, params={"search": search, "languages": "en"}, timeout=60)
            r.raise_for_status()
        except requests.exceptions.Timeout:
            print(f"      ⚠️  Gutendex timed out for '{search}' — try running again later.")
            continue
        except requests.exceptions.RequestException as e:
            print(f"      ⚠️  Request failed for '{search}': {e}")
            continue
        results = r.json().get("results", [])

        # Filter to only results where this philosopher is actually an author
        def is_author(book):
            return any(search.split(",")[0].lower() in a.get("name","").lower()
                       for a in book.get("authors", []))

        authored = [b for b in results if is_author(b)]
        dl_ready = [b for b in authored if any(f in TEXT_FORMATS for f in b.get("formats", {}))]

        print(f"\n  ► {philo['api_name']}")
        print(f"    Gutendex results : {len(results)}  |  "
              f"filtered (as author): {len(authored)}  |  "
              f"with plain-text: {len(dl_ready)}")

        for book in dl_ready[:4]:
            authors_str = ", ".join(a.get("name","?") for a in book.get("authors",[]))
            print(f"      [{book['id']:6d}] {book.get('title','?')[:55]:55s}  "
                  f"dl={book.get('download_count',0):5d}  "
                  f"authors={authors_str[:40]}")

        if not dl_ready:
            print(f"      ⚠️  No downloadable plain-text found — check search term in registry")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — Kaggle: inspect which author names exist in the CSV
# ════════════════════════════════════════════════════════════════════════════
def check_kaggle():
    print("\n" + "═"*65)
    print("SECTION 3 — Kaggle CSV: author column matching")
    print("═"*65)

    # Re-load .env with override=True so values win even if vars were already
    # set (e.g. as empty strings) in the current shell environment.
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path, override=True)

    username = os.getenv("KAGGLE_USERNAME", "").strip()
    key      = os.getenv("KAGGLE_KEY", "").strip()

    print(f"\n  Credentials check:")
    print(f"    KAGGLE_USERNAME : {'SET (' + username[:3] + '***)' if username else '✗ NOT SET'}")
    print(f"    KAGGLE_KEY      : {'SET (***)' if key else '✗ NOT SET'}")

    if not username or not key:
        print("\n  ⚠️  KAGGLE credentials not set — skipping.")
        print(f"     .env path checked: {env_path}")
        print("     Make sure KAGGLE_USERNAME and KAGGLE_KEY have values (not just blank lines).")
        return

    # Push credentials into os.environ BEFORE importing kaggle,
    # because the kaggle library reads them at import time.
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"]      = key

    try:
        import kaggle
        import pandas as pd
        kaggle.api.authenticate()   # force re-auth with the env creds
    except ImportError:
        print("  ⚠️  Run: pip install kaggle pandas")
        return
    except Exception as e:
        print(f"  ✗  Kaggle authentication failed: {e}")
        return

    DATASET = "kauvinlucas/30000-philosophy-quotes-ai-semantics"
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n  Downloading dataset…")
    kaggle.api.dataset_download_files(DATASET, path=str(TMP_DIR), unzip=False, quiet=True)

    zip_path = next(TMP_DIR.glob("*.zip"), None)
    if not zip_path:
        print("  ❌  No zip found after download.")
        return

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_files = [e for e in zf.namelist() if e.lower().endswith(".csv")]
        for csv_name in csv_files:
            print(f"\n  File: {csv_name}")
            with zf.open(csv_name) as f:
                df = pd.read_csv(f)

            print(f"  Shape: {df.shape}  |  Columns: {list(df.columns)}")

            # Find the author column (flexible naming)
            author_col = next(
                (c for c in df.columns if "author" in c.lower() or "philosopher" in c.lower()),
                None
            )
            if not author_col:
                print(f"  ⚠️  No obvious author column found. All columns: {list(df.columns)}")
                continue

            print(f"  Author column: '{author_col}'")
            unique_authors = df[author_col].dropna().unique()
            print(f"  Unique authors in CSV: {len(unique_authors)}")
            print(f"  Sample: {sorted(unique_authors)[:10]}")

            print(f"\n  ── Target philosopher match results ──────────────────────")
            for philo in TARGET_PHILOSOPHERS:
                target = philo["kaggle_author"]
                # Exact match
                exact = df[df[author_col] == target]
                # Fuzzy: any word in target name appears in author column value
                key_word = target.split()[-1]  # last name
                fuzzy = df[df[author_col].str.contains(key_word, case=False, na=False)]

                print(f"\n  ► {target}")
                print(f"    Exact match : {len(exact):4d} quotes")
                print(f"    Fuzzy ('{key_word}'): {len(fuzzy):4d} quotes")
                if len(fuzzy) > 0 and len(exact) == 0:
                    # Show what the actual column value is so we can fix the registry
                    actual_names = fuzzy[author_col].unique()[:3]
                    print(f"    ⚠️  Column value differs — actual: {list(actual_names)}")
                    print(f"        → Update 'kaggle_author' in philosopher_registry.py")
                if len(exact) > 0:
                    print(f"    Sample quote: {repr(str(fuzzy.iloc[0].get('quote', fuzzy.iloc[0].iloc[1]))[:100])}")


# ════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════════════════
def print_summary(api_found):
    print("\n" + "═"*65)
    print("SUMMARY — Registry correctness report")
    print("═"*65)
    print(f"\n  Target philosophers : {ALL_API_NAMES}")
    print(f"\n  Next steps:")
    print("  1. Fix any ✗ NOT FOUND names in ingestion/philosopher_registry.py")
    print("  2. Fix any kaggle_author mismatches flagged above")
    print("  3. For Gutenberg entries with 0 plain-text books, try alternate search terms")
    print("  4. Once all three sections show green, update the ingestion scripts")


if __name__ == "__main__":
    api_found = check_philosophers_api()
    check_gutenberg()
    check_kaggle()
    print_summary(api_found)
    print("\n✓ Cross-source linkage check complete.\n")
