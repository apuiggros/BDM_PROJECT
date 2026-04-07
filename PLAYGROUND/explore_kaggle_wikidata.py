"""
explore_kaggle_wikidata.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PURPOSE: Interactively explore two structured data sources:

  1. WIKIDATA SPARQL — No credentials needed, run immediately.
     Understand the shape of philosopher timeline records.

  2. KAGGLE — Requires KAGGLE_USERNAME + KAGGLE_KEY in .env.
     Inspect the CSV schema of the philosophy-quotes dataset.

Run with:  python PLAYGROUND/explore_kaggle_wikidata.py
"""

import io
import os
import textwrap
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

WIKIDATA_URL   = "https://query.wikidata.org/sparql"
WIKIDATA_AGENT = os.getenv("WIKIDATA_USER_AGENT", "BDM-Explorer/1.0")
KAGGLE_DATASET = "kauvinlucas/30000-philosophy-quotes-ai-semantics"
TMP_DIR        = Path("/tmp/bdm_playground")

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: Wikidata — small sample query, inspect result shape
# ─────────────────────────────────────────────────────────────────────────────
SPARQL_SAMPLE = """
SELECT DISTINCT
  ?philosopher ?philosopherLabel
  ?birthDate ?deathDate
  ?nationalityLabel ?influencedByLabel ?mainWorkLabel
WHERE {
  ?philosopher wdt:P31 wd:Q5 ;
               wdt:P106 wd:Q4964182 .
  OPTIONAL { ?philosopher wdt:P569 ?birthDate . }
  OPTIONAL { ?philosopher wdt:P570 ?deathDate . }
  OPTIONAL { ?philosopher wdt:P27  ?nationality . }
  OPTIONAL { ?philosopher wdt:P737 ?influencedBy . }
  OPTIONAL { ?philosopher wdt:P800 ?mainWork . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
}
LIMIT 25
"""


def explore_wikidata():
    print("\n" + "═"*60)
    print("SECTION 1 — Wikidata SPARQL (25-record sample)")
    print("═"*60)

    headers = {"Accept": "application/sparql-results+json", "User-Agent": WIKIDATA_AGENT}
    r = requests.get(WIKIDATA_URL, params={"query": SPARQL_SAMPLE, "format": "json"},
                     headers=headers, timeout=60)
    r.raise_for_status()
    raw     = r.json()
    columns = raw["head"]["vars"]
    rows    = raw["results"]["bindings"]

    print(f"\n  Columns ({len(columns)}): {columns}")
    print(f"  Rows returned      : {len(rows)}")
    print(f"\n  Raw binding example (first row):")
    for k, v in rows[0].items():
        print(f"    {k:25s}: type={v.get('type','?'):12s}  value={v.get('value','?')[:80]}")

    # Flatten to DataFrame for easier inspection
    flat = [{col: row.get(col, {}).get("value") for col in columns} for row in rows]
    df   = pd.DataFrame(flat)

    print(f"\n  DataFrame shape : {df.shape}")
    print(f"  Dtypes:\n{df.dtypes}")
    print(f"\n  First 5 rows:\n")
    print(df.head().to_string())

    print(f"\n  Null count per column:")
    print(df.isnull().sum().to_string())

    print(f"\n  Sample values — 'philosopherLabel':")
    print(df["philosopherLabel"].dropna().head(10).tolist())


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: Kaggle — download and inspect CSV schema
# ─────────────────────────────────────────────────────────────────────────────
def explore_kaggle():
    print("\n" + "═"*60)
    print("SECTION 2 — Kaggle Dataset Inspection")
    print(f"  Dataset: {KAGGLE_DATASET}")
    print("═"*60)

    username = os.getenv("KAGGLE_USERNAME", "")
    key      = os.getenv("KAGGLE_KEY", "")

    if not username or not key:
        print("\n  ⚠️  KAGGLE_USERNAME / KAGGLE_KEY not set — skipping Kaggle section.")
        print("     Add your credentials to .env and re-run.")
        return

    try:
        import kaggle
    except ImportError:
        print("  ⚠️  `kaggle` package not installed — run: pip install kaggle")
        return

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n  Downloading to {TMP_DIR} …")
    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=str(TMP_DIR), unzip=False, quiet=False)

    zip_path = next(TMP_DIR.glob("*.zip"), None)
    if not zip_path:
        print("  ❌  No zip file found after download.")
        return

    print(f"\n  Zip contents:")
    with zipfile.ZipFile(zip_path, "r") as zf:
        for entry in zf.namelist():
            print(f"    {entry}")
        # Load first CSV
        csv_files = [e for e in zf.namelist() if e.lower().endswith(".csv")]
        if not csv_files:
            print("  ❌  No CSV files found inside zip.")
            return

        for csv_name in csv_files:
            print(f"\n  ── Inspecting: {csv_name} ──")
            with zf.open(csv_name) as f:
                df = pd.read_csv(f, nrows=5000)   # Read a sample, not all rows

            print(f"  Shape          : {df.shape}  (sample of up to 5000 rows)")
            print(f"  Columns ({len(df.columns)}): {list(df.columns)}")
            print(f"\n  Dtypes:\n{df.dtypes.to_string()}")
            print(f"\n  Null count:\n{df.isnull().sum().to_string()}")
            print(f"\n  First 3 rows:\n")
            print(df.head(3).to_string())

            print(f"\n  Value samples per column:")
            for col in df.columns[:6]:   # Limit columns printed
                sample = df[col].dropna().head(3).tolist()
                print(f"    {col:25s}: {sample}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    explore_wikidata()
    explore_kaggle()
    print("\n✓ Kaggle + Wikidata exploration complete.\n")
