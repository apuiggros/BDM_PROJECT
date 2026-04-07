"""
kaggle_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Kaggle Datasets API  +  Wikidata SPARQL Endpoint
Data Type     : Structured (CSV) — philosophical quotes and timelines
Landing Zone  : s3://landing-zone/kaggle_wikidata/tables/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Two complementary structured data sources are ingested in this script:

  1. KAGGLE — Downloads curated philosophy-quotes CSV datasets using the
     official Kaggle Python client.  The dataset slug is configurable so
     that instructors / TAs can substitute a different dataset easily.

  2. WIKIDATA — Issues SPARQL queries against the public Wikidata endpoint
     (no API key required) to extract philosopher timelines: birth/death
     dates, nationality, main influence, and major works.  Results are
     serialized to CSV using pandas and uploaded to MinIO.

No data cleaning or transformation is performed here; CSVs are stored as-is
in the landing zone.
"""

import csv
import io
import json
import logging
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import requests
from botocore.client import Config
from dotenv import load_dotenv

# ── Allow import from the same /ingestion/ package ────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from philosopher_registry import TARGET_PHILOSOPHERS

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("kaggle_ingest")

# Kaggle dataset to download (owner/dataset-name format)
KAGGLE_DATASET = "kauvinlucas/30000-philosophy-quotes-ai-semantics"

# Temporary local directory for the downloaded zip before uploading
TMP_DIR = Path("/tmp/bdm_kaggle")

# Wikidata SPARQL endpoint
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_USER_AGENT = os.getenv(
    "WIKIDATA_USER_AGENT", "BDM-P1-Ingestor/1.0 (contact@student.example.com)"
)

# MinIO connection parameters
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")
S3_PREFIX        = "kaggle_wikidata/tables"

# SPARQL query: target philosophers only, using a VALUES clause for filtering.
# The labels are matched against the wikidata_label field in philosopher_registry.py.
def build_wikidata_sparql() -> str:
    """
    Build a targeted SPARQL query that fetches biographical and philosophical
    data for only the 5 target philosophers using a VALUES filter.
    This is far more efficient than pulling all 5000 philosophers.
    """
    labels = " ".join(
        f'"{p["wikidata_label"]}"@en' for p in TARGET_PHILOSOPHERS
    )
    return f"""
SELECT DISTINCT
  ?philosopher ?philosopherLabel
  ?birthDate ?deathDate
  ?nationalityLabel
  ?influencedByLabel
  ?mainWorkLabel
WHERE {{
  VALUES ?philosopherLabel {{ {labels} }}
  ?philosopher wdt:P31 wd:Q5 ;           # instance of: human
               wdt:P106 wd:Q4964182 .    # occupation: philosopher

  OPTIONAL {{ ?philosopher wdt:P569 ?birthDate . }}
  OPTIONAL {{ ?philosopher wdt:P570 ?deathDate . }}
  OPTIONAL {{ ?philosopher wdt:P27  ?nationality . }}
  OPTIONAL {{ ?philosopher wdt:P737 ?influencedBy . }}
  OPTIONAL {{ ?philosopher wdt:P800 ?mainWork . }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""


# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    """Return a boto3 client configured for the local MinIO instance."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)


def upload_csv(client: boto3.client, df: pd.DataFrame, key: str) -> None:
    """Serialize a DataFrame to CSV bytes and upload to MinIO."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    encoded = buf.getvalue().encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET, Key=key, Body=encoded, ContentType="text/csv"
    )
    logger.info(
        "CSV uploaded → s3://%s/%s  (%d rows, %d bytes)",
        MINIO_BUCKET, key, len(df), len(encoded),
    )


def upload_raw_bytes(client: boto3.client, data: bytes, key: str, content_type: str = "text/csv") -> None:
    """Upload raw bytes directly to MinIO (used for Kaggle CSV files)."""
    client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=data, ContentType=content_type)
    logger.info("Raw file uploaded → s3://%s/%s  (%d bytes)", MINIO_BUCKET, key, len(data))


# ─── Kaggle Ingestion ─────────────────────────────────────────────────────────
def ingest_kaggle(client: boto3.client, timestamp: str) -> None:
    """
    Download a Kaggle dataset using the kaggle CLI Python API and upload
    all CSV files found in the downloaded zip to MinIO.

    Kaggle credentials must be set as environment variables:
        KAGGLE_USERNAME and KAGGLE_KEY  (see .env file)
    """
    try:
        import kaggle  # imported here so missing credentials don't crash the module
    except OSError:
        logger.warning("KAGGLE_USERNAME or KAGGLE_KEY missing in .env — skipping Kaggle ingestion.")
        return

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading Kaggle dataset: %s", KAGGLE_DATASET)

    # Download the dataset zip into TMP_DIR
    kaggle.api.dataset_download_files(
        KAGGLE_DATASET,
        path=str(TMP_DIR),
        unzip=False,
        quiet=False,
    )

    # Find the downloaded zip and extract CSV files
    zip_path = next(TMP_DIR.glob("*.zip"), None)
    if not zip_path:
        logger.error("No zip file found in %s after download.", TMP_DIR)
        return

    logger.info("Extracting: %s", zip_path)
    with zipfile.ZipFile(zip_path, "r") as zf:
        for entry in zf.namelist():
            if not entry.lower().endswith(".csv"):
                continue
            raw_bytes = zf.read(entry)
            filename  = Path(entry).name
            key       = f"{S3_PREFIX}/kaggle_{filename.replace(' ', '_')}_{timestamp}.csv"
            upload_raw_bytes(client, raw_bytes, key)

    # Clean up temp files
    zip_path.unlink(missing_ok=True)
    logger.info("✓ Kaggle ingestion complete.")


# ─── Wikidata Ingestion ───────────────────────────────────────────────────────
def query_wikidata(sparql: str) -> list[dict]:
    """
    Execute a SPARQL SELECT query against the Wikidata public endpoint and
    return the results as a list of row-dictionaries.
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": WIKIDATA_USER_AGENT,
    }
    response = requests.get(
        WIKIDATA_SPARQL_URL,
        params={"query": sparql, "format": "json"},
        headers=headers,
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()

    # Flatten the SPARQL response into a list of plain dicts
    cols = raw["results"]["bindings"]
    rows = []
    for binding in cols:
        row = {var: binding[var]["value"] if var in binding else None
               for var in raw["head"]["vars"]}
        rows.append(row)
    return rows


def ingest_wikidata(client: boto3.client, timestamp: str) -> None:
    """
    Run the Wikidata SPARQL query and upload results as a CSV to MinIO.
    """
    sparql = build_wikidata_sparql()
    logger.info("Querying Wikidata for: %s", [p["wikidata_label"] for p in TARGET_PHILOSOPHERS])
    rows = query_wikidata(sparql)
    if not rows:
        logger.warning("Wikidata query returned 0 results — skipping upload.")
        return

    df = pd.DataFrame(rows)
    key = f"{S3_PREFIX}/wikidata_philosophers_{timestamp}.csv"
    upload_csv(client, df, key)
    logger.info("✓ Wikidata ingestion complete.")


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Orchestrate the full Kaggle + Wikidata ingestion cycle.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    ingest_kaggle(client, timestamp)
    ingest_wikidata(client, timestamp)

    logger.info("✓ kaggle_ingest.py finished.")


if __name__ == "__main__":
    run()
