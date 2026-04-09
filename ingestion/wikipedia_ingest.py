"""
wikipedia_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Wikipedia REST API (https://en.wikipedia.org/api/rest_v1/)
Data Type     : Semi-structured (JSON — biography summary + structured metadata)
Landing Zone  : s3://landing-zone/wikipedia/raw_json/{domain}/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Fetches structured biography summaries for ALL target figures in the registry
(philosophers, scientists, authors, etc.) using the public Wikipedia REST API.
No API key required.

Data is organized by domain under a subdirectory:
  wikipedia/raw_json/philosophy/plato_wikipedia.json
  wikipedia/raw_json/science/einstein_wikipedia.json
  wikipedia/raw_json/literature/wilde_wikipedia.json

This makes Wikipedia the universal biographical backbone of the pipeline: every
historical figure in character_registry.py automatically gets a biography,
regardless of its domain.
"""

import json
import logging
import os
import sys
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
# Import ALL figures from the generic registry
from character_registry import TARGET_FIGURES

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("wikipedia_ingest")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

S3_PREFIX = "wikipedia/raw_json"

WIKIPEDIA_API_BASE = "https://en.wikipedia.org/api/rest_v1/page/summary"


# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
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
        logger.info("Created bucket: %s", bucket)
    else:
        logger.info("Bucket already exists: %s", bucket)

def upload_to_minio(client: boto3.client, data: dict, object_key: str) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info("Uploaded %d bytes → s3://%s/%s", len(payload), MINIO_BUCKET, object_key)


# ─── Wikipedia Fetch ──────────────────────────────────────────────────────────
def fetch_wikipedia_summary(wiki_title: str) -> dict | None:
    """
    Fetch the Wikipedia page summary for a given title.
    Wikipedia's REST API uses URL-encoded titles with underscores.
    Returns the full JSON response dict, or None on failure.
    """
    # Normalize: spaces → underscores (Wikipedia URL format)
    normalized = wiki_title.strip().replace(" ", "_")
    url = f"{WIKIPEDIA_API_BASE}/{normalized}"
    
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "BDM-Pipeline/1.0"})
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        logger.warning("  ✗ HTTP error fetching '%s': %s", wiki_title, e)
        return None
    except Exception as e:
        logger.warning("  ✗ Unexpected error for '%s': %s", wiki_title, e)
        return None


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    For each target figure in character_registry.py (all domains):
    1. Derive their Wikipedia article title from wikidata_label.
    2. Fetch the Wikipedia REST API summary.
    3. Upload the raw JSON to MinIO, organized by domain subfolder.
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    
    logger.info("Starting Wikipedia biography ingestion for %d figures...", len(TARGET_FIGURES))
    
    for figure in TARGET_FIGURES:
        name   = figure["api_name"]
        domain = figure["domain"]
        wiki_title = figure.get("wikidata_label", name)
        slug   = figure["gutenberg_author_slug"]
        
        # Organize files by domain for clean separation
        obj_key = f"{S3_PREFIX}/{domain}/{slug}_wikipedia.json"
        
        logger.info("[%s] Fetching Wikipedia biography for: %s", domain, name)
        data = fetch_wikipedia_summary(wiki_title)
        
        if not data:
            logger.warning("  ✗ No data returned for '%s'. Skipping.", name)
            continue
        
        extract_preview = data.get("extract", "")[:150].replace("\n", " ")
        logger.info("  ✓ %s — %s...", name, extract_preview)
        
        upload_to_minio(client, data, obj_key)
    
    logger.info("✓ Wikipedia biography ingestion complete.")


if __name__ == "__main__":
    run()
