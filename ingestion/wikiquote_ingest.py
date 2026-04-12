"""
wikiquote_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Wikiquote MediaWiki API (https://en.wikiquote.org/w/api.php)
Data Type     : Semi-structured (JSON — verified quotes extract)
Landing Zone  : s3://landing-zone/wikiquote/raw_json/{domain}/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Fetches verified quotes for ALL target figures in the registry
using the public Wikiquote MediaWiki API.

Data is organized by domain under a subdirectory:
  wikiquote/raw_json/philosophy/plato_wikiquote.json
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
from character_registry import TARGET_FIGURES

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("wikiquote_ingest")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

S3_PREFIX = "wikiquote/raw_json"

WIKIQUOTE_API_BASE = "https://en.wikiquote.org/w/api.php"


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


# ─── Wikiquote Fetch ──────────────────────────────────────────────────────────
def fetch_wikiquote_content(title: str) -> dict | None:
    """
    Fetch the Wikiquote page content for a given title.
    Returns the full JSON response dict, or None on failure.
    """
    params = {
        "action": "query",
        "titles": title,
        "prop": "extracts",
        "format": "json",
        "exintro": "false"
    }
    
    try:
        resp = requests.get(
            WIKIQUOTE_API_BASE, 
            params=params, 
            timeout=15, 
            headers={"User-Agent": "BDM-Pipeline/1.0"}
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("  ✗ Error fetching Wikiquote for '%s': %s", title, e)
        return None


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    For each target figure in character_registry.py:
    1. Derive Wikiquote title from wikidata_label (spaces -> underscores).
    2. Fetch content via MediaWiki API.
    3. Upload raw JSON to MinIO with domain-based partitioning.
    4. Apply idempotency pre-check.
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    
    logger.info("Starting Wikiquote ingestion for %d figures...", len(TARGET_FIGURES))
    
    for figure in TARGET_FIGURES:
        name   = figure["api_name"]
        domain = figure["domain"]
        # Use wikidata_label with underscores
        title  = figure.get("wikidata_label", name).replace(" ", "_")
        slug   = figure["gutenberg_author_slug"]
        
        obj_key = f"{S3_PREFIX}/{domain}/{slug}_wikiquote.json"
        
        # Idempotency check: Skip if already exists
        try:
            client.head_object(Bucket=MINIO_BUCKET, Key=obj_key)
            logger.info("[%s] Already exists in MinIO: %s. Skipping.", domain, name)
            continue
        except client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                pass  # Object not found — safe to proceed
            else:
                logger.warning("  ✗ Unexpected S3 error checking %s: %s", obj_key, e)
                continue
            
        logger.info("[%s] Fetching Wikiquote for: %s", domain, name)
        data = fetch_wikiquote_content(title)
        
        if not data:
            continue
            
        # Presence check in MediaWiki format
        pages = data.get("query", {}).get("pages", {})
        if not pages or "-1" in pages:
            logger.warning("  ✗ Page not found on Wikiquote for '%s'.", title)
            continue
            
        upload_to_minio(client, data, obj_key)
    
    logger.info("✓ Wikiquote ingestion complete.")

if __name__ == "__main__":
    run()
