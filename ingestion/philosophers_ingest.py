"""
philosophers_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Philosophers API  (https://philosophersapi.com/)
Data Type     : Semi-structured (JSON)
Landing Zone  : s3://landing-zone/philosophers_api/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Connects to the public Philosophers REST API and fetches the full list of 114
philosophers (single flat list — no pagination). Records are filtered to only
the 5 target philosophers defined in philosopher_registry.py, and stored as
a timestamped JSON blob in MinIO.

Also stores the SEP/IEP academic article URLs for each target so a future
scraping step can enrich the dataset without re-hitting this API.

No data transformation occurs here; raw JSON is preserved as-is.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

# ── Allow import from the same /ingestion/ package ────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
# Import from the new generic registry; filter to philosophy domain only
# (this script hits the Philosophers API which is specific to philosophers)
from character_registry import figures_by_domain, ALL_API_NAMES
TARGET_PHILOSOPHERS = figures_by_domain("philosophy")

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("philosophers_ingest")

# API base URL (no auth required — public REST API)
API_BASE_URL = "https://philosophersapi.com/api"

# MinIO connection parameters (read from .env)
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

# S3 prefix inside the bucket that maps to the landing zone folder
S3_PREFIX = "philosophers_api/raw_json"


# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    """
    Build and return a boto3 S3 client configured to talk to the local
    MinIO instance.  The `path` addressing style is required for MinIO.
    """
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",          # MinIO ignores this, but boto3 needs it
    )


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    """Create the MinIO bucket if it does not already exist."""
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)
    else:
        logger.info("Bucket already exists: %s", bucket)


# ─── Upload Helpers ───────────────────────────────────────────────────────────
def upload_to_minio(client: boto3.client, data: dict | list, object_key: str) -> None:
    """Serialize `data` to JSON and upload it to MinIO."""
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info("Uploaded %d bytes → s3://%s/%s", len(payload), MINIO_BUCKET, object_key)

def upload_binary_to_minio(client: boto3.client, data: bytes, object_key: str, content_type: str) -> None:
    """Upload raw bytes (e.g. images) to MinIO."""
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=data,
        ContentType=content_type,
    )
    logger.info("Uploaded image (%d bytes) → s3://%s/%s", len(data), MINIO_BUCKET, object_key)


# ─── Ingestion Logic ──────────────────────────────────────────────────────────
def fetch_target_philosophers() -> list[dict]:
    """
    Fetch all 114 records from the API and filter to only the target figures
    belonging to the 'philosophy' domain.
    """
    logger.info("Fetching full philosopher list from API…")
    response = requests.get(f"{API_BASE_URL}/philosophers", timeout=30)
    response.raise_for_status()
    all_records: list[dict] = response.json()
    logger.info("Total records from API: %d", len(all_records))

    # Build lookup by lowercase name
    name_map = {r["name"].lower(): r for r in all_records}

    filtered: list[dict] = []
    for philo in TARGET_PHILOSOPHERS:
        target_name = philo["api_name"]
        record = name_map.get(target_name.lower())
        if record:
            # Attach registry metadata for path building
            record["_registry"] = philo
            record["_academic_links"] = {
                "stanford_sep": record.get("speLink"),
                "internet_iep": record.get("iepLink"),
                "wikipedia":    f"https://en.wikipedia.org/wiki/{record.get('wikiTitle','').replace(' ', '_')}",
            }
            filtered.append(record)
            logger.info("  ✓ Found: %s (school: %s)", record["name"], record.get("school", "?"))
        else:
            logger.warning("  ✗ NOT FOUND in API: '%s' — check registry spelling", target_name)

    logger.info("Target philosophers matched: %d/%d", len(filtered), len(TARGET_PHILOSOPHERS))
    return filtered


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Orchestrate the full ingestion cycle:
    1. Connect to MinIO and ensure the bucket exists.
    2. Fetch all API records and filter to the 5 target philosophers.
    3. Store filtered records as a timestamped JSON file.
    4. Download and store all associated images (thumbnails, illustrations, face crops).
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    philosophers = fetch_target_philosophers()
    if not philosophers:
        logger.error("No target philosophers found — aborting upload.")
        return

    # 1. Upload the core metadata JSON
    # metadata is stored per-domain for consistency
    upload_to_minio(
        client,
        philosophers,
        f"{S3_PREFIX}/philosophy/philosophers_catalog.json",
    )

    # 2. Extract and download all images for these specific philosophers
    logger.info("Fetching images for target philosophers...")
    BASE_DOMAIN = "https://philosophersapi.com"
    
    for philo in philosophers:
        slug = philo["name"].lower().replace(" ", "_")
        images_dict = philo.get("images") or {}
        
        for category, urls in images_dict.items():
            for image_key, url_path in urls.items():
                if not url_path:
                    continue
                full_url = f"{BASE_DOMAIN}{url_path}"
                try:
                    # Determine extension (mostly .jpg or .png)
                    ext = url_path.split(".")[-1].lower() if "." in url_path else "jpg"
                    content_type = f"image/{ext}" if ext in ["png", "jpeg", "jpg"] else "application/octet-stream"
                    
                    domain  = philo["_registry"]["domain"]
                    slug    = philo["_registry"]["gutenberg_author_slug"]
                    obj_key = f"philosophers_api/raw_images/{domain}/{slug}/{category}/{image_key}.{ext}"
                    
                    # Idempotency check: Skip if already exists
                    try:
                        client.head_object(Bucket=MINIO_BUCKET, Key=obj_key)
                        continue
                    except Exception:
                        pass  # Object does not exist, safe to proceed
                        
                    resp = requests.get(full_url, timeout=15)
                    resp.raise_for_status()
                    
                    upload_binary_to_minio(client, resp.content, obj_key, content_type)
                except Exception as e:
                    logger.warning("Failed to download image %s: %s", full_url, e)

    logger.info("✓ Philosophers API metadata and image ingestion complete.")


if __name__ == "__main__":
    run()
