"""
philosophers_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Philosophers API  (https://philosophersapi.com/)
Data Type     : Semi-structured (JSON)
Landing Zone  : s3://landing-zone/philosophers_api/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Connects to the public Philosophers REST API, fetches the full list of
philosophers and their associated concepts/schools, then pushes each
paginated response as a timestamped JSON blob into the MinIO object store.

No data transformation takes place here; raw JSON is stored exactly as
received from the API (data-at-rest integrity preserved for the Trusted Zone).
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

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


# ─── Upload Helper ────────────────────────────────────────────────────────────
def upload_to_minio(client: boto3.client, data: dict | list, object_key: str) -> None:
    """
    Serialize `data` to JSON and upload it to MinIO under `object_key`.

    Parameters
    ----------
    client     : Boto3 S3 client pointed at MinIO.
    data       : Python dict or list to serialize.
    object_key : Full S3 key (prefix + filename) inside the bucket.
    """
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info("Uploaded %d bytes → s3://%s/%s", len(payload), MINIO_BUCKET, object_key)


# ─── Ingestion Logic ──────────────────────────────────────────────────────────
def fetch_philosophers() -> list[dict]:
    """
    Retrieve the complete list of philosophers from the API.

    The Philosophers API is paginated; this function follows the `next` link
    until all pages have been consumed and returns the aggregated records.
    """
    records: list[dict] = []
    url = f"{API_BASE_URL}/philosophers"

    while url:
        logger.info("Fetching: %s", url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        body = response.json()

        # The API may return a list directly or wrap it in a results key
        page_data = body if isinstance(body, list) else body.get("results", body)
        if isinstance(page_data, list):
            records.extend(page_data)
        else:
            records.append(page_data)

        # Follow pagination if present (adjust key to match actual API response)
        url = body.get("next") if isinstance(body, dict) else None

    logger.info("Total philosophers fetched: %d", len(records))
    return records


def fetch_concepts() -> list[dict]:
    """
    Retrieve philosophical concepts/schools from the API (if endpoint exists).
    """
    url = f"{API_BASE_URL}/concepts"
    logger.info("Fetching concepts from: %s", url)
    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        logger.warning("Concepts endpoint not found — skipping.")
        return []
    response.raise_for_status()
    body = response.json()
    return body if isinstance(body, list) else body.get("results", [])


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Orchestrate the full ingestion cycle:
    1. Connect to MinIO and ensure the bucket exists.
    2. Fetch data from all Philosophers API endpoints.
    3. Push each dataset as a timestamped JSON file to the landing zone.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    # --- Philosophers ---
    philosophers = fetch_philosophers()
    upload_to_minio(
        client,
        philosophers,
        f"{S3_PREFIX}/philosophers_{timestamp}.json",
    )

    # --- Concepts / Schools ---
    concepts = fetch_concepts()
    if concepts:
        upload_to_minio(
            client,
            concepts,
            f"{S3_PREFIX}/concepts_{timestamp}.json",
        )

    logger.info("✓ Philosophers API ingestion complete.")


if __name__ == "__main__":
    run()
