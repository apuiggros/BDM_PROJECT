"""
news_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : GNews API (https://gnews.io)
Data Type     : Text (JSON articles/headlines)
Landing Zone  : s3://landing-zone/news_api/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
This script queries the GNews API for articles matching specific keywords.
It aggregates the news into a single daily snapshot and uploads it to MinIO.
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

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("news_ingest")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

NEWS_API_KEY      = os.getenv("NEWS_API_KEY")

S3_PREFIX = "news_api/raw_json"

# List of categories for top headlines. GNews supports:
# general, world, nation, business, technology, entertainment, sports, science, health
TARGET_CATEGORIES = [
    "world",
    "technology",
    "science"
]

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


def upload_to_minio(client: boto3.client, payload: bytes, object_key: str) -> None:
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info("Uploaded %d bytes → s3://%s/%s", len(payload), MINIO_BUCKET, object_key)


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    if not NEWS_API_KEY:
        logger.error("NEWS_API_KEY not found in .env! Aborting news ingestion.")
        return

    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Starting GNews API ingestion...")
    
    # Snapshot approach: We store one file per day per query, or one big file per day.
    # We will build one aggregated file per day for idempotency.
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    
    all_articles = []
    
    for category in TARGET_CATEGORIES:
        logger.info("  Fetching top trending headlines for category: '%s'", category)
        url = "https://gnews.io/api/v4/top-headlines"
        params = {
            "category": category,
            "lang": "en",  # Change to 'es' if you want Spanish news
            "country": "us",
            "max": 10,     # Max articles per category
            "apikey": NEWS_API_KEY
        }
        
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            articles = data.get("articles", [])
            
            # Tag the articles with their category
            for a in articles:
                a["_source_category"] = category
                
            all_articles.extend(articles)
            logger.info("    ✓ Found %d trending articles.", len(articles))
        except Exception as e:
            logger.warning("    ✗ Failed to fetch top headlines for '%s': %s", category, e)
            
    if not all_articles:
        logger.warning("No articles fetched. Aborting upload.")
        return
        
    payload = json.dumps(all_articles, ensure_ascii=False, indent=2).encode("utf-8")
    
    # Example: s3://landing-zone/news_api/raw_json/news_snapshot_20260408.json
    # Overwrites safely if the DAG is ran multiple times on the same day.
    obj_key = f"{S3_PREFIX}/news_snapshot_{date_str}.json"
    
    upload_to_minio(client, payload, obj_key)
    logger.info("✓ News API ingestion complete.")


if __name__ == "__main__":
    run()
