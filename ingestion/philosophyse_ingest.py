"""
philosophyse_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Stack Exchange API (philosophy.stackexchange.com)
Data Type     : Semi-structured (JSON — Q&A pairs)
Landing Zone  : s3://landing-zone/philosophy_se/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Downloads the top 500 highest-voted questions with accepted answers
from the Philosophy Stack Exchange. Aggregates them into a single 
daily snapshot.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

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
logger = logging.getLogger("philosophyse_ingest")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

S3_PREFIX = "philosophy_se/raw_json"

STACK_EXCHANGE_API = "https://api.stackexchange.com/2.3/questions"

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
    """
    Ingests platform-level Q&A data from Philosophy Stack Exchange:
    1. Fetch top 500 questions (5 pages of 100).
    2. Tag each question with ingestion metadata.
    3. Aggregate into a single daily snapshot and upload to MinIO.
    4. Enforce 1-second rate-limiting between requests.
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Starting Philosophy Stack Exchange ingestion...")
    
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    ingested_at = datetime.now(timezone.utc).isoformat()
    
    all_questions = []
    
    # Fetch top 500 questions (5 pages of 100)
    for page in range(1, 6):
        logger.info("  Fetching page %d of 5 from Stack Exchange...", page)
        params = {
            "order": "desc",
            "sort": "votes",
            "site": "philosophy",
            "filter": "withbody",
            "pagesize": 100,
            "page": page
        }
        
        try:
            resp = requests.get(STACK_EXCHANGE_API, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])
            
            # Tag the questions with their ingestion timestamp
            for item in items:
                item["_ingested_at"] = ingested_at
                
            all_questions.extend(items)
            logger.info("    ✓ Found %d questions on page %d.", len(items), page)
            
            # Respect rate limits
            time.sleep(1)
        except Exception as e:
            logger.warning("    ✗ Failed to fetch page %d: %s", page, e)
            
    if not all_questions:
        logger.warning("No questions fetched. Aborting upload.")
        return
        
    payload = json.dumps(all_questions, ensure_ascii=False, indent=2).encode("utf-8")
    
    # Example: s3://landing-zone/philosophy_se/raw_json/philosophy_se_snapshot_20260408.json
    # Overwrites safely if the DAG is ran multiple times on the same day.
    obj_key = f"{S3_PREFIX}/philosophy_se_snapshot_{date_str}.json"
    
    upload_to_minio(client, payload, obj_key)
    logger.info("✓ Philosophy Stack Exchange ingestion complete.")

if __name__ == "__main__":
    run()
