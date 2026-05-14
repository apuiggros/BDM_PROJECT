"""
philosophyse_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Stack Exchange API (philosophy.stackexchange.com)
Data Type     : Semi-structured (JSON — Q&A pairs)
Landing Zone  : s3://landing-zone/philosophy_se/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Downloads the top 500 highest-voted questions from the Philosophy Stack
Exchange, with all answer bodies embedded inline via a custom API filter.

Notes
-----
- Stack Exchange's `accepted=true` query parameter does not actually filter
  the results (verified empirically: 210/500 records still come back without
  an accepted answer). The drop-questions-without-accepted-answer step
  therefore lives in the Trusted Zone, where the design rule "we only use
  Q+A pairs downstream" justifies filtering them out.
- The Landing Zone keeps every record returned by the API as-is, in line
  with the information-preserving policy.
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

STACK_EXCHANGE_BASE = "https://api.stackexchange.com/2.3"
STACK_EXCHANGE_API = f"{STACK_EXCHANGE_BASE}/questions"
STACK_EXCHANGE_FILTER_CREATE = f"{STACK_EXCHANGE_BASE}/filters/create"

# Fields we need beyond the `default` filter so the response includes the
# accepted answer body inline with each question record.
FILTER_INCLUDES = ";".join([
    "question.body",
    "question.answers",
    "answer.body",
    "answer.score",
    "answer.is_accepted",
    "answer.creation_date",
    "answer.owner",
    "answer.answer_id",
])

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


# ─── Stack Exchange API helpers ───────────────────────────────────────────────
def create_qa_filter() -> str:
    """
    Create a Stack Exchange API filter that, on top of the `default` filter,
    inlines the question body and the full list of answers (with their bodies,
    scores, owners, etc.) inside each question response.

    Returns the opaque filter ID assigned by Stack Exchange.
    """
    params = {
        "include": FILTER_INCLUDES,
        "base": "default",
        "unsafe": "false",
    }
    resp = requests.get(STACK_EXCHANGE_FILTER_CREATE, params=params, timeout=15)
    resp.raise_for_status()
    items = resp.json().get("items", [])
    if not items or "filter" not in items[0]:
        raise RuntimeError(f"Unexpected response creating SE filter: {resp.text[:300]}")
    filter_id = items[0]["filter"]
    logger.info("  Created Stack Exchange filter: %s", filter_id)
    return filter_id


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Ingests platform-level Q&A data from Philosophy Stack Exchange:
    1. Create a custom filter that inlines answer bodies in the response.
    2. Fetch the top 500 questions sorted by votes (5 pages of 100).
    3. Tag each question with ingestion metadata.
    4. Aggregate into a single daily snapshot and upload to MinIO.
    5. Enforce SE-directed backoff (or 1-second default) between requests.

    Filtering for "has an accepted answer" happens in the Trusted Zone — see
    the module docstring for the reasoning.
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Starting Philosophy Stack Exchange ingestion...")

    qa_filter = create_qa_filter()

    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    ingested_at = datetime.now(timezone.utc).isoformat()

    all_questions = []

    # Fetch top 500 accepted-answer questions (5 pages of 100)
    for page in range(1, 6):
        logger.info("  Fetching page %d of 5 from Stack Exchange...", page)
        params = {
            "order": "desc",
            "sort": "votes",
            "site": "philosophy",
            "filter": qa_filter,    # inlines answer bodies
            "pagesize": 100,
            "page": page,
        }

        try:
            resp = requests.get(STACK_EXCHANGE_API, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("items", [])

            # Tag the questions with their ingestion timestamp
            for item in items:
                item["_ingested_at"] = ingested_at

            all_questions.extend(items)
            logger.info(
                "    ✓ Found %d questions on page %d (has_more=%s, quota_remaining=%s).",
                len(items), page, data.get("has_more"), data.get("quota_remaining"),
            )

            # Respect Stack Exchange's `backoff` directive if present,
            # otherwise default to 1s rate-limiting.
            backoff = data.get("backoff")
            time.sleep(int(backoff) if backoff else 1)
        except Exception as e:
            logger.warning("    ✗ Failed to fetch page %d: %s", page, e)

    if not all_questions:
        logger.warning("No questions fetched. Aborting upload.")
        return

    # Sanity-check: every record should have at least one answer with a body.
    missing_answers = sum(1 for q in all_questions if not q.get("answers"))
    if missing_answers:
        logger.warning(
            "  %d/%d questions came back without an inline answers array — "
            "filter may not have been applied correctly.",
            missing_answers, len(all_questions),
        )

    # Informational: how many will be kept once the Trusted Zone drops
    # questions without an accepted answer.
    with_accepted = sum(1 for q in all_questions if q.get("accepted_answer_id"))
    logger.info(
        "  %d/%d questions have an accepted answer (the rest will be dropped in the Trusted Zone).",
        with_accepted, len(all_questions),
    )
        
    payload = json.dumps(all_questions, ensure_ascii=False, indent=2).encode("utf-8")
    
    # Example: s3://landing-zone/philosophy_se/raw_json/philosophy_se_snapshot_20260408.json
    # Overwrites safely if the DAG is ran multiple times on the same day.
    obj_key = f"{S3_PREFIX}/philosophy_se_snapshot_{date_str}.json"
    
    upload_to_minio(client, payload, obj_key)
    logger.info("✓ Philosophy Stack Exchange ingestion complete.")

if __name__ == "__main__":
    run()
