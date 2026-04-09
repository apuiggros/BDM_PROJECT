"""
gutenberg_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Project Gutenberg  (https://www.gutenberg.org/)
Data Type     : Unstructured (plain text .txt)
Landing Zone  : s3://landing-zone/gutenberg/raw_text/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Project Gutenberg hosts thousands of public-domain philosophical works.
This script fetches plain-text versions of canonical philosophy books using
two approaches:

  1. Direct download via the Gutenberg mirror (gutenberg.org/files/<id>/<id>.txt
     or gutenberg.org/ebooks/<id>.txt.utf-8).
  2. The Gutendex REST API (https://gutendex.com/) — a community-built JSON API
     that wraps Gutenberg's catalog, making it easy to search by author/subject
     and resolve download URLs programmatically.

For each book, the raw UTF-8 text is uploaded to MinIO as-is.  No stripping
of the Gutenberg boilerplate header/footer occurs at this stage; that is
reserved for the Trusted Zone transformation step.
"""

import io
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
# Import ALL figures: Gutenberg works for any historical author
from character_registry import TARGET_FIGURES

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("gutenberg_ingest")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")
S3_PREFIX        = "gutenberg/raw_text"

GUTENDEX_API = "https://gutendex.com/books"

# Preferred text formats (checked in priority order, first match wins)
TEXT_FORMATS: list[str] = [
    "text/plain; charset=utf-8",
    "text/plain; charset=us-ascii",
    "text/plain",
]


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
        logger.info("Created bucket: %s", bucket)


# ─── Upload Helpers ───────────────────────────────────────────────────────────
def upload_text(client: boto3.client, text: str, key: str) -> None:
    """Upload a raw text string to MinIO as UTF-8 bytes."""
    data = text.encode("utf-8", errors="replace")
    client.put_object(
        Bucket=MINIO_BUCKET, Key=key, Body=data, ContentType="text/plain; charset=utf-8"
    )
    logger.info("Text uploaded → s3://%s/%s  (%d bytes)", MINIO_BUCKET, key, len(data))


def upload_json_metadata(client: boto3.client, data: dict | list, key: str) -> None:
    """Upload catalog metadata as JSON for provenance tracking."""
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET, Key=key, Body=payload, ContentType="application/json"
    )
    logger.info("Metadata uploaded → s3://%s/%s", MINIO_BUCKET, key)


# ─── Gutendex API ─────────────────────────────────────────────────────────────
def search_author(philo: dict) -> list[dict]:
    """
    Search the Gutendex catalog using the gutenberg_search term from the
    registry, then filter results to only books where the target philosopher
    is an actual author (avoids books merely *about* them).
    """
    search_term = philo["gutenberg_search"]
    slug        = philo["gutenberg_author_slug"]
    params = {"search": search_term, "languages": "en"}
    logger.info("Searching Gutendex for: %s", search_term)
    try:
        response = requests.get(GUTENDEX_API, params=params, timeout=60)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.warning("Gutendex timed out for '%s' — skipping.", search_term)
        return []

    all_results = response.json().get("results", [])

    # Keep only books where this philosopher appears as an author
    def is_authored_by(book: dict) -> bool:
        return any(
            slug in a.get("name", "").lower()
            for a in book.get("authors", [])
        )

    authored = [b for b in all_results if is_authored_by(b)]
    logger.info(
        "  %s: %d results, %d confirmed as author",
        search_term, len(all_results), len(authored)
    )
    return authored


def resolve_text_url(book: dict) -> str | None:
    """
    Extract the best available plain-text download URL from a Gutendex
    book object by checking the `formats` dict in priority order.
    """
    formats: dict[str, str] = book.get("formats", {})
    for fmt in TEXT_FORMATS:
        url = formats.get(fmt)
        if url:
            return url
    # Fallback: try any key that contains "plain"
    for fmt_key, url in formats.items():
        if "plain" in fmt_key.lower():
            return url
    return None


def download_text(url: str) -> str | None:
    """
    Download the text body from a Gutenberg CDN URL.
    Returns the decoded string or None on failure.
    """
    try:
        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return None


import time

# ─── Main Ingestion Logic ─────────────────────────────────────────────────────
def ingest_author(client: boto3.client, figure: dict, timestamp: str) -> None:
    """
    For a single figure entry from the registry:
    1. Search Gutendex using the gutenberg_search term.
    2. Upload catalog metadata JSON for provenance.
    3. Download and upload ALL confirmed plain-text files (with safety throttling).
    Paths are organized by domain: gutenberg/raw_text/{domain}/
    """
    slug   = figure["gutenberg_author_slug"]
    name   = figure["api_name"]
    domain = figure["domain"]
    books  = search_author(figure)

    if not books:
        logger.warning("No books found for: %s", name)
        return

    # Upload catalog metadata (provenance record), organized by domain
    meta_key = f"{S3_PREFIX}/{domain}/{slug}_catalog.json"
    upload_json_metadata(client, books, meta_key)

    # Download and upload plain-text files
    downloaded = 0
    for book in books:
        book_id    = book.get("id", "unknown")
        title_slug = book.get("title", "untitled")[:60].replace(" ", "_").replace("/", "-")
        text_url   = resolve_text_url(book)

        if not text_url:
            logger.warning("No plain-text URL for book %s (%s) — skipping.", book_id, title_slug)
            continue
            
        key = f"{S3_PREFIX}/{domain}/{slug}_{book_id}_{title_slug}.txt"

        # Idempotency check: Skip if already exists
        try:
            client.head_object(Bucket=MINIO_BUCKET, Key=key)
            logger.info("  ↳ Already exists in MinIO: %s (Skipping download)", key)
            continue
        except Exception:
            pass  # Object does not exist, safe to proceed

        logger.info("Downloading: [%s] %s", book_id, book.get("title", "?"))
        text = download_text(text_url)
        if not text:
            continue

        upload_text(client, text, key)
        downloaded += 1

        # SAFETY: Project Gutenberg aggressively rate-limits rapid scraping.
        time.sleep(1.5)

    logger.info("✓ [%s] %s: %d book(s) ingested.", domain, name, downloaded)


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Iterate over ALL figures in TARGET_FIGURES from character_registry and ingest
    plain-text books from Project Gutenberg for each, organized by domain.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    for figure in TARGET_FIGURES:
        try:
            ingest_author(client, figure, timestamp)
        except Exception as exc:
            logger.error(
                "Ingestion failed for '%s': %s", figure["api_name"], exc, exc_info=True
            )

    logger.info("✓ Project Gutenberg ingestion complete.")


if __name__ == "__main__":
    run()
