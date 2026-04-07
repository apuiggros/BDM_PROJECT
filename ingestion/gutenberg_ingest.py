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
logger = logging.getLogger("gutenberg_ingest")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")
S3_PREFIX        = "gutenberg/raw_text"

GUTENDEX_API     = "https://gutendex.com/books"

# Philosopher authors to search for via Gutendex (name as it appears in catalog)
TARGET_AUTHORS: list[str] = [
    "Plato",
    "Aristotle",
    "Nietzsche, Friedrich Wilhelm",
    "Kant, Immanuel",
    "Descartes, René",
    "Hume, David",
    "Locke, John",
    "Rousseau, Jean-Jacques",
    "Voltaire",
    "Schopenhauer, Arthur",
]

# Maximum books to download per author per run
MAX_BOOKS_PER_AUTHOR: int = 3

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
def search_author(author_name: str) -> list[dict]:
    """
    Search the Gutendex catalog for books by a specific author.
    Returns a list of book metadata objects.
    """
    params = {"search": author_name, "languages": "en"}
    logger.info("Searching Gutendex for: %s", author_name)
    response = requests.get(GUTENDEX_API, params=params, timeout=30)
    response.raise_for_status()
    return response.json().get("results", [])


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


# ─── Main Ingestion Logic ─────────────────────────────────────────────────────
def ingest_author(client: boto3.client, author: str, timestamp: str) -> None:
    """
    For a given philosopher author:
    1. Search Gutendex for their English public-domain works.
    2. Download and upload each text file to MinIO (up to MAX_BOOKS_PER_AUTHOR).
    3. Upload the catalog metadata JSON for provenance.
    """
    books = search_author(author)
    if not books:
        logger.warning("No books found for author: %s", author)
        return

    # Normalise author name for use in S3 keys (remove commas/spaces)
    author_slug = author.replace(",", "").replace(" ", "_").lower()

    # Upload catalog metadata for this author
    meta_key = f"{S3_PREFIX}/{author_slug}_catalog_{timestamp}.json"
    upload_json_metadata(client, books[:MAX_BOOKS_PER_AUTHOR], meta_key)

    # Download each book's plain text
    downloaded = 0
    for book in books:
        if downloaded >= MAX_BOOKS_PER_AUTHOR:
            break

        book_id    = book.get("id", "unknown")
        title_slug = book.get("title", "untitled")[:60].replace(" ", "_").replace("/", "-")
        text_url   = resolve_text_url(book)

        if not text_url:
            logger.warning("No plain-text format found for book %s (%s) — skipping.", book_id, title_slug)
            continue

        logger.info("Downloading: [%s] %s", book_id, book.get("title", "?"))
        text = download_text(text_url)
        if not text:
            continue

        key = f"{S3_PREFIX}/{author_slug}_{book_id}_{title_slug}_{timestamp}.txt"
        upload_text(client, text, key)
        downloaded += 1

    logger.info("Author '%s': %d book(s) ingested.", author, downloaded)


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Iterate over the TARGET_AUTHORS list and ingest texts from Project Gutenberg
    for each philosopher via the Gutendex API.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    for author in TARGET_AUTHORS:
        try:
            ingest_author(client, author, timestamp)
        except Exception as exc:
            # Catch broad exceptions so one failed author doesn't abort the run
            logger.error("Ingestion failed for author '%s': %s", author, exc, exc_info=True)

    logger.info("✓ Project Gutenberg ingestion complete.")


if __name__ == "__main__":
    run()
