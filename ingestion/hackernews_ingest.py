"""
hackernews_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Hacker News Algolia API (https://hn.algolia.com/api/v1/search)
Data Type     : Semi-structured (JSON story records)
Landing Zone  : s3://landing-zone/hackernews/raw_json/{snapshot_date}.json
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
For each figure in `character_registry.TARGET_FIGURES`, queries the Hacker
News Algolia index for stories matching a **quoted phrase** of the figure's
api_name (e.g. `"Immanuel Kant"`). Combines per-figure results into a single
daily JSON snapshot and uploads it to MinIO.

Why HN, not Reddit
------------------
Reddit's unauthenticated search endpoint started returning a browser-check
HTML page (HTTP 403) for all programmatic clients in 2026. Their OAuth flow
requires Reddit "Responsible Builder Policy" gating on the account level,
which several team accounts can't pass without verified email + karma.

The HN Algolia API is open (no auth, no key), has a quoted-phrase syntax,
returns clean JSON, and — most importantly — HN's audience genuinely
discusses our 9 figures (Kant, Einstein, Darwin etc.) with high
signal-to-noise. The top Algolia hit for "Immanuel Kant" right now is a
242-point story; for Reddit it was a Spanish "chimichanga" food post.

So: better data, no auth wall, simpler code. Same lakehouse shape — drops
into MinIO as JSON and the rest of the trusted/exploitation pipeline doesn't
care which "public discourse" source produced it.

Search params
-------------
- `query="<api_name>"`  — quoted phrase, Algolia treats it as one term.
- `tags=story`           — exclude comments and Ask/Show HN sub-types so the
                           snapshot stays clean.
- `hitsPerPage=25`       — 25 × 9 figures = up to 225 stories/day.

Idempotency
-----------
Same as the other batch sources: snapshot file is keyed by UTC date, so
re-running on the same day overwrites the same object.
"""

import json
import logging
import os
import sys
import time
import urllib.parse
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

# ── Allow import from the same /ingestion/ package ────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from character_registry import TARGET_FIGURES

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("hackernews_ingest")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "landing-zone")

S3_PREFIX = "hackernews/raw_json"

HN_SEARCH_URL = "https://hn.algolia.com/api/v1/search"
USER_AGENT = "BDM-Project/1.0 (academic demo, github.com/apuiggros/BDM_PROJECT)"

HITS_PER_FIGURE       = 25
PAUSE_BETWEEN_FIGURES = 0.5  # seconds — Algolia is generous but be polite


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


# ─── HN Algolia Fetch ─────────────────────────────────────────────────────────
def fetch_hn_for_figure(query: str, hits: int = HITS_PER_FIGURE) -> list[dict]:
    """
    One quoted-phrase search against the HN Algolia index. Returns the raw
    `hits` array (each item is one story).
    """
    quoted = f'"{query}"'
    params = {
        "query":       quoted,
        "tags":        "story",
        "hitsPerPage": hits,
    }
    try:
        resp = requests.get(
            HN_SEARCH_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("hits", [])
    except requests.RequestException as e:
        logger.warning("  ✗ Error fetching HN for '%s': %s", query, e)
        return []


def normalize_hit(hit: dict, figure_slug: str, figure_api_name: str) -> dict:
    """Project Algolia's response into our lakehouse-friendly shape."""
    return {
        "object_id":         hit.get("objectID"),
        "mentioned_figure":  figure_api_name,
        "figure_slug":       figure_slug,
        "title":             hit.get("title"),
        "url":               hit.get("url"),         # null for Ask HN self-posts
        "author":            hit.get("author"),
        "points":            hit.get("points"),
        "num_comments":      hit.get("num_comments"),
        "story_text":        hit.get("story_text"),  # populated only for self-posts
        "created_at":        hit.get("created_at"),  # ISO string from Algolia
        "created_at_i":      hit.get("created_at_i"),
        "tags":              hit.get("_tags") or [],
    }


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    snapshot_date = date.today().isoformat()
    obj_key = f"{S3_PREFIX}/{snapshot_date}.json"

    logger.info(
        "Starting HN ingestion: %d figures, hitsPerPage=%d, snapshot=%s",
        len(TARGET_FIGURES), HITS_PER_FIGURE, snapshot_date,
    )

    all_stories: list[dict] = []
    for fig in TARGET_FIGURES:
        slug = fig["gutenberg_author_slug"]
        name = fig["api_name"]
        logger.info("[%s] Searching HN for: \"%s\"", fig["domain"], name)
        hits = fetch_hn_for_figure(name)
        for hit in hits:
            all_stories.append(normalize_hit(hit, slug, name))
        time.sleep(PAUSE_BETWEEN_FIGURES)

    snapshot = {
        "snapshot_date":    snapshot_date,
        "ingested_at_utc":  datetime.now(timezone.utc).isoformat(),
        "user_agent":       USER_AGENT,
        "hits_per_figure":  HITS_PER_FIGURE,
        "total_stories":    len(all_stories),
        "stories":          all_stories,
    }

    payload = json.dumps(snapshot, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=obj_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info(
        "✓ Uploaded snapshot: %d stories, %d bytes → s3://%s/%s",
        len(all_stories), len(payload), MINIO_BUCKET, obj_key,
    )


if __name__ == "__main__":
    run()
