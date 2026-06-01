"""
cleaning/structured/hackernews.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Hacker News stories cleaning

Reads every HN snapshot file in the Landing Zone
(`landing-zone:hackernews/raw_json/{YYYY-MM-DD}.json` — one file per day,
each containing stories for all 9 figures), normalizes the records,
deduplicates them on `object_id`, derives the URL host, and writes the
result to the `trusted_hn_stories` table in `duckdb/trusted.duckdb`.

Why no Spark here
-----------------
Each daily snapshot is a single small JSON file (≤ ~225 stories × ~1 KB).
boto3 + duckdb in-process is the right size; the team's tool-justification
rule applies (don't add a JVM for hundreds of rows).

Schema
------
    object_id          VARCHAR PRIMARY KEY    -- HN's Algolia objectID
    figure_slug        VARCHAR                 -- dim_figure FK
    mentioned_figure   VARCHAR                 -- api_name used as the search query
    title              VARCHAR
    url                VARCHAR                 -- NULL for Ask HN self-posts
    host               VARCHAR                 -- derived from url (e.g. nytimes.com)
    author             VARCHAR
    points             INTEGER
    num_comments       INTEGER
    story_text         VARCHAR                 -- truncated to 1000 chars
    is_self            BOOLEAN                 -- TRUE if url IS NULL
    created_at         TIMESTAMP
    snapshot_date      DATE
    ingested_at        TIMESTAMP

Cleaning rules
--------------
- Drop rows with no `object_id`.
- Derive `host` from `url` via urllib (`nytimes.com`, `github.com`, …).
- Truncate `story_text` to 1000 chars (full text is in landing).
- Cast `created_at` (ISO string) to TIMESTAMP.
- Dedupe on `object_id`, keeping the earliest snapshot that saw it.

Run
---
    python cleaning/structured/hackernews.py
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_hn")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_hn_stories"

PREFIX = "hackernews/raw_json/"

STORY_TEXT_MAX_LEN = 1000


def _client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_snapshots(client) -> list[dict]:
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    out: list[dict] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".json"):
                out.append({
                    "key": o["Key"],
                    "last_modified": o["LastModified"].replace(tzinfo=None),
                })
    return out


def derive_host(url: str | None) -> str | None:
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc
        # Strip "www." prefix so "www.nytimes.com" and "nytimes.com" group together.
        return netloc[4:] if netloc.startswith("www.") else netloc or None
    except Exception:
        return None


def normalize_snapshot(snapshot_bytes: bytes, ingested_at: datetime) -> list[dict[str, Any]]:
    """Parse one HN snapshot JSON into a list of trusted rows."""
    payload = json.loads(snapshot_bytes)
    snap_date_raw = payload.get("snapshot_date")
    snap_date = date.fromisoformat(snap_date_raw) if snap_date_raw else None
    rows: list[dict[str, Any]] = []
    for s in payload.get("stories", []):
        object_id = s.get("object_id")
        if not object_id:
            continue
        url = s.get("url")
        story_text = s.get("story_text") or None
        if story_text and len(story_text) > STORY_TEXT_MAX_LEN:
            story_text = story_text[:STORY_TEXT_MAX_LEN]
        created_at_iso = s.get("created_at")
        created_at = None
        if created_at_iso:
            try:
                created_at = datetime.fromisoformat(
                    created_at_iso.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except ValueError:
                created_at = None
        rows.append({
            "object_id":        str(object_id),
            "figure_slug":      s.get("figure_slug"),
            "mentioned_figure": s.get("mentioned_figure"),
            "title":            s.get("title"),
            "url":              url,
            "host":             derive_host(url),
            "author":           s.get("author"),
            "points":           s.get("points"),
            "num_comments":     s.get("num_comments"),
            "story_text":       story_text,
            "is_self":          url is None,
            "created_at":       created_at,
            "snapshot_date":    snap_date,
            "ingested_at":      ingested_at,
        })
    return rows


def run() -> None:
    client = _client()
    snapshots = list_snapshots(client)
    if not snapshots:
        logger.error("No snapshots found under %s — aborting.", PREFIX)
        return
    logger.info("Found %d snapshot file(s).", len(snapshots))

    all_rows: list[dict[str, Any]] = []
    for snap in snapshots:
        body = client.get_object(
            Bucket=os.getenv("MINIO_BUCKET", "landing-zone"),
            Key=snap["key"],
        )["Body"].read()
        rows = normalize_snapshot(body, snap["last_modified"])
        logger.info("  %s → %d stories (pre-dedupe)", snap["key"], len(rows))
        all_rows.extend(rows)

    if not all_rows:
        logger.error("No rows parsed — aborting before write.")
        return

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        import pandas as pd
        pdf = pd.DataFrame(all_rows)

        con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        con.execute(f"""
            CREATE TABLE {TABLE_NAME} AS
            SELECT
                object_id,
                figure_slug,
                mentioned_figure,
                title,
                url,
                host,
                author,
                CAST(points AS INTEGER)       AS points,
                CAST(num_comments AS INTEGER) AS num_comments,
                story_text,
                is_self,
                created_at,
                CAST(snapshot_date AS DATE)   AS snapshot_date,
                ingested_at
            FROM pdf
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY object_id
                ORDER BY ingested_at ASC NULLS LAST
            ) = 1
        """)
        con.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_id "
            f"ON {TABLE_NAME}(object_id)"
        )

        total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
        figures = con.execute(
            f"SELECT count(DISTINCT figure_slug) FROM {TABLE_NAME}"
        ).fetchone()[0]
        hosts = con.execute(
            f"SELECT count(DISTINCT host) FROM {TABLE_NAME} WHERE host IS NOT NULL"
        ).fetchone()[0]
        logger.info(
            "Wrote %d unique stories | %d figures | %d distinct hosts",
            total, figures, hosts,
        )

        for slug, n in con.execute(f"""
            SELECT figure_slug, count(*) FROM {TABLE_NAME}
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall():
            logger.info("    figure=%-12s stories=%d", slug, n)
    finally:
        con.close()


if __name__ == "__main__":
    run()
