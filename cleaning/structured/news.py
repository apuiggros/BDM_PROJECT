"""
cleaning/structured/news.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — News articles cleaning

Reads every GNews snapshot file in the Landing Zone
(`landing-zone:news_api/raw_json/news_snapshot_*.json`), normalizes the
records, deduplicates them on the GNews `id`, and writes the result to
the `trusted_news_articles` table in `duckdb/trusted.duckdb`.

Schema
------
    article_id        VARCHAR PRIMARY KEY
    title             VARCHAR
    description       VARCHAR
    content_excerpt   VARCHAR    -- truncated GNews `content` with the
                                 --   "... [N chars]" suffix stripped
    url               VARCHAR
    image_url         VARCHAR
    published_at      TIMESTAMP
    lang              VARCHAR
    source_name       VARCHAR
    source_url        VARCHAR
    category          VARCHAR    -- _source_category from the snapshot
    snapshot_date     DATE       -- parsed from the filename
    ingested_at       TIMESTAMP  -- the file's S3 LastModified

Cleaning rules:
- Strip the GNews truncation marker (e.g. "… [2065 chars]") from `content`.
- Cast `publishedAt` (ISO-8601) to TIMESTAMP at DuckDB write time.
- Parse the snapshot date out of the filename (news_snapshot_YYYYMMDD.json).
- Drop articles with empty `title`.
- Dedupe on `article_id`, keeping the row with the earliest `ingested_at`
  (i.e. the snapshot that first saw the article).

Run
---
    python cleaning/structured/news.py
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_news")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_news_articles"

PREFIX = "news_api/raw_json/"

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── Helpers ──────────────────────────────────────────────────────────────────
# GNews truncates `content` and adds "... [N chars]" or "… [N chars]".
_TRUNC_RE = re.compile(r"\s*\.{3}\s*\[\s*\d+\s*chars\s*\]\s*$")
_ELLIPSIS_TRUNC_RE = re.compile(r"\s*…\s*\[\s*\d+\s*chars\s*\]\s*$")
_SNAPSHOT_RE = re.compile(r"news_snapshot_(\d{8})\.json$")


def strip_truncation_marker(content: str | None) -> str | None:
    if not content:
        return content
    s = _TRUNC_RE.sub("", content)
    s = _ELLIPSIS_TRUNC_RE.sub("", s)
    return s.strip() or None


def parse_snapshot_date(key: str) -> str | None:
    m = _SNAPSHOT_RE.search(key)
    if not m:
        return None
    raw = m.group(1)
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def list_snapshot_objects() -> list[dict]:
    """Driver-side: list every news snapshot with its LastModified."""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    out = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".json"):
                out.append({
                    "key":           o["Key"],
                    "last_modified": o["LastModified"].isoformat(),
                })
    return out


# ─── Worker ───────────────────────────────────────────────────────────────────
def fetch_and_parse_snapshot(snapshot: dict) -> list[dict[str, Any]]:
    """
    Read one snapshot JSON from MinIO and return a list of normalized rows.
    Workers create their own boto3 clients (no shared state).
    """
    endpoint   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password")
    bucket     = os.getenv("MINIO_BUCKET",     "landing-zone")

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    key = snapshot["key"]
    snapshot_date = parse_snapshot_date(key)
    ingested_at = datetime.fromisoformat(snapshot["last_modified"]).replace(tzinfo=None)

    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        articles = json.loads(body)
    except Exception as e:  # pragma: no cover — surfaced via collect
        return [{"_error": f"{key}: {e!s}"}]

    if not isinstance(articles, list):
        return [{"_error": f"{key}: expected list, got {type(articles).__name__}"}]

    rows: list[dict[str, Any]] = []
    for art in articles:
        article_id = art.get("id")
        title = (art.get("title") or "").strip()
        if not article_id or not title:
            continue

        published_at_iso = art.get("publishedAt")
        published_at_dt = None
        if published_at_iso:
            try:
                # GNews uses Z-suffixed ISO-8601: "2026-04-29T00:30:44Z".
                published_at_dt = datetime.fromisoformat(
                    published_at_iso.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except ValueError:
                published_at_dt = None

        source = art.get("source") or {}
        rows.append({
            "article_id":      article_id,
            "title":           title,
            "description":     art.get("description"),
            "content_excerpt": strip_truncation_marker(art.get("content")),
            "url":             art.get("url"),
            "image_url":       art.get("image"),
            "published_at":    published_at_dt,
            "lang":            art.get("lang"),
            "source_name":     source.get("name"),
            "source_url":      source.get("url"),
            "category":        art.get("_source_category"),
            "snapshot_date":   snapshot_date,
            "ingested_at":     ingested_at,
        })
    return rows


# ─── Spark schema ─────────────────────────────────────────────────────────────
NEWS_SCHEMA = StructType([
    StructField("article_id",      StringType(),    False),
    StructField("title",           StringType(),    False),
    StructField("description",     StringType(),    True),
    StructField("content_excerpt", StringType(),    True),
    StructField("url",             StringType(),    True),
    StructField("image_url",       StringType(),    True),
    StructField("published_at",    TimestampType(), True),
    StructField("lang",            StringType(),    True),
    StructField("source_name",     StringType(),    True),
    StructField("source_url",      StringType(),    True),
    StructField("category",        StringType(),    True),
    StructField("snapshot_date",   StringType(),    True),  # cast → DATE in SQL
    StructField("ingested_at",     TimestampType(), True),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_news")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        snapshots = list_snapshot_objects()
        if not snapshots:
            logger.error("No snapshots found under %s — aborting.", PREFIX)
            return
        logger.info("Found %d snapshot file(s) — parsing in parallel via Spark RDD…",
                    len(snapshots))

        rdd = spark.sparkContext.parallelize(
            snapshots, numSlices=min(8, len(snapshots))
        )
        results = rdd.flatMap(fetch_and_parse_snapshot).collect()

        errors = [r["_error"] for r in results if "_error" in r]
        rows   = [r for r in results if "_error" not in r]
        for e in errors:
            logger.warning("  ✗ %s", e)
        logger.info("Parsed %d total rows across snapshots (pre-dedupe).", len(rows))

        if not rows:
            logger.error("No rows parsed — aborting before write.")
            return

        df = spark.createDataFrame(rows, schema=NEWS_SCHEMA)

        # Dedupe on article_id, keeping the earliest ingested_at.
        from pyspark.sql.window import Window
        from pyspark.sql import functions as F
        w = Window.partitionBy("article_id").orderBy(F.col("ingested_at").asc_nulls_last())
        df_deduped = (
            df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
        )

        pdf = df_deduped.toPandas()  # noqa: F841 — referenced from DuckDB SQL below

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"""
                CREATE TABLE {TABLE_NAME} AS
                SELECT
                    article_id,
                    title,
                    description,
                    content_excerpt,
                    url,
                    image_url,
                    published_at,
                    lang,
                    source_name,
                    source_url,
                    category,
                    CAST(snapshot_date AS DATE) AS snapshot_date,
                    ingested_at
                FROM pdf
            """)
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_id "
                f"ON {TABLE_NAME}(article_id)"
            )

            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            logger.info("Wrote %d unique articles to %s in %s",
                        total, TABLE_NAME, DUCKDB_PATH)

            for cat, n in con.execute(f"""
                SELECT category, count(*) FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 2 DESC
            """).fetchall():
                logger.info("    category=%-12s  rows=%d", cat, n)

            for d, n in con.execute(f"""
                SELECT snapshot_date, count(*) FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall():
                logger.info("    snapshot=%s  first-seen=%d", d, n)

            null_pub = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE published_at IS NULL"
            ).fetchone()[0]
            null_excerpt = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE content_excerpt IS NULL"
            ).fetchone()[0]
            logger.info("Coverage: published_at present %d/%d, content_excerpt present %d/%d",
                        total - null_pub, total, total - null_excerpt, total)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
