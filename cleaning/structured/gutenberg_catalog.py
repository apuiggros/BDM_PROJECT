"""
cleaning/structured/gutenberg_catalog.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Project Gutenberg catalog cleaning

Reads the per-figure Gutendex catalog files in the Landing Zone
(`landing-zone:gutenberg/raw_text/{domain}/{slug}_catalog.json`),
flattens each book record, and writes them to the `trusted_gutenberg_books`
table in `duckdb/trusted.duckdb`.

Schema
------
    book_id              INTEGER PRIMARY KEY    -- Gutendex id
    figure_slug          VARCHAR                -- from the catalog filename
    domain               VARCHAR                -- philosophy / literature / science
    title                VARCHAR
    primary_author       VARCHAR                -- authors[0].name
    primary_author_birth INTEGER
    primary_author_death INTEGER
    all_author_names     VARCHAR[]
    translator_names     VARCHAR[]
    editor_names         VARCHAR[]
    languages            VARCHAR[]
    subjects             VARCHAR[]
    bookshelves          VARCHAR[]
    copyright            BOOLEAN
    media_type           VARCHAR
    download_count       INTEGER
    summary_text         VARCHAR                -- summaries[0]
    text_url             VARCHAR                -- text/plain (utf-8 preferred)
    html_url             VARCHAR
    epub_url             VARCHAR
    has_local_text       BOOLEAN                -- a .txt sibling exists in landing zone
    local_text_key       VARCHAR                -- s3 key of that .txt
    catalog_key          VARCHAR                -- provenance

Cleaning rules
- Promote authors[0] to `primary_author*`; keep all names in `all_author_names`.
- Pick `summaries[0]` (most records have one auto-generated summary).
- Resolve format URLs by MIME type from the `formats` dict.
- Build a (figure_slug, book_id) → key map by listing the bucket; populate
  `has_local_text` and `local_text_key`.
- Dedupe on book_id: a book that appears under multiple figures (e.g. an
  anthology) keeps the first occurrence; collisions are logged.

Run
---
    python cleaning/structured/gutenberg_catalog.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, LongType, StringType,
    StructField, StructType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_gutenberg_catalog")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_gutenberg_books"

PREFIX = "gutenberg/raw_text/"

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── MinIO helpers (driver side) ──────────────────────────────────────────────
def _client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_catalog_objects() -> list[dict]:
    """Find every Gutendex catalog file."""
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    out = []
    for page in _client().get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
        for o in page.get("Contents", []):
            if o["Key"].endswith("_catalog.json"):
                # Path: gutenberg/raw_text/{domain}/{slug}_catalog.json
                parts = o["Key"].split("/")
                if len(parts) < 4:
                    continue
                domain = parts[2]
                slug   = parts[3].removesuffix("_catalog.json")
                out.append({"key": o["Key"], "domain": domain, "slug": slug})
    return out


def build_local_text_index() -> dict[tuple[str, int], str]:
    """
    Walk the gutenberg prefix and map (figure_slug, book_id) → s3 key
    for each `.txt` file.  Filenames are like
        {slug}_{book_id}_{title}.txt
    """
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    index: dict[tuple[str, int], str] = {}
    for page in _client().get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
        for o in page.get("Contents", []):
            key = o["Key"]
            if not key.endswith(".txt"):
                continue
            # gutenberg/raw_text/{domain}/{slug}_{id}_{title}.txt
            tail = key.rsplit("/", 1)[-1]
            # Split off the trailing extension, then partition into slug/id/title.
            stem = tail.removesuffix(".txt")
            # The slug is whatever the catalog file uses; figure_slug must match.
            # We don't know which underscore separates slug from book_id, so we
            # find the first all-digits chunk.
            parts = stem.split("_")
            for i, p in enumerate(parts):
                if i > 0 and p.isdigit():
                    slug = "_".join(parts[:i])
                    book_id = int(p)
                    index[(slug, book_id)] = key
                    break
    return index


# ─── Worker ───────────────────────────────────────────────────────────────────
PREFERRED_TEXT_KEYS = (
    "text/plain; charset=utf-8",
    "text/plain; charset=us-ascii",
    "text/plain",
)


def _pick_text_url(formats: dict) -> str | None:
    for k in PREFERRED_TEXT_KEYS:
        if formats.get(k):
            return formats[k]
    for k, v in (formats or {}).items():
        if "plain" in k.lower():
            return v
    return None


def _pick_first(formats: dict, *needles: str) -> str | None:
    for k, v in (formats or {}).items():
        if any(n in k.lower() for n in needles):
            return v
    return None


def fetch_and_parse_catalog(catalog: dict) -> list[dict[str, Any]]:
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

    key    = catalog["key"]
    slug   = catalog["slug"]
    domain = catalog["domain"]

    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        records = json.loads(body)
    except Exception as e:
        return [{"_error": f"{key}: {e!s}"}]

    if not isinstance(records, list):
        return [{"_error": f"{key}: expected list, got {type(records).__name__}"}]

    rows: list[dict[str, Any]] = []
    for rec in records:
        if not isinstance(rec, dict):
            continue
        book_id = rec.get("id")
        if book_id is None:
            continue

        authors     = rec.get("authors")     or []
        translators = rec.get("translators") or []
        editors     = rec.get("editors")     or []
        formats     = rec.get("formats")     or {}
        summaries   = rec.get("summaries")   or []

        primary = authors[0] if authors else {}
        rows.append({
            "book_id":              int(book_id),
            "figure_slug":          slug,
            "domain":               domain,
            "title":                rec.get("title"),
            "primary_author":       primary.get("name"),
            "primary_author_birth": primary.get("birth_year"),
            "primary_author_death": primary.get("death_year"),
            "all_author_names":     [a.get("name") for a in authors if a.get("name")],
            "translator_names":     [t.get("name") for t in translators if t.get("name")],
            "editor_names":         [e.get("name") for e in editors if e.get("name")],
            "languages":            list(rec.get("languages") or []),
            "subjects":             list(rec.get("subjects")  or []),
            "bookshelves":          list(rec.get("bookshelves") or []),
            "copyright":            bool(rec.get("copyright")) if rec.get("copyright") is not None else None,
            "media_type":           rec.get("media_type"),
            "download_count":       rec.get("download_count"),
            "summary_text":         summaries[0] if summaries else None,
            "text_url":             _pick_text_url(formats),
            "html_url":             _pick_first(formats, "html"),
            "epub_url":             _pick_first(formats, "epub"),
            "catalog_key":          key,
        })
    return rows


# ─── Spark schema ─────────────────────────────────────────────────────────────
BOOK_SCHEMA = StructType([
    StructField("book_id",              LongType(),               False),
    StructField("figure_slug",          StringType(),             False),
    StructField("domain",               StringType(),             False),
    StructField("title",                StringType(),             True),
    StructField("primary_author",       StringType(),             True),
    StructField("primary_author_birth", IntegerType(),            True),
    StructField("primary_author_death", IntegerType(),            True),
    StructField("all_author_names",     ArrayType(StringType()),  False),
    StructField("translator_names",     ArrayType(StringType()),  False),
    StructField("editor_names",         ArrayType(StringType()),  False),
    StructField("languages",            ArrayType(StringType()),  False),
    StructField("subjects",             ArrayType(StringType()),  False),
    StructField("bookshelves",          ArrayType(StringType()),  False),
    StructField("copyright",            BooleanType(),            True),
    StructField("media_type",           StringType(),             True),
    StructField("download_count",       IntegerType(),            True),
    StructField("summary_text",         StringType(),             True),
    StructField("text_url",             StringType(),             True),
    StructField("html_url",             StringType(),             True),
    StructField("epub_url",             StringType(),             True),
    StructField("catalog_key",          StringType(),             False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_gutenberg_catalog")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        catalogs = list_catalog_objects()
        if not catalogs:
            logger.error("No catalog files under %s — aborting.", PREFIX)
            return
        logger.info("Found %d catalog file(s).", len(catalogs))

        # Build the local-text index on the driver (fast: one bucket walk).
        text_index = build_local_text_index()
        logger.info("Indexed %d local .txt files.", len(text_index))

        rdd = spark.sparkContext.parallelize(catalogs, numSlices=min(9, len(catalogs)))
        results = rdd.flatMap(fetch_and_parse_catalog).collect()

        errors = [r["_error"] for r in results if "_error" in r]
        rows   = [r for r in results if "_error" not in r]
        for e in errors:
            logger.warning("  ✗ %s", e)
        logger.info("Parsed %d book rows (pre-dedupe).", len(rows))

        # Dedupe on book_id, log collisions.
        seen, deduped = {}, []
        collisions = 0
        for r in rows:
            bid = r["book_id"]
            if bid in seen:
                collisions += 1
                logger.info("  • book_id=%d appears under both %s and %s (keeping first)",
                            bid, seen[bid], r["figure_slug"])
                continue
            seen[bid] = r["figure_slug"]
            deduped.append(r)
        if collisions:
            logger.info("  → %d cross-figure collisions", collisions)
        rows = deduped

        if not rows:
            logger.error("No rows after dedupe — aborting.")
            return

        df = spark.createDataFrame(rows, schema=BOOK_SCHEMA)
        pdf = df.toPandas()  # noqa: F841 — referenced from DuckDB SQL

        # Attach has_local_text / local_text_key from the index built above.
        pdf["local_text_key"] = pdf.apply(
            lambda r: text_index.get((r["figure_slug"], int(r["book_id"]))),
            axis=1,
        )
        pdf["has_local_text"] = pdf["local_text_key"].notna()

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM pdf")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_book "
                f"ON {TABLE_NAME}(book_id)"
            )
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_slug "
                f"ON {TABLE_NAME}(figure_slug)"
            )

            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            with_text = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE has_local_text"
            ).fetchone()[0]
            with_summary = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE summary_text IS NOT NULL"
            ).fetchone()[0]

            logger.info("Wrote %d books to %s in %s", total, TABLE_NAME, DUCKDB_PATH)
            logger.info("  has_local_text: %d/%d   summary_text present: %d/%d",
                        with_text, total, with_summary, total)

            for slug, n_books, n_with_text in con.execute(f"""
                SELECT figure_slug,
                       count(*),
                       count(*) FILTER (WHERE has_local_text)
                FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall():
                logger.info("    %-12s  catalog=%3d  local_text=%3d", slug, n_books, n_with_text)

            # Top languages and a translator-presence sanity check.
            logger.info("  languages:")
            for lang, n in con.execute(f"""
                SELECT lang, count(*) FROM (
                    SELECT unnest(languages) AS lang FROM {TABLE_NAME}
                ) GROUP BY 1 ORDER BY 2 DESC LIMIT 6
            """).fetchall():
                logger.info("      %-6s %d", lang, n)
            n_with_translators = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE len(translator_names) > 0"
            ).fetchone()[0]
            n_with_editors = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE len(editor_names) > 0"
            ).fetchone()[0]
            logger.info("  translators present: %d   editors present: %d",
                        n_with_translators, n_with_editors)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
