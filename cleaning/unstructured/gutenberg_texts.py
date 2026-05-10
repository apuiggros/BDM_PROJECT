"""
cleaning/unstructured/gutenberg_texts.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Project Gutenberg book text cleaning

Reads each plain-text Gutenberg book from the Landing Zone
(`landing-zone:gutenberg/raw_text/<domain>/<slug>_<id>_*.txt`),
strips the Project Gutenberg boilerplate header/footer, and writes the
cleaned body to a parallel prefix:

    landing-zone:gutenberg/cleaned_text/<domain>/<slug>_<book_id>.txt

It then ALTERs the existing `trusted_gutenberg_books` table to add
`cleaned_text_key`, `cleaned_chars`, `original_chars`, and
`boilerplate_stripped`, and UPDATEs each row with the result of cleaning
that book's text.

PG markers
----------
The boilerplate is delimited by lines like
    *** START OF THE PROJECT GUTENBERG EBOOK <TITLE> ***
    *** END OF THE PROJECT GUTENBERG EBOOK <TITLE> ***
We slice between them. If either marker is missing the body is kept
as-is and `boilerplate_stripped` is set to FALSE so a downstream job
can decide what to do.

Run
---
    python cleaning/unstructured/gutenberg_texts.py
"""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, IntegerType, LongType, StringType, StructField, StructType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_gutenberg_texts")

DUCKDB_PATH    = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME     = "trusted_gutenberg_books"
RAW_PREFIX     = "gutenberg/raw_text/"
CLEANED_PREFIX = "gutenberg/cleaned_text/"

START_RE = re.compile(r"\*\*\*\s*START OF (?:THE|THIS) PROJECT GUTENBERG EBOOK[^*]*\*\*\*", re.IGNORECASE)
END_RE   = re.compile(r"\*\*\*\s*END OF (?:THE|THIS) PROJECT GUTENBERG EBOOK[^*]*\*\*\*",   re.IGNORECASE)

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def list_book_index() -> list[dict]:
    """
    Driver-side: list every Gutenberg .txt under the raw prefix and produce
    one work item per book {key, figure_slug, domain, book_id}.
    """
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    items: list[dict] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=RAW_PREFIX
    ):
        for o in page.get("Contents", []):
            key = o["Key"]
            if not key.endswith(".txt"):
                continue
            # Path: gutenberg/raw_text/<domain>/<slug>_<id>_<title>.txt
            parts = key.split("/")
            if len(parts) < 4:
                continue
            domain = parts[2]
            stem = parts[3].removesuffix(".txt")
            tokens = stem.split("_")
            slug, book_id = None, None
            for i, tok in enumerate(tokens):
                if i > 0 and tok.isdigit():
                    slug = "_".join(tokens[:i])
                    book_id = int(tok)
                    break
            if slug is None or book_id is None:
                continue
            items.append({
                "key":         key,
                "figure_slug": slug,
                "domain":      domain,
                "book_id":     book_id,
            })
    return items


def clean_and_upload(item: dict) -> dict[str, Any]:
    """
    Worker: read the raw text from MinIO, strip PG boilerplate,
    upload the cleaned bytes to the trusted prefix, return a row.
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

    raw_key = item["key"]
    book_id = int(item["book_id"])
    slug    = item["figure_slug"]
    domain  = item["domain"]

    try:
        raw_bytes = client.get_object(Bucket=bucket, Key=raw_key)["Body"].read()
        raw_text  = raw_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        return {
            "book_id":              book_id,
            "cleaned_text_key":     None,
            "original_chars":       None,
            "cleaned_chars":        None,
            "boilerplate_stripped": False,
            "_error":               f"{raw_key}: {e!s}",
        }

    original_chars = len(raw_text)

    start_match = START_RE.search(raw_text)
    end_match   = END_RE.search(raw_text)
    if start_match and end_match and end_match.start() > start_match.end():
        body = raw_text[start_match.end():end_match.start()].strip()
        boilerplate_stripped = True
    else:
        body = raw_text.strip()
        boilerplate_stripped = False

    cleaned_key = f"{CLEANED_PREFIX}{domain}/{slug}_{book_id}.txt"
    cleaned_bytes = body.encode("utf-8", errors="replace")
    try:
        client.put_object(
            Bucket=bucket,
            Key=cleaned_key,
            Body=cleaned_bytes,
            ContentType="text/plain; charset=utf-8",
        )
    except Exception as e:
        return {
            "book_id":              book_id,
            "cleaned_text_key":     None,
            "original_chars":       original_chars,
            "cleaned_chars":        None,
            "boilerplate_stripped": boilerplate_stripped,
            "_error":               f"upload {cleaned_key}: {e!s}",
        }

    return {
        "book_id":              book_id,
        "cleaned_text_key":     cleaned_key,
        "original_chars":       original_chars,
        "cleaned_chars":        len(body),
        "boilerplate_stripped": boilerplate_stripped,
    }


CLEAN_RESULT_SCHEMA = StructType([
    StructField("book_id",              LongType(),    False),
    StructField("cleaned_text_key",     StringType(),  True),
    StructField("original_chars",       IntegerType(), True),
    StructField("cleaned_chars",        IntegerType(), True),
    StructField("boilerplate_stripped", BooleanType(), False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_gutenberg_texts")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        items = list_book_index()
        if not items:
            logger.error("No Gutenberg .txt files under %s — aborting.", RAW_PREFIX)
            return
        logger.info("Found %d book texts — cleaning in parallel…", len(items))

        rdd = spark.sparkContext.parallelize(items, numSlices=min(16, len(items)))
        results = rdd.map(clean_and_upload).collect()

        errors = [(r["book_id"], r["_error"]) for r in results if "_error" in r]
        for bid, err in errors:
            logger.warning("  ✗ book_id=%s: %s", bid, err)

        rows = [{k: v for k, v in r.items() if k != "_error"} for r in results
                if r.get("cleaned_text_key")]
        if not rows:
            logger.error("Nothing cleaned successfully — aborting before write.")
            return

        df = spark.createDataFrame(rows, schema=CLEAN_RESULT_SCHEMA)
        pdf = df.toPandas()  # noqa: F841

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            # Idempotent column adds — safe to re-run.
            for col_def in (
                "cleaned_text_key VARCHAR",
                "original_chars INTEGER",
                "cleaned_chars INTEGER",
                "boilerplate_stripped BOOLEAN",
            ):
                col_name = col_def.split()[0]
                con.execute(
                    f"ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS {col_def}"
                )
                # Reset on re-run so removed rows don't keep stale values.
                con.execute(f"UPDATE {TABLE_NAME} SET {col_name} = NULL")

            con.execute(f"""
                UPDATE {TABLE_NAME} AS t
                SET cleaned_text_key     = p.cleaned_text_key,
                    original_chars       = p.original_chars,
                    cleaned_chars        = p.cleaned_chars,
                    boilerplate_stripped = p.boilerplate_stripped
                FROM pdf AS p
                WHERE t.book_id = p.book_id
            """)

            n_total   = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            n_cleaned = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE cleaned_text_key IS NOT NULL"
            ).fetchone()[0]
            n_stripped = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE boilerplate_stripped"
            ).fetchone()[0]
            avg_orig = con.execute(
                f"SELECT AVG(original_chars) FROM {TABLE_NAME} WHERE original_chars IS NOT NULL"
            ).fetchone()[0]
            avg_clean = con.execute(
                f"SELECT AVG(cleaned_chars) FROM {TABLE_NAME} WHERE cleaned_chars IS NOT NULL"
            ).fetchone()[0]

            logger.info("Updated %s: cleaned=%d/%d  boilerplate_stripped=%d",
                        TABLE_NAME, n_cleaned, n_total, n_stripped)
            logger.info("  avg original_chars=%.0f  avg cleaned_chars=%.0f  saved=%.1f%%",
                        avg_orig or 0, avg_clean or 0,
                        100.0 * (1 - (avg_clean or 0) / (avg_orig or 1)))

            for slug, n_clean, mean_c in con.execute(f"""
                SELECT figure_slug,
                       count(*) FILTER (WHERE cleaned_text_key IS NOT NULL) AS n_clean,
                       AVG(cleaned_chars) FILTER (WHERE cleaned_chars IS NOT NULL) AS mean_clean
                FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall():
                logger.info("    %-12s  cleaned=%2d  avg_chars=%s",
                            slug, n_clean,
                            f"{int(mean_c):,}" if mean_c is not None else "—")
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
