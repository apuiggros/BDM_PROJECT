"""
cleaning/structured/wikipedia.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Wikipedia summary cleaning

Reads the Wikipedia REST API JSON for every figure from the Landing Zone
(MinIO bucket `landing-zone`, prefix `wikipedia/raw_json/<domain>/`),
flattens the nested fields we care about, and writes one row per figure
to the `trusted_wikipedia` table in `duckdb/trusted.duckdb`.

Schema
------
    figure_slug         VARCHAR PRIMARY KEY
    domain              VARCHAR
    page_id             INTEGER
    title               VARCHAR
    summary_text        VARCHAR    -- the `extract` field (already plain text)
    description         VARCHAR    -- short tagline, e.g. "French philosopher (1596–1650)"
    revision            VARCHAR    -- Wikipedia revision id (kept as VARCHAR)
    timestamp           TIMESTAMP
    thumbnail_url       VARCHAR
    original_image_url  VARCHAR
    page_url            VARCHAR    -- canonical desktop page url
    source_key          VARCHAR    -- s3 key of the landing-zone object

The Wikipedia REST API summary endpoint returns a stable shape, so cleaning
is mostly nested-dict access plus a TIMESTAMP cast.

Run
---
    python cleaning/structured/wikipedia.py
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
    IntegerType, StringType, StructField, StructType,
)

# ─── Locate sibling modules ───────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "ingestion"))
from character_registry import TARGET_FIGURES  # type: ignore  # noqa: E402

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_wikipedia")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_wikipedia"

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── Worker function ──────────────────────────────────────────────────────────
def fetch_and_parse_one(figure: dict) -> list[dict[str, Any]]:
    """
    Read one figure's Wikipedia summary JSON from MinIO and return a single
    flattened row (wrapped in a list so it can be flatMap-ed).
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

    slug   = figure["gutenberg_author_slug"]
    domain = figure["domain"]
    key    = f"wikipedia/raw_json/{domain}/{slug}_wikipedia.json"

    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read())
    except Exception as e:
        return [{"_error": f"{slug}: {e!s}"}]

    # The REST summary endpoint returns the page object at the top level.
    if not isinstance(data, dict) or not data.get("title"):
        return [{"_error": f"{slug}: unexpected JSON shape (keys={list(data)[:5]})"}]

    page_id = data.get("pageid")
    row = {
        "figure_slug":        slug,
        "domain":             domain,
        "page_id":            int(page_id) if page_id is not None else None,
        "title":              data.get("title"),
        "summary_text":       data.get("extract"),
        "description":        data.get("description"),
        "revision":           str(data["revision"]) if data.get("revision") is not None else None,
        "timestamp":          data.get("timestamp"),  # ISO string; cast in DuckDB
        "thumbnail_url":      (data.get("thumbnail")     or {}).get("source"),
        "original_image_url": (data.get("originalimage") or {}).get("source"),
        "page_url":           ((data.get("content_urls") or {}).get("desktop") or {}).get("page"),
        "source_key":         key,
    }
    return [row]


# ─── Spark schema ─────────────────────────────────────────────────────────────
WIKIPEDIA_SCHEMA = StructType([
    StructField("figure_slug",        StringType(),  False),
    StructField("domain",             StringType(),  False),
    StructField("page_id",            IntegerType(), True),
    StructField("title",              StringType(),  True),
    StructField("summary_text",       StringType(),  True),
    StructField("description",        StringType(),  True),
    StructField("revision",           StringType(),  True),
    StructField("timestamp",          StringType(),  True),  # cast to TIMESTAMP in DuckDB
    StructField("thumbnail_url",      StringType(),  True),
    StructField("original_image_url", StringType(),  True),
    StructField("page_url",           StringType(),  True),
    StructField("source_key",         StringType(),  False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_wikipedia")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Parsing %d figures via Spark RDD…", len(TARGET_FIGURES))
        rdd = spark.sparkContext.parallelize(TARGET_FIGURES, numSlices=min(4, len(TARGET_FIGURES)))
        results = rdd.flatMap(fetch_and_parse_one).collect()

        errors = [r["_error"] for r in results if "_error" in r]
        rows   = [r for r in results if "_error" not in r]
        for e in errors:
            logger.warning("  ✗ %s", e)
        if not rows:
            logger.error("No rows parsed — aborting before write.")
            return

        df  = spark.createDataFrame(rows, schema=WIKIPEDIA_SCHEMA)
        pdf = df.toPandas()  # noqa: F841 — referenced from DuckDB SQL below

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            # CAST the ISO-8601 string timestamp to a real TIMESTAMP column.
            con.execute(f"""
                CREATE TABLE {TABLE_NAME} AS
                SELECT
                    figure_slug,
                    domain,
                    page_id,
                    title,
                    summary_text,
                    description,
                    revision,
                    CAST(timestamp AS TIMESTAMP) AS timestamp,
                    thumbnail_url,
                    original_image_url,
                    page_url,
                    source_key
                FROM pdf
            """)
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_slug "
                f"ON {TABLE_NAME}(figure_slug)"
            )

            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            null_summary = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE summary_text IS NULL OR summary_text = ''"
            ).fetchone()[0]
            null_thumb = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE thumbnail_url IS NULL"
            ).fetchone()[0]

            logger.info("Wrote %d rows to %s in %s", total, TABLE_NAME, DUCKDB_PATH)
            logger.info("Coverage: summary present %d/%d, thumbnail present %d/%d",
                        total - null_summary, total, total - null_thumb, total)

            for slug, domain, title, ts, thumb in con.execute(f"""
                SELECT figure_slug, domain, title, timestamp, thumbnail_url IS NOT NULL
                FROM {TABLE_NAME} ORDER BY domain, figure_slug
            """).fetchall():
                logger.info("    %-12s %-12s thumb=%s  %s   ts=%s",
                            slug, domain, "Y" if thumb else "N", title, ts)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
