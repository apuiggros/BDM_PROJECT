"""
cleaning/structured/philosophers.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Philosophers API cleaning

Reads the single catalog file written by `philosophers_ingest.py`
(`landing-zone:philosophers_api/raw_json/philosophy/philosophers_catalog.json`),
which holds one record per target philosopher (5 figures), and writes
typed, deduplicated rows to `trusted_philosophers` in `duckdb/trusted.duckdb`.

Schema
------
    figure_slug         VARCHAR PRIMARY KEY  -- from _registry.gutenberg_author_slug
    name                VARCHAR
    api_id              VARCHAR              -- the philosophers-API UUID
    domain              VARCHAR              -- always 'philosophy' for this source
    school              VARCHAR              -- lowercased
    born                INTEGER              -- signed; BC = negative
    died                INTEGER              -- signed; BC = negative
    birth_date_full     VARCHAR              -- e.g. '31 March 1596' (NULL for Plato)
    death_date_full     VARCHAR              -- e.g. '11 February 1650' (NULL for Plato)
    topical_description VARCHAR              -- only present on a few records
    interests           VARCHAR[]            -- comma-split, lowercased, stripped
    wiki_title          VARCHAR
    librivox_ids        VARCHAR[]
    sep_link            VARCHAR              -- Stanford SEP url
    iep_link            VARCHAR              -- Internet IEP url
    wikipedia_link      VARCHAR              -- canonical Wikipedia url
    has_ebooks          BOOLEAN
    images_json         VARCHAR              -- raw nested images dict, JSON-encoded

Cleaning rules (information-preserving):
- Years like "427 BC" → -427; "1596 AD" → 1596; unparseable → NULL.
- `interests` (a comma-separated string) → list of trimmed lowercase tags.
- `school` lowercased; whitespace trimmed.
- `images` kept as a JSON string column (the variable nested shape of the
  source dict makes a fixed Spark struct fragile; downstream picks its
  preferred resolution via `json_extract` in DuckDB).

Run
---
    python cleaning/structured/philosophers.py
"""

from __future__ import annotations

import json
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
    ArrayType, BooleanType, IntegerType, StringType, StructField, StructType,
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
logger = logging.getLogger("trusted_philosophers")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_philosophers"

CATALOG_KEY = "philosophers_api/raw_json/philosophy/philosophers_catalog.json"

# Index registry slugs by api_name so we can attach figure_slug back on.
SLUG_BY_NAME = {f["api_name"]: f["gutenberg_author_slug"] for f in TARGET_FIGURES}

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── Helpers ──────────────────────────────────────────────────────────────────
_YEAR_RE = re.compile(r"\s*(\d+)\s*(BCE?|CE|AD)?\s*$", re.IGNORECASE)


def normalize_year(value: Any) -> int | None:
    """
    Parse a year string like '427 BC', '1596 AD', '1900 AD' into a signed
    integer (BC → negative). Returns None if unparseable.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = _YEAR_RE.match(s)
    if not m:
        return None
    year = int(m.group(1))
    suffix = (m.group(2) or "").upper()
    if suffix in ("BC", "BCE"):
        return -year
    return year


def split_interests(value: Any) -> list[str]:
    """Comma-separated string → list of trimmed lowercase tags."""
    if not value:
        return []
    return [tok.strip().lower() for tok in str(value).split(",") if tok.strip()]


def normalize_school(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip().lower()
    return s or None


def fetch_catalog() -> list[dict]:
    """Driver-side fetch of the single catalog file."""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    body = client.get_object(Bucket=bucket, Key=CATALOG_KEY)["Body"].read()
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError(f"Expected list at {CATALOG_KEY}, got {type(data).__name__}")
    return data


# ─── Per-record parser (used by Spark workers) ────────────────────────────────
def parse_record(record: dict) -> dict[str, Any]:
    name      = record.get("name")
    registry  = record.get("_registry") or {}
    links     = record.get("_academic_links") or {}

    # figure_slug should come from the registry that the ingestion script
    # already attached. Fall back to the name lookup if it's missing.
    slug = registry.get("gutenberg_author_slug") or SLUG_BY_NAME.get(name)

    return {
        "figure_slug":         slug,
        "name":                name,
        "api_id":              record.get("id"),
        "domain":              registry.get("domain") or "philosophy",
        "school":              normalize_school(record.get("school")),
        "born":                normalize_year(record.get("birthYear")),
        "died":                normalize_year(record.get("deathYear")),
        "birth_date_full":     record.get("birthDate"),
        "death_date_full":     record.get("deathDate"),
        "topical_description": record.get("topicalDescription"),
        "interests":           split_interests(record.get("interests")),
        "wiki_title":          record.get("wikiTitle"),
        "librivox_ids":        [str(i) for i in (record.get("libriVoxIDs") or [])],
        "sep_link":            links.get("stanford_sep") or record.get("speLink"),
        "iep_link":            links.get("internet_iep") or record.get("iepLink"),
        "wikipedia_link":      links.get("wikipedia"),
        "has_ebooks":          bool(record.get("hasEBooks")),
        "images_json":         json.dumps(record.get("images") or {}, ensure_ascii=False),
    }


# ─── Spark schema ─────────────────────────────────────────────────────────────
PHILOSOPHER_SCHEMA = StructType([
    StructField("figure_slug",         StringType(),               False),
    StructField("name",                StringType(),               True),
    StructField("api_id",              StringType(),               True),
    StructField("domain",              StringType(),               False),
    StructField("school",              StringType(),               True),
    StructField("born",                IntegerType(),              True),
    StructField("died",                IntegerType(),              True),
    StructField("birth_date_full",     StringType(),               True),
    StructField("death_date_full",     StringType(),               True),
    StructField("topical_description", StringType(),               True),
    StructField("interests",           ArrayType(StringType()),    False),
    StructField("wiki_title",          StringType(),               True),
    StructField("librivox_ids",        ArrayType(StringType()),    False),
    StructField("sep_link",            StringType(),               True),
    StructField("iep_link",            StringType(),               True),
    StructField("wikipedia_link",      StringType(),               True),
    StructField("has_ebooks",          BooleanType(),              False),
    StructField("images_json",         StringType(),               False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_philosophers")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        records = fetch_catalog()
        logger.info("Loaded %d records from %s", len(records), CATALOG_KEY)

        rdd = spark.sparkContext.parallelize(records, numSlices=min(4, len(records)))
        rows = rdd.map(parse_record).collect()

        # Drop any rows that ended up without a figure_slug (would violate PK).
        bad = [r for r in rows if not r.get("figure_slug")]
        rows = [r for r in rows if r.get("figure_slug")]
        for r in bad:
            logger.warning("  ✗ Dropping record without figure_slug: name=%r", r.get("name"))

        # Deduplicate on figure_slug (keep first occurrence).
        seen, deduped = set(), []
        for r in rows:
            if r["figure_slug"] in seen:
                logger.warning("  ✗ Dropping duplicate slug: %s", r["figure_slug"])
                continue
            seen.add(r["figure_slug"])
            deduped.append(r)
        rows = deduped

        if not rows:
            logger.error("No rows parsed — aborting before write.")
            return

        df  = spark.createDataFrame(rows, schema=PHILOSOPHER_SCHEMA)
        pdf = df.toPandas()  # noqa: F841 — referenced from DuckDB SQL below

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM pdf")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_slug "
                f"ON {TABLE_NAME}(figure_slug)"
            )

            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            logger.info("Wrote %d rows to %s in %s", total, TABLE_NAME, DUCKDB_PATH)

            # Per-figure summary so we can eyeball year parsing + interests.
            for slug, name, school, born, died, n_interests, has_topical in con.execute(f"""
                SELECT figure_slug, name, school, born, died,
                       len(interests) AS n_interests,
                       topical_description IS NOT NULL AS has_topical
                FROM {TABLE_NAME}
                ORDER BY born NULLS LAST
            """).fetchall():
                logger.info(
                    "    %-10s %-32s school=%-15s  born=%5s  died=%5s  interests=%d  topical=%s",
                    slug, name, school, born, died, n_interests,
                    "Y" if has_topical else "N",
                )

            # Sanity check: born < died for every figure.
            anomalies = con.execute(f"""
                SELECT figure_slug, born, died FROM {TABLE_NAME}
                WHERE born IS NOT NULL AND died IS NOT NULL AND born >= died
            """).fetchall()
            if anomalies:
                logger.warning("  ⚠ Anomalous born/died ordering: %s", anomalies)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
