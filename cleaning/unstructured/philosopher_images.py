"""
cleaning/unstructured/philosopher_images.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Philosopher image manifest

Walks the Landing Zone images
    landing-zone:philosophers_api/raw_images/<domain>/<api_slug>/<kind>Images/<file>
parses the resolution out of the filename, and writes one row per file
to `trusted_philosopher_images` in `duckdb/trusted.duckdb`.

The bytes themselves stay in MinIO; this is purely a manifest so a
Frontend or agent can pick the right resolution without re-listing.

Schema
------
    image_id        VARCHAR PRIMARY KEY     -- md5 of source_key
    figure_slug     VARCHAR                 -- canonical slug from the registry
    api_slug        VARCHAR                 -- the slug as it appears in the path
    domain          VARCHAR
    image_type      VARCHAR                 -- face | full | illustration | thumbnail
    width           INTEGER
    height          INTEGER
    file_size_bytes BIGINT
    source_key      VARCHAR
    is_primary      BOOLEAN                 -- the highest-res face per figure

`is_primary` is the canonical choice for the Frontend / interview UI.
We pick face images by default; if a figure has no face image the
highest-resolution `full` image is chosen instead.

Run
---
    python cleaning/unstructured/philosopher_images.py
"""

from __future__ import annotations

import hashlib
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
sys.path.insert(0, str(REPO_ROOT / "ingestion"))
from character_registry import TARGET_FIGURES  # type: ignore  # noqa: E402

load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_philosopher_images")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_philosopher_images"

PREFIX = "philosophers_api/raw_images/"

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# Map api-side slug ('descartes', 'friedrich_nietzsche', …) to our canonical
# registry slug ('descartes', 'nietzsche', …).
_API_TO_REGISTRY: dict[str, str] = {}
for f in TARGET_FIGURES:
    api_name = f.get("api_name") or ""
    api_slug = api_name.lower().replace(" ", "_")
    _API_TO_REGISTRY[api_slug] = f["gutenberg_author_slug"]


# Filename patterns:
#   faceImages/face{W}x{H}.jpg
#   fullImages/full{W}x{H}.jpg
#   illustrations/ill{W}x{H}.png
#   thumbnailIllustrations/thumbnailIll{W}x{H}.png
_FILENAME_RE = re.compile(
    r"(?P<kind>face|full|ill|thumbnailIll)"
    r"(?P<w>\d+)x(?P<h>\d+)"
    r"\.(?:jpg|jpeg|png|webp)$",
    re.IGNORECASE,
)
_KIND_TO_TYPE = {
    "face":         "face",
    "full":         "full",
    "ill":          "illustration",
    "thumbnailill": "thumbnail",
}


def parse_image_object(obj: dict) -> dict | None:
    """Translate one S3 object into a manifest row, or None to skip."""
    key = obj["Key"]
    parts = key.split("/")
    # philosophers_api/raw_images/<domain>/<api_slug>/<kindFolder>/<file>
    if len(parts) < 6:
        return None
    domain   = parts[2]
    api_slug = parts[3]
    filename = parts[5]

    m = _FILENAME_RE.match(filename)
    if not m:
        return None

    image_type = _KIND_TO_TYPE.get(m.group("kind").lower())
    if image_type is None:
        return None

    figure_slug = _API_TO_REGISTRY.get(api_slug, api_slug)

    return {
        "image_id":        hashlib.md5(key.encode("utf-8")).hexdigest(),
        "figure_slug":     figure_slug,
        "api_slug":        api_slug,
        "domain":          domain,
        "image_type":      image_type,
        "width":           int(m.group("w")),
        "height":          int(m.group("h")),
        "file_size_bytes": int(obj["Size"]),
        "source_key":      key,
        "is_primary":      False,  # filled in below
    }


def list_image_rows() -> list[dict]:
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    rows: list[dict] = []
    skipped = 0
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
        for o in page.get("Contents", []):
            row = parse_image_object(o)
            if row is None:
                skipped += 1
                continue
            rows.append(row)
    if skipped:
        logger.info("Skipped %d non-image objects under %s", skipped, PREFIX)
    return rows


def mark_primaries(rows: list[dict]) -> list[dict]:
    """
    For each figure_slug, mark exactly one row as is_primary=True:
    prefer the highest-area face image; fall back to highest-area full.
    """
    by_figure: dict[str, list[dict]] = {}
    for r in rows:
        by_figure.setdefault(r["figure_slug"], []).append(r)

    for slug, group in by_figure.items():
        faces = [r for r in group if r["image_type"] == "face"]
        fulls = [r for r in group if r["image_type"] == "full"]
        pool  = faces or fulls
        if not pool:
            continue
        winner = max(pool, key=lambda r: r["width"] * r["height"])
        winner["is_primary"] = True
    return rows


IMAGE_SCHEMA = StructType([
    StructField("image_id",        StringType(),  False),
    StructField("figure_slug",     StringType(),  False),
    StructField("api_slug",        StringType(),  False),
    StructField("domain",          StringType(),  False),
    StructField("image_type",      StringType(),  False),
    StructField("width",           IntegerType(), False),
    StructField("height",          IntegerType(), False),
    StructField("file_size_bytes", LongType(),    False),
    StructField("source_key",      StringType(),  False),
    StructField("is_primary",      BooleanType(), False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_philosopher_images")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        rows = list_image_rows()
        if not rows:
            logger.error("No images parsed under %s — aborting.", PREFIX)
            return
        logger.info("Parsed %d image rows.", len(rows))

        rows = mark_primaries(rows)

        df  = spark.createDataFrame(rows, schema=IMAGE_SCHEMA)
        pdf = df.toPandas()  # noqa: F841

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM pdf")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_id "
                f"ON {TABLE_NAME}(image_id)"
            )
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_slug "
                f"ON {TABLE_NAME}(figure_slug)"
            )
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_primary "
                f"ON {TABLE_NAME}(figure_slug, is_primary)"
            )

            n_total   = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            n_primary = con.execute(
                f"SELECT count(*) FROM {TABLE_NAME} WHERE is_primary"
            ).fetchone()[0]
            logger.info("Wrote %d image rows to %s (primary picks: %d)",
                        n_total, TABLE_NAME, n_primary)

            # Per-figure breakdown.
            for slug, n_face, n_full, n_ill, n_thumb, primary_key in con.execute(f"""
                SELECT figure_slug,
                       count(*) FILTER (WHERE image_type='face')         AS n_face,
                       count(*) FILTER (WHERE image_type='full')         AS n_full,
                       count(*) FILTER (WHERE image_type='illustration') AS n_ill,
                       count(*) FILTER (WHERE image_type='thumbnail')    AS n_thumb,
                       MAX(CASE WHEN is_primary THEN source_key END)     AS primary_key
                FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall():
                logger.info("    %-25s  face=%d full=%d ill=%d thumb=%d  primary=%s",
                            slug, n_face, n_full, n_ill, n_thumb,
                            (primary_key or "—").rsplit("/", 2)[-2:])
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
