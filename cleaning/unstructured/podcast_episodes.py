"""
cleaning/unstructured/podcast_episodes.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Podcast episode manifest

Builds an index of every audio file under
    landing-zone:podcasts/raw_audio/<feed>/<file>.mp3
and writes it to `trusted_podcast_episodes` in `duckdb/trusted.duckdb`.

The audio bytes are NOT transformed; the trusted layer is just the manifest
that lets a downstream agent (or Exploitation-Zone job) iterate over the
episodes without re-listing MinIO.

Schema
------
    episode_id        VARCHAR PRIMARY KEY  -- md5 of source_key (stable across runs)
    feed_name         VARCHAR              -- top-level folder under raw_audio/
    episode_title     VARCHAR              -- humanized version of the filename stem
    file_size_bytes   BIGINT
    content_type      VARCHAR              -- MIME type from the object metadata
    source_key        VARCHAR              -- s3 key of the audio object
    last_modified     TIMESTAMP            -- LastModified of the object

No `mutagen` or audio-decoding dependency — duration/bitrate stay out of
the manifest until a downstream consumer actually needs them.

Run
---
    python cleaning/unstructured/podcast_episodes.py
"""

from __future__ import annotations

import hashlib
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
    LongType, StringType, StructField, StructType, TimestampType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_podcast_episodes")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_podcast_episodes"

PREFIX = "podcasts/raw_audio/"
AUDIO_EXTS = (".mp3", ".m4a", ".wav", ".ogg", ".flac")

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── Helpers ──────────────────────────────────────────────────────────────────
_EP_PREFIX_RE = re.compile(r"^ep[_\- ]+", re.IGNORECASE)


def humanize_episode_title(stem: str) -> str:
    """
    Filenames look like 'ep_The_Making_of_Season_5___BONUS' — strip the
    'ep_' prefix and turn underscores into spaces. Multiple underscores
    collapse to a single dash to preserve the 'BONUS' separators.
    """
    s = _EP_PREFIX_RE.sub("", stem)
    s = re.sub(r"_{2,}", " — ", s)
    s = s.replace("_", " ").strip()
    return s


def list_episodes() -> list[dict]:
    """Driver-side: list every audio object under the podcasts prefix."""
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    out: list[dict] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
        for o in page.get("Contents", []):
            key = o["Key"]
            if not key.lower().endswith(AUDIO_EXTS):
                continue
            # podcasts/raw_audio/<feed>/<file>.<ext>
            parts = key.split("/")
            if len(parts) < 4:
                continue
            feed_name = parts[2]
            stem      = Path(parts[3]).stem
            out.append({
                "key":           key,
                "feed_name":     feed_name,
                "episode_stem":  stem,
                "size":          int(o["Size"]),
                "last_modified": o["LastModified"].isoformat(),
            })
    return out


def build_row(item: dict) -> dict[str, Any]:
    """
    Worker: HEAD the object to get the content_type, then return a row.
    Cheap (HEAD only) so this scales to thousands of episodes if needed.
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

    key = item["key"]
    try:
        head = client.head_object(Bucket=bucket, Key=key)
        content_type = head.get("ContentType")
    except Exception:
        content_type = None

    last_modified = datetime.fromisoformat(
        item["last_modified"]
    ).replace(tzinfo=None)

    return {
        "episode_id":      hashlib.md5(key.encode("utf-8")).hexdigest(),
        "feed_name":       item["feed_name"],
        "episode_title":   humanize_episode_title(item["episode_stem"]),
        "file_size_bytes": int(item["size"]),
        "content_type":    content_type,
        "source_key":      key,
        "last_modified":   last_modified,
    }


EPISODE_SCHEMA = StructType([
    StructField("episode_id",      StringType(),    False),
    StructField("feed_name",       StringType(),    False),
    StructField("episode_title",   StringType(),    True),
    StructField("file_size_bytes", LongType(),      False),
    StructField("content_type",    StringType(),    True),
    StructField("source_key",      StringType(),    False),
    StructField("last_modified",   TimestampType(), True),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_podcast_episodes")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        episodes = list_episodes()
        if not episodes:
            logger.error("No audio files under %s — aborting.", PREFIX)
            return
        logger.info("Found %d audio files — manifesting in parallel…", len(episodes))

        rdd = spark.sparkContext.parallelize(episodes, numSlices=min(16, len(episodes)))
        rows = rdd.map(build_row).collect()

        df  = spark.createDataFrame(rows, schema=EPISODE_SCHEMA)
        pdf = df.toPandas()  # noqa: F841

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM pdf")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_id "
                f"ON {TABLE_NAME}(episode_id)"
            )
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_feed "
                f"ON {TABLE_NAME}(feed_name)"
            )

            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            total_bytes = con.execute(
                f"SELECT SUM(file_size_bytes) FROM {TABLE_NAME}"
            ).fetchone()[0] or 0
            logger.info("Wrote %d episodes (%.1f MB total) to %s",
                        total, total_bytes / (1024 * 1024), TABLE_NAME)

            for feed, n, mb in con.execute(f"""
                SELECT feed_name, count(*) AS n,
                       SUM(file_size_bytes) / 1048576.0 AS mb
                FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall():
                logger.info("    %-30s  episodes=%4d  size=%7.1f MB", feed, n, mb)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
