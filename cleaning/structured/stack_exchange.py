"""
cleaning/structured/stack_exchange.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Philosophy Stack Exchange Q&A cleaning

Reads every Stack Exchange snapshot in the Landing Zone
(`landing-zone:philosophy_se/raw_json/philosophy_se_snapshot_*.json`)
and writes two normalized tables to `duckdb/trusted.duckdb`:

    trusted_se_questions   — one row per question
    trusted_se_answers     — one row per answer (FK question_id)

Schemas
-------
    trusted_se_questions
        question_id          INTEGER PRIMARY KEY
        title                VARCHAR
        body_text            VARCHAR     -- HTML stripped to plain text
        body_html            VARCHAR     -- original HTML preserved
        score                INTEGER
        view_count           INTEGER
        answer_count         INTEGER
        accepted_answer_id   INTEGER     -- nullable
        tags                 VARCHAR[]
        creation_date        TIMESTAMP
        last_activity_date   TIMESTAMP
        link                 VARCHAR
        owner_user_id        INTEGER
        owner_display_name   VARCHAR
        snapshot_date        DATE
        ingested_at          TIMESTAMP

    trusted_se_answers
        answer_id            INTEGER PRIMARY KEY
        question_id          INTEGER     -- FK
        body_text            VARCHAR
        body_html            VARCHAR
        score                INTEGER
        is_accepted          BOOLEAN
        creation_date        TIMESTAMP
        owner_user_id        INTEGER
        owner_display_name   VARCHAR
        ingested_at          TIMESTAMP

Cleaning rules
- Strip HTML to plain text via BeautifulSoup; keep both columns.
- Cast Unix-second timestamps → TIMESTAMP at DuckDB write time.
- Filter at write-time: keep only questions where at least one answer has
  a non-empty body. We do NOT require an accepted answer.
- Dedupe both tables on their natural key, keeping the earliest
  `ingested_at` (i.e. the snapshot that first saw the row).

Run
---
    python cleaning/structured/stack_exchange.py
"""

from __future__ import annotations

import html
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
    ArrayType, BooleanType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

# ─── Configuration ────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("trusted_se")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
QUESTIONS_TABLE = "trusted_se_questions"
ANSWERS_TABLE   = "trusted_se_answers"

PREFIX = "philosophy_se/raw_json/"
_SNAPSHOT_RE = re.compile(r"philosophy_se_snapshot_(\d{8})\.json$")

os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def parse_snapshot_date(key: str) -> str | None:
    m = _SNAPSHOT_RE.search(key)
    if not m:
        return None
    raw = m.group(1)
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def list_snapshot_objects() -> list[dict]:
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
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
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
    Read one SE snapshot JSON and emit a list of tagged rows
    ({'_kind': 'question'|'answer', ...}). Errors are tagged with
    {'_error': ...} so they collect cleanly.
    """
    from bs4 import BeautifulSoup  # imported on the worker

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
    file_ingested_at = datetime.fromisoformat(
        snapshot["last_modified"]
    ).replace(tzinfo=None)

    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        questions = json.loads(body)
    except Exception as e:
        return [{"_error": f"{key}: {e!s}"}]

    if not isinstance(questions, list):
        return [{"_error": f"{key}: expected list, got {type(questions).__name__}"}]

    def html_to_text(html: str | None) -> str | None:
        if not html:
            return None
        return BeautifulSoup(html, "lxml").get_text(" ", strip=True) or None

    def ts(unix_seconds: Any) -> datetime | None:
        try:
            return datetime.utcfromtimestamp(int(unix_seconds))
        except (TypeError, ValueError):
            return None

    def per_record_ingested(rec: dict) -> datetime:
        v = rec.get("_ingested_at")
        if not v:
            return file_ingested_at
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).replace(tzinfo=None)
        except (TypeError, ValueError):
            return file_ingested_at

    out: list[dict[str, Any]] = []
    for q in questions:
        if not isinstance(q, dict):
            continue
        q_ingested = per_record_ingested(q)

        # Build answer rows first; we need them to know whether the question
        # is keepable (≥1 answer with non-empty body).
        kept_answers: list[dict[str, Any]] = []
        for a in (q.get("answers") or []):
            if not isinstance(a, dict):
                continue
            a_html = a.get("body")
            a_text = html_to_text(a_html)
            if not a_text:
                continue
            owner = a.get("owner") or {}
            kept_answers.append({
                "_kind":              "answer",
                "answer_id":          a.get("answer_id"),
                "question_id":        a.get("question_id") or q.get("question_id"),
                "body_text":          a_text,
                "body_html":          a_html,
                "score":              a.get("score"),
                "is_accepted":        bool(a.get("is_accepted")),
                "creation_date":      ts(a.get("creation_date")),
                "owner_user_id":      owner.get("user_id"),
                "owner_display_name": owner.get("display_name"),
                "ingested_at":        q_ingested,
            })

        if not kept_answers:
            continue  # filter rule: skip questions with no usable answer

        # Determine accepted_answer_id (an answer flagged is_accepted, if any).
        accepted_id = next(
            (a["answer_id"] for a in kept_answers if a["is_accepted"]),
            None,
        )

        owner = q.get("owner") or {}
        q_html = q.get("body")
        out.append({
            "_kind":              "question",
            "question_id":        q.get("question_id"),
            "title":              (html.unescape(q.get("title") or "").strip() or None),
            "body_text":          html_to_text(q_html),
            "body_html":          q_html,
            "score":              q.get("score"),
            "view_count":         q.get("view_count"),
            "answer_count":       q.get("answer_count"),
            "accepted_answer_id": accepted_id,
            "tags":               list(q.get("tags") or []),
            "creation_date":      ts(q.get("creation_date")),
            "last_activity_date": ts(q.get("last_activity_date")),
            "link":               q.get("link"),
            "owner_user_id":      owner.get("user_id"),
            "owner_display_name": owner.get("display_name"),
            "snapshot_date":      snapshot_date,
            "ingested_at":        q_ingested,
        })
        out.extend(kept_answers)
    return out


# ─── Spark schemas ────────────────────────────────────────────────────────────
QUESTION_SCHEMA = StructType([
    StructField("question_id",        LongType(),              False),
    StructField("title",              StringType(),            True),
    StructField("body_text",          StringType(),            True),
    StructField("body_html",          StringType(),            True),
    StructField("score",              IntegerType(),           True),
    StructField("view_count",         IntegerType(),           True),
    StructField("answer_count",       IntegerType(),           True),
    StructField("accepted_answer_id", LongType(),              True),
    StructField("tags",               ArrayType(StringType()), False),
    StructField("creation_date",      TimestampType(),         True),
    StructField("last_activity_date", TimestampType(),         True),
    StructField("link",               StringType(),            True),
    StructField("owner_user_id",      LongType(),              True),
    StructField("owner_display_name", StringType(),            True),
    StructField("snapshot_date",      StringType(),            True),  # → DATE in SQL
    StructField("ingested_at",        TimestampType(),         True),
])

ANSWER_SCHEMA = StructType([
    StructField("answer_id",          LongType(),      False),
    StructField("question_id",        LongType(),      False),
    StructField("body_text",          StringType(),    True),
    StructField("body_html",          StringType(),    True),
    StructField("score",              IntegerType(),   True),
    StructField("is_accepted",        BooleanType(),   False),
    StructField("creation_date",      TimestampType(), True),
    StructField("owner_user_id",      LongType(),      True),
    StructField("owner_display_name", StringType(),    True),
    StructField("ingested_at",        TimestampType(), True),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_stack_exchange")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        snapshots = list_snapshot_objects()
        if not snapshots:
            logger.error("No snapshots under %s — aborting.", PREFIX)
            return
        logger.info("Found %d SE snapshot(s) — parsing in parallel…", len(snapshots))

        rdd = spark.sparkContext.parallelize(
            snapshots, numSlices=min(8, len(snapshots))
        )
        results = rdd.flatMap(fetch_and_parse_snapshot).collect()

        errors = [r["_error"] for r in results if "_error" in r]
        for e in errors:
            logger.warning("  ✗ %s", e)
        questions = [{k: v for k, v in r.items() if k != "_kind"}
                     for r in results if r.get("_kind") == "question"]
        answers   = [{k: v for k, v in r.items() if k != "_kind"}
                     for r in results if r.get("_kind") == "answer"]
        logger.info("Parsed %d questions, %d answers (pre-dedupe).",
                    len(questions), len(answers))

        if not questions or not answers:
            logger.error("Nothing to write — aborting.")
            return

        # Drop questions/answers without a primary key (defensive).
        questions = [q for q in questions if q.get("question_id") is not None]
        answers   = [a for a in answers   if a.get("answer_id")   is not None]

        from pyspark.sql.window import Window
        from pyspark.sql import functions as F

        df_q = spark.createDataFrame(questions, schema=QUESTION_SCHEMA)
        df_a = spark.createDataFrame(answers,   schema=ANSWER_SCHEMA)

        wq = Window.partitionBy("question_id").orderBy(F.col("ingested_at").asc_nulls_last())
        wa = Window.partitionBy("answer_id").orderBy(F.col("ingested_at").asc_nulls_last())

        df_q = (df_q.withColumn("_rn", F.row_number().over(wq))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn"))
        df_a = (df_a.withColumn("_rn", F.row_number().over(wa))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn"))

        pdf_q = df_q.toPandas()  # noqa: F841
        pdf_a = df_a.toPandas()  # noqa: F841

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {QUESTIONS_TABLE}")
            con.execute(f"""
                CREATE TABLE {QUESTIONS_TABLE} AS
                SELECT
                    question_id,
                    title,
                    body_text,
                    body_html,
                    score,
                    view_count,
                    answer_count,
                    accepted_answer_id,
                    tags,
                    creation_date,
                    last_activity_date,
                    link,
                    owner_user_id,
                    owner_display_name,
                    CAST(snapshot_date AS DATE) AS snapshot_date,
                    ingested_at
                FROM pdf_q
            """)
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{QUESTIONS_TABLE}_id "
                f"ON {QUESTIONS_TABLE}(question_id)"
            )

            con.execute(f"DROP TABLE IF EXISTS {ANSWERS_TABLE}")
            con.execute(f"CREATE TABLE {ANSWERS_TABLE} AS SELECT * FROM pdf_a")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{ANSWERS_TABLE}_id "
                f"ON {ANSWERS_TABLE}(answer_id)"
            )
            con.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{ANSWERS_TABLE}_qid "
                f"ON {ANSWERS_TABLE}(question_id)"
            )

            n_q = con.execute(f"SELECT count(*) FROM {QUESTIONS_TABLE}").fetchone()[0]
            n_a = con.execute(f"SELECT count(*) FROM {ANSWERS_TABLE}").fetchone()[0]
            n_q_with_acc = con.execute(
                f"SELECT count(*) FROM {QUESTIONS_TABLE} WHERE accepted_answer_id IS NOT NULL"
            ).fetchone()[0]
            n_a_acc = con.execute(
                f"SELECT count(*) FROM {ANSWERS_TABLE} WHERE is_accepted"
            ).fetchone()[0]
            avg_per_q = con.execute(f"""
                SELECT AVG(c) FROM (
                    SELECT count(*) AS c FROM {ANSWERS_TABLE} GROUP BY question_id
                )
            """).fetchone()[0]

            logger.info("Wrote %d questions and %d answers to %s",
                        n_q, n_a, DUCKDB_PATH)
            logger.info("  questions with accepted answer: %d/%d", n_q_with_acc, n_q)
            logger.info("  accepted answers in answer table: %d", n_a_acc)
            logger.info("  avg answers per kept question: %.2f", avg_per_q or 0)

            # Top tags as a sanity check.
            logger.info("  top tags:")
            for tag, n in con.execute(f"""
                SELECT t, count(*) AS n
                FROM (SELECT unnest(tags) AS t FROM {QUESTIONS_TABLE})
                GROUP BY t ORDER BY n DESC LIMIT 8
            """).fetchall():
                logger.info("      %-30s %d", tag, n)

            # Make sure HTML stripping happened (no <p> in body_text).
            leftover_html = con.execute(f"""
                SELECT count(*) FROM {QUESTIONS_TABLE}
                WHERE body_text LIKE '%<p>%' OR body_text LIKE '%</p>%'
            """).fetchone()[0]
            if leftover_html:
                logger.warning("  ⚠ %d questions still have <p> tags in body_text", leftover_html)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
