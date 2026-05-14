"""
cleaning/structured/wikiquote.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Trusted Zone — Wikiquote cleaning job

Reads the rendered Wikiquote HTML for every figure from the Landing Zone
(MinIO bucket `landing-zone`, prefix `wikiquote/raw_json/<domain>/`),
parses individual quote rows out of the page sections, and writes them to
the `trusted_wikiquote_quotes` table in `duckdb/trusted.duckdb`.

Schema
------
    quote_id       VARCHAR PRIMARY KEY
    figure_slug    VARCHAR
    quote_text     VARCHAR
    quote_type     VARCHAR  ('by_figure' | 'about_figure')
    source_work    VARCHAR  (h3 subsection label, NULL for about_figure)
    citation_text  VARCHAR  (joined nested-ul citations, may be NULL)
    position       INTEGER  (order within source page; basis for stable id)
    source_url     VARCHAR

Sections classified as `by_figure`: anything starting with "Quotation",
"Sourced", or containing "Quotes" but not "Quotes about". Sections starting
with "Quotes about" become `about_figure`. Misattributed / See also /
External links / Further reading / References / Bibliography are skipped.

Run
---
    python cleaning/structured/wikiquote.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote as urlquote

import boto3
import duckdb
from botocore.client import Config
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, md5, substring
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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
logger = logging.getLogger("trusted_wikiquote")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
LANDING_BUCKET   = os.getenv("MINIO_BUCKET", "landing-zone")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"
TABLE_NAME  = "trusted_wikiquote_quotes"

# Make sure Spark workers use the same Python interpreter (and therefore
# the same venv site-packages) as the driver. Without this, Spark may
# spawn workers under the system Python, which won't see bs4 / lxml /
# duckdb / boto3.
os.environ.setdefault("PYSPARK_PYTHON",        sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Header text fragments used to classify each <h2> on the page.
BY_FIGURE_HEADINGS = ("quotation", "sourced", "quotes")
ABOUT_HEADING      = "quotes about"
SKIP_HEADINGS      = (
    "misattributed", "see also", "external links",
    "further reading", "references", "bibliography",
    "in popular culture",
)


def classify_section(heading_text: str) -> str:
    """Return 'by_figure', 'about_figure', 'skip', or 'other'."""
    t = heading_text.strip().lower()
    if t.startswith(ABOUT_HEADING):
        return "about_figure"
    if any(t.startswith(s) for s in SKIP_HEADINGS):
        return "skip"
    if any(t.startswith(s) or s in t for s in BY_FIGURE_HEADINGS):
        return "by_figure"
    return "other"


# ─── HTML walker ──────────────────────────────────────────────────────────────
def extract_quotes_from_html(
    html: str,
    figure_slug: str,
    source_url: str,
) -> list[dict[str, Any]]:
    """
    Walk a rendered Wikiquote page and return one dict per quote.

    The walker visits the direct children of `mw-parser-output`. Section
    breaks are detected via either the modern wrapper
    `<div class="mw-heading mw-heading2"><h2>…</h2></div>` or the legacy
    bare `<h2>`. The current `<h3>` wrapper is tracked as `source_work`
    inside `by_figure` sections only.

    Each top-level `<li>` of any `<ul>` inside an active section becomes
    a quote row. Nested `<ul>` items (citations) are stripped from the
    quote text and joined into `citation_text`.
    """
    soup = BeautifulSoup(html, "lxml")
    root = soup.find("div", class_="mw-parser-output") or soup

    rows: list[dict[str, Any]] = []
    current_kind: str | None = None
    current_source_work: str | None = None
    position = 0

    for el in root.children:
        if not getattr(el, "name", None):
            continue

        # ── Section break: <h2> wrapper or bare <h2> ──
        h2 = None
        classes = el.get("class", []) or []
        if el.name == "div" and "mw-heading2" in classes:
            h2 = el.find("h2")
        elif el.name == "h2":
            h2 = el

        if h2 is not None:
            heading_text = h2.get_text(" ", strip=True)
            kind = classify_section(heading_text)
            current_kind = kind if kind in ("by_figure", "about_figure") else None
            current_source_work = None
            continue

        # ── Subsection: <h3> wrapper or bare <h3> (only meaningful if active) ──
        h3 = None
        if el.name == "div" and "mw-heading3" in classes:
            h3 = el.find("h3")
        elif el.name == "h3":
            h3 = el

        if h3 is not None:
            current_source_work = h3.get_text(" ", strip=True) or None
            continue

        # Skip everything outside a section we care about.
        if current_kind is None:
            continue

        # ── Quote list ──
        if el.name == "ul":
            for li in el.find_all("li", recursive=False):
                row = _li_to_quote_row(
                    li,
                    figure_slug=figure_slug,
                    quote_type=current_kind,
                    source_work=current_source_work if current_kind == "by_figure" else None,
                    position=position,
                    source_url=source_url,
                )
                if row is not None:
                    rows.append(row)
                    position += 1

    return rows


def _li_to_quote_row(
    li,
    *,
    figure_slug: str,
    quote_type: str,
    source_work: str | None,
    position: int,
    source_url: str,
) -> dict[str, Any] | None:
    """Convert a single <li> into a quote row, separating quote text from citations."""
    li_clone = BeautifulSoup(str(li), "lxml").li
    citations: list[str] = []
    for sub_ul in li_clone.find_all("ul"):
        for sub_li in sub_ul.find_all("li", recursive=False):
            cite = sub_li.get_text(" ", strip=True)
            if cite:
                citations.append(cite)
        sub_ul.decompose()

    quote_text = li_clone.get_text(" ", strip=True)
    if not quote_text:
        return None

    return {
        "figure_slug": figure_slug,
        "quote_text": quote_text,
        "quote_type": quote_type,
        "source_work": source_work,
        "citation_text": " | ".join(citations) if citations else None,
        "position": position,
        "source_url": source_url,
    }


# ─── Worker function (driver-side or Spark-side) ──────────────────────────────
def fetch_and_parse_one(figure: dict) -> list[dict[str, Any]]:
    """
    Read one figure's Wikiquote JSON from MinIO and return its quote rows.

    Used as a Spark RDD `flatMap` function — must build its own boto3
    client because clients aren't picklable across partitions.
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

    slug = figure["gutenberg_author_slug"]
    domain = figure["domain"]
    title = figure.get("wikidata_label", figure["api_name"]).replace(" ", "_")
    key = f"wikiquote/raw_json/{domain}/{slug}_wikiquote.json"
    source_url = f"https://en.wikiquote.org/wiki/{urlquote(title)}"

    try:
        resp = client.get_object(Bucket=bucket, Key=key)
        data = json.loads(resp["Body"].read())
    except Exception as e:
        # Surface as empty list; the driver will log per-figure totals.
        return [{"_error": f"{slug}: {e!s}"}]

    html = data.get("parse", {}).get("text", {}).get("*")
    if not html:
        return [{"_error": f"{slug}: missing parse.text.*"}]

    return extract_quotes_from_html(html, slug, source_url)


# ─── Spark / DuckDB write ─────────────────────────────────────────────────────
QUOTE_SCHEMA = StructType([
    StructField("figure_slug",   StringType(),  False),
    StructField("quote_text",    StringType(),  False),
    StructField("quote_type",    StringType(),  False),
    StructField("source_work",   StringType(),  True),
    StructField("citation_text", StringType(),  True),
    StructField("position",      IntegerType(), False),
    StructField("source_url",    StringType(),  False),
])


def run() -> None:
    spark = (
        SparkSession.builder
        .appName("trusted_wikiquote")
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

        # Split error rows from real rows (workers signal errors via {"_error": ...}).
        errors = [r["_error"] for r in results if "_error" in r]
        rows = [r for r in results if "_error" not in r]
        for e in errors:
            logger.warning("  ✗ %s", e)
        if not rows:
            logger.error("No quotes parsed — aborting before write.")
            return

        df = spark.createDataFrame(rows, schema=QUOTE_SCHEMA)
        df = df.withColumn(
            "quote_id",
            md5(concat_ws("||", col("figure_slug"), col("position").cast("string"),
                          substring(col("quote_text"), 1, 60))),
        ).select(
            "quote_id", "figure_slug", "quote_text", "quote_type",
            "source_work", "citation_text", "position", "source_url",
        )

        pdf = df.toPandas()  # noqa: F841 — used by DuckDB via the Python scope

        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(DUCKDB_PATH))
        try:
            con.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            con.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM pdf")
            con.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE_NAME}_id "
                f"ON {TABLE_NAME}(quote_id)"
            )

            # ─── Post-write summary ───
            total = con.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
            by_kind = con.execute(
                f"SELECT quote_type, count(*) FROM {TABLE_NAME} GROUP BY 1 ORDER BY 1"
            ).fetchall()
            per_figure = con.execute(f"""
                SELECT figure_slug,
                       count(*) FILTER (WHERE quote_type = 'by_figure')   AS by_figure,
                       count(*) FILTER (WHERE quote_type = 'about_figure') AS about_figure
                FROM {TABLE_NAME}
                GROUP BY 1 ORDER BY 1
            """).fetchall()

            logger.info("Wrote %d rows to %s in %s", total, TABLE_NAME, DUCKDB_PATH)
            logger.info("By type: %s", dict(by_kind))
            logger.info("Per figure (slug, by_figure, about_figure):")
            for slug, n_by, n_about in per_figure:
                logger.info("    %-12s by_figure=%-4d  about_figure=%-4d", slug, n_by, n_about)
        finally:
            con.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
