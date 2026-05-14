"""
verify_trusted_zone.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sanity-checks the Trusted Zone after a full pipeline run.

Verifies:
- Each expected table exists in `duckdb/trusted.duckdb`.
- Row counts are within the expected order of magnitude (a regression
  early-warning, not a strict equality).
- Key cross-source joins work (figure_slug across tables).
- The Gutenberg cleaned-text bytes referenced by `cleaned_text_key`
  actually exist in the MinIO landing zone.

Run:
    python scripts/verify_trusted_zone.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
import duckdb
from botocore.client import Config
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

DUCKDB_PATH = REPO_ROOT / "duckdb" / "trusted.duckdb"

# (table, lower bound on row count)
EXPECTED_TABLES: dict[str, int] = {
    "trusted_philosophers":          5,
    "trusted_wikipedia":             9,
    "trusted_wikiquote_quotes":   1500,
    "trusted_news_articles":       100,
    "trusted_se_questions":        400,
    "trusted_se_answers":         3500,
    "trusted_gutenberg_books":     150,
    "trusted_podcast_episodes":     50,
    "trusted_philosopher_images":  100,
}


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def main() -> int:
    if not DUCKDB_PATH.exists():
        print(f"✗ DuckDB file not found at {DUCKDB_PATH}")
        return 1

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    ok = True

    print("=" * 70)
    print("TRUSTED ZONE — table presence & row counts")
    print("=" * 70)
    actual_tables = {
        r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    }
    for table, expected_min in EXPECTED_TABLES.items():
        if table not in actual_tables:
            print(f"  ✗ {table:35s} MISSING")
            ok = False
            continue
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        marker = "✓" if n >= expected_min else "⚠"
        if n < expected_min:
            ok = False
        print(f"  {marker} {table:35s} {n:>6,} rows  (expected ≥ {expected_min})")

    print()
    print("=" * 70)
    print("CROSS-SOURCE JOINS")
    print("=" * 70)
    rows = con.execute("""
        SELECT p.figure_slug,
               (w.summary_text IS NOT NULL)                AS has_wiki,
               (SELECT count(*) FROM trusted_wikiquote_quotes q
                  WHERE q.figure_slug = p.figure_slug
                    AND q.quote_type = 'by_figure')        AS by_quotes,
               (SELECT count(*) FROM trusted_gutenberg_books b
                  WHERE b.figure_slug = p.figure_slug
                    AND b.cleaned_text_key IS NOT NULL)    AS clean_books,
               (SELECT count(*) FROM trusted_philosopher_images i
                  WHERE i.figure_slug = p.figure_slug
                    AND i.is_primary) > 0                  AS has_primary_image
        FROM trusted_philosophers p
        LEFT JOIN trusted_wikipedia w USING (figure_slug)
        ORDER BY p.figure_slug
    """).fetchall()
    print(f"  {'figure':<12s} wiki  by_quotes  clean_books  primary_img")
    for slug, has_wiki, by_q, n_books, has_img in rows:
        if not has_wiki or not by_q or not n_books or not has_img:
            ok = False
        print(f"  {slug:<12s}   {'Y' if has_wiki else 'N'}     {by_q:>6}        {n_books:>4}          {'Y' if has_img else 'N'}")

    # Ensure the non-philosophy figures are present in Wikipedia / Wikiquote / Gutenberg.
    others = con.execute("""
        SELECT figure_slug, count(*) FROM trusted_gutenberg_books
        WHERE figure_slug NOT IN (SELECT figure_slug FROM trusted_philosophers)
        GROUP BY figure_slug ORDER BY 1
    """).fetchall()
    print()
    print("  non-philosophy figures (catalog rows):")
    for slug, n in others:
        print(f"      {slug:<12s} {n}")

    print()
    print("=" * 70)
    print("MINIO — cleaned-text byte presence")
    print("=" * 70)
    keys = [r[0] for r in con.execute(
        "SELECT cleaned_text_key FROM trusted_gutenberg_books "
        "WHERE cleaned_text_key IS NOT NULL"
    ).fetchall()]
    print(f"  {len(keys)} cleaned_text_keys referenced; spot-checking 5…")
    client = get_minio_client()
    bucket = os.getenv("MINIO_BUCKET", "landing-zone")
    sample_keys = keys[:: max(1, len(keys) // 5)][:5]
    for k in sample_keys:
        try:
            head = client.head_object(Bucket=bucket, Key=k)
            size = head["ContentLength"]
            print(f"    ✓ {k}  ({size:,} bytes)")
        except Exception as e:
            print(f"    ✗ {k}: {e}")
            ok = False

    print()
    print("=" * 70)
    if ok:
        print("RESULT: ✓ all checks passed")
        return 0
    print("RESULT: ✗ one or more checks failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
