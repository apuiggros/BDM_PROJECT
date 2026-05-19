"""
scripts/verify_corpus_chunks.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Milvus `corpus_chunks` health check — the green-light gate at the end of the
Exploitation Zone DAG, mirroring scripts/verify_exploit_zone.py for DuckDB.

Validates:
- The collection exists and is loaded.
- Entity count is above a loose lower bound (real regression, not noise).
- Every expected `source` is represented.
- figure_slug values are all known dim_figure slugs (or '' for non-figure).
- A sample similarity search actually returns hits.

Exits 0 on success, 1 on any failure.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
COLLECTION  = "corpus_chunks"
EMBED_DIM   = 384
MIN_ENTITIES = 2_000   # books alone produce far more; loose on purpose.
EXPECTED_SOURCES = {"gutenberg", "wikipedia", "wikiquote", "stackexchange"}

ROOT       = Path(__file__).resolve().parents[1]
DUCKDB_DIR = Path(os.getenv("DUCKDB_DIR", ROOT / "duckdb"))
EXPLOIT_DB = DUCKDB_DIR / "exploit.duckdb"

FAIL: list[str] = []
OK:   list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    (OK if ok else FAIL).append(f"{name}: {detail}" if detail else name)


def main() -> int:
    from pymilvus import Collection, connections, utility

    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT)
    except Exception as e:  # noqa: BLE001
        print(f"FATAL: cannot reach Milvus at {MILVUS_HOST}:{MILVUS_PORT} — {e}",
              file=sys.stderr)
        return 1

    if not utility.has_collection(COLLECTION):
        print(f"FATAL: collection {COLLECTION} does not exist", file=sys.stderr)
        return 1

    col = Collection(COLLECTION)
    col.load()
    n = col.num_entities
    check("entity_count", n >= MIN_ENTITIES, f"{n} (min {MIN_ENTITIES})")

    # Sources present. NOTE: a `col.query` is hard-capped at 16,384 rows by
    # Milvus and returns insertion-order rows, so sampling "pk >= 0" only ever
    # sees the first-inserted source (books). Instead do a targeted existence
    # query per expected source — exact regardless of collection size.
    present, absent = [], []
    for s in sorted(EXPECTED_SOURCES):
        hit = col.query(expr=f'source == "{s}"',
                        output_fields=["pk"], limit=1)
        (present if hit else absent).append(s)
    check("sources_present", not absent,
          f"present {present}; missing {absent}")

    # figure_slug values must be known dim_figure slugs (or '' for non-figure).
    # Same capping problem — so ask Milvus directly for any row whose slug is
    # outside the valid set (and non-empty). Empty result = all valid.
    if EXPLOIT_DB.exists():
        dcon = duckdb.connect(str(EXPLOIT_DB), read_only=True)
        valid = sorted(r[0] for r in
                       dcon.execute("SELECT figure_slug FROM dim_figure").fetchall())
        dcon.close()
        valid_list = ", ".join(f'"{v}"' for v in valid)
        bad = col.query(
            expr=f'figure_slug != "" and figure_slug not in [{valid_list}]',
            output_fields=["figure_slug"], limit=5)
        check("figure_slug_valid", not bad,
              f"unknown slugs sample: {[r['figure_slug'] for r in bad]}")
    else:
        check("figure_slug_valid", False, "exploit.duckdb missing for slug check")

    # A real search returns hits (index + load actually worked).
    try:
        probe = [0.0] * EMBED_DIM
        probe[0] = 1.0
        hits = col.search(
            [probe], "embedding",
            {"metric_type": "COSINE", "params": {"ef": 64}},
            limit=5, output_fields=["source", "figure_slug"],
        )
        check("search_returns_hits", len(hits[0]) > 0,
              f"{len(hits[0])} hits")
    except Exception as e:  # noqa: BLE001
        check("search_returns_hits", False, str(e))

    for line in OK:   print("OK  ", line)
    for line in FAIL: print("FAIL", line)
    print(f"\n{len(OK)} checks passed, {len(FAIL)} failed.")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
