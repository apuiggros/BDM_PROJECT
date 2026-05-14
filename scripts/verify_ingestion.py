"""
verify_ingestion.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Sanity-checks the Landing Zone after the Wikiquote and Stack Exchange
ingestion fixes. For each source it loads the freshly written objects from
MinIO and verifies that the expected structure is present so the Trusted
Zone has something real to parse.

Run:
    python scripts/verify_ingestion.py
"""

from __future__ import annotations

import io
import json
import os
import sys
from collections import Counter
from pathlib import Path

import boto3
from botocore.client import Config
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Allow running from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingestion"))
from character_registry import TARGET_FIGURES  # type: ignore  # noqa: E402

load_dotenv()

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def fetch_json(client, key: str):
    obj = client.get_object(Bucket=MINIO_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


# ─── Wikiquote ────────────────────────────────────────────────────────────────

# Header text fragments treated as "by_figure" sections (own quotes).
BY_FIGURE_HEADINGS = ("quotation", "sourced", "quotes")

# Header text fragments treated as "about_figure" sections.
ABOUT_HEADING = "quotes about"

# Sections we explicitly want to skip when surveying.
SKIP_HEADINGS = ("misattributed", "see also", "external links",
                 "further reading", "references", "bibliography",
                 "in popular culture")


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


def survey_wikiquote_html(html: str) -> dict:
    """
    Walk the rendered Wikiquote HTML and return per-section counts of
    top-level <li> elements (which is roughly the per-quote count).
    """
    soup = BeautifulSoup(html, "lxml")
    # MediaWiki wraps content in <div class="mw-parser-output">; if absent,
    # the body root is fine.
    root = soup.find("div", class_="mw-parser-output") or soup

    sections = {"by_figure": 0, "about_figure": 0, "other": 0, "skip": 0}
    section_breakdown: list[tuple[str, str, int]] = []  # (kind, heading, count)
    sample_quotes: list[str] = []

    current_kind = "other"
    current_heading = "(intro)"
    current_li_count = 0

    def flush():
        nonlocal current_li_count
        if current_li_count:
            sections[current_kind] = sections.get(current_kind, 0) + current_li_count
            section_breakdown.append((current_kind, current_heading, current_li_count))
        current_li_count = 0

    for el in root.children:
        # Section break: modern MediaWiki wraps each <h2> in a
        # <div class="mw-heading mw-heading2">; older output puts the h2
        # directly under root. Handle both.
        h2 = None
        if getattr(el, "name", None) == "div":
            classes = el.get("class", []) or []
            if "mw-heading2" in classes:
                h2 = el.find("h2")
        elif getattr(el, "name", None) == "h2":
            h2 = el

        if h2 is not None:
            flush()
            # The h2 text contains "Heading [ edit ]" because of the edit
            # link span; the bare title lives in the h2's own text.
            heading_text = h2.get_text(" ", strip=True)
            current_kind = classify_section(heading_text)
            current_heading = heading_text
            continue

        # Within a section, count <li> elements at the *top level* of
        # any <ul> we encounter (each top-level li is one quote; nested
        # uls are citations and shouldn't be counted again).
        if getattr(el, "name", None) == "ul":
            top_level_lis = el.find_all("li", recursive=False)
            current_li_count += len(top_level_lis)
            if current_kind == "by_figure" and len(sample_quotes) < 2:
                for li in top_level_lis[:2]:
                    # Quote text = li's own text, excluding nested <ul>
                    li_clone = BeautifulSoup(str(li), "lxml").li
                    for sub in li_clone.find_all("ul"):
                        sub.decompose()
                    text = li_clone.get_text(" ", strip=True)
                    if text:
                        sample_quotes.append(text[:140])

        # h3 inside a "by_figure" section names the source work; we don't
        # need to do anything special — the <ul>s following will continue
        # to flow into current_li_count.

    flush()
    return {
        "section_counts": sections,
        "section_breakdown": section_breakdown,
        "sample_quotes": sample_quotes,
    }


def verify_wikiquote(client) -> bool:
    print("=" * 78)
    print("WIKIQUOTE")
    print("=" * 78)
    ok = True
    totals = Counter()

    for figure in TARGET_FIGURES:
        slug = figure["gutenberg_author_slug"]
        domain = figure["domain"]
        key = f"wikiquote/raw_json/{domain}/{slug}_wikiquote.json"

        try:
            data = fetch_json(client, key)
        except Exception as e:
            print(f"  ✗ [{slug}] could not load: {e}")
            ok = False
            continue

        # Structure check.
        html = data.get("parse", {}).get("text", {}).get("*")
        if not html:
            print(f"  ✗ [{slug}] missing parse.text.* (got keys: {list(data.keys())})")
            ok = False
            continue

        sections = data.get("parse", {}).get("sections", [])
        survey = survey_wikiquote_html(html)
        sc = survey["section_counts"]
        totals["by_figure"] += sc["by_figure"]
        totals["about_figure"] += sc["about_figure"]

        status = "✓" if sc["by_figure"] > 0 else "⚠"
        print(f"  {status} [{slug:>10}] html={len(html):>7,}b  "
              f"sections(api)={len(sections):>3}  "
              f"by_figure={sc['by_figure']:>4}  "
              f"about_figure={sc['about_figure']:>4}  "
              f"other={sc['other']:>3}  skip={sc['skip']:>3}")

        if sc["by_figure"] == 0:
            ok = False
            print(f"      ⚠ no quotes parsed under by_figure sections — section breakdown:")
            for kind, heading, n in survey["section_breakdown"][:6]:
                print(f"          [{kind}] {heading[:60]} -> {n} li")

    print(f"\n  Totals across 9 figures: by_figure={totals['by_figure']}  "
          f"about_figure={totals['about_figure']}")

    # Show a sample quote from one figure to confirm we extract real text.
    print("\n  Sample quote (Descartes):")
    try:
        data = fetch_json(client, "wikiquote/raw_json/philosophy/descartes_wikiquote.json")
        survey = survey_wikiquote_html(data["parse"]["text"]["*"])
        for q in survey["sample_quotes"][:2]:
            print(f"    • {q}")
    except Exception as e:
        print(f"    (could not sample: {e})")

    return ok


# ─── Stack Exchange ───────────────────────────────────────────────────────────

def verify_stack_exchange(client) -> bool:
    print()
    print("=" * 78)
    print("STACK EXCHANGE")
    print("=" * 78)
    ok = True

    # Find the latest snapshot file.
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix="philosophy_se/raw_json/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    if not keys:
        print("  ✗ no SE snapshots found")
        return False
    keys.sort()
    latest = keys[-1]
    print(f"  Snapshot: {latest}")

    data = fetch_json(client, latest)
    if not isinstance(data, list):
        print(f"  ✗ expected list, got {type(data).__name__}")
        return False

    n = len(data)
    with_answers_array = sum(1 for q in data if isinstance(q.get("answers"), list) and q["answers"])
    with_accepted_id   = sum(1 for q in data if q.get("accepted_answer_id"))
    with_question_body = sum(1 for q in data if q.get("body"))

    # Of the ones with accepted_answer_id, do we have the matching answer body?
    accepted_with_body = 0
    accepted_body_lengths: list[int] = []
    for q in data:
        aid = q.get("accepted_answer_id")
        if not aid:
            continue
        for a in q.get("answers", []):
            if a.get("answer_id") == aid:
                if a.get("body"):
                    accepted_with_body += 1
                    accepted_body_lengths.append(len(a["body"]))
                break

    print(f"  Total questions:                 {n}")
    print(f"  Questions with body:             {with_question_body}/{n}")
    print(f"  Questions with answers array:    {with_answers_array}/{n}")
    print(f"  Questions with accepted_answer:  {with_accepted_id}/{n}")
    print(f"  Accepted answers with body text: {accepted_with_body}/{with_accepted_id}")
    if accepted_body_lengths:
        avg = sum(accepted_body_lengths) // len(accepted_body_lengths)
        print(f"  Accepted answer body length:     "
              f"min={min(accepted_body_lengths)}  avg={avg}  max={max(accepted_body_lengths)}")

    if accepted_with_body != with_accepted_id:
        print("  ✗ Some accepted answers are missing body text")
        ok = False
    if with_accepted_id == 0:
        print("  ✗ Zero questions have an accepted answer")
        ok = False

    # Sample one Q+A pair so we can eyeball the content.
    sample = next((q for q in data if q.get("accepted_answer_id")), None)
    if sample:
        aid = sample["accepted_answer_id"]
        accepted = next((a for a in sample.get("answers", []) if a.get("answer_id") == aid), None)
        print("\n  Sample accepted Q+A pair:")
        print(f"    Q (id={sample['question_id']}, score={sample['score']}): "
              f"{sample['title'][:90]}")
        if accepted:
            body_text = BeautifulSoup(accepted.get("body", ""), "lxml").get_text(" ", strip=True)
            print(f"    A (id={accepted['answer_id']}, score={accepted['score']}): "
                  f"{body_text[:200]}...")

    return ok


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    client = get_client()
    wikiquote_ok = verify_wikiquote(client)
    se_ok = verify_stack_exchange(client)

    print()
    print("=" * 78)
    if wikiquote_ok and se_ok:
        print("RESULT: ✓ all checks passed")
        return 0
    print("RESULT: ✗ one or more checks failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
