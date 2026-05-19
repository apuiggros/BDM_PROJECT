"""
scripts/build_p2_report.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Generates documents/P2_TECHNICAL_REPORT.pdf — a deep, visual walkthrough of
the P2 application and its pipeline logic (P2-focused; Trusted Zone recapped;
streaming presented as a planned integration).

Diagrams are drawn with matplotlib (vector → 200 DPI PNG); the document is
assembled with reportlab Platypus. Follows the installed `pdf` skill's
recommended reportlab path.

    python scripts/build_p2_report.py
"""
from __future__ import annotations

import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "documents" / "_report_assets"
ASSETS.mkdir(parents=True, exist_ok=True)
OUT = ROOT / "documents" / "P2_TECHNICAL_REPORT.pdf"

# ─── palette ──────────────────────────────────────────────────────────────────
C_LAND = "#9aa5b1"
C_TRUST = "#4c78a8"
C_EXPL = "#54a24b"
C_CONS = "#e45756"
C_STORE = "#7d5ba6"
C_PLAN = "#bfa14a"
INK = "#1f2933"


def _box(ax, x, y, w, h, text, fc, tc="white", fs=9, style="round"):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"{style},pad=0.02,rounding_size=0.06",
        linewidth=1.1, edgecolor="white", facecolor=fc, zorder=2))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center",
            fontsize=fs, color=tc, weight="bold", zorder=3, wrap=True)


def _arrow(ax, p1, p2, color=INK, style="-|>", lw=1.6, rad=0.0):
    ax.add_patch(FancyArrowPatch(
        p1, p2, arrowstyle=style, mutation_scale=14, lw=lw,
        color=color, connectionstyle=f"arc3,rad={rad}", zorder=1))


def _canvas(w=12, h=6):
    fig, ax = plt.subplots(figsize=(w, h))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")
    return fig, ax


def _save(fig, name):
    p = ASSETS / name
    fig.savefig(p, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    return str(p)


# ─── Diagram 1 — end-to-end pipeline ─────────────────────────────────────────
def diagram_pipeline():
    fig, ax = _canvas(12, 6.4)
    ax.text(50, 97, "Historical Conversational AI — End-to-End Pipeline",
            ha="center", fontsize=13, weight="bold", color=INK)

    _box(ax, 3, 70, 18, 16,
         "LANDING\n9 raw sources\n(MinIO buckets)", C_LAND, fs=9)
    _box(ax, 28, 70, 19, 16,
         "TRUSTED ZONE\nSpark cleaning\n→ 9 conformed sets", C_TRUST, fs=9)
    _box(ax, 54, 70, 19, 16,
         "EXPLOITATION\nStar schema +\nvector corpus", C_EXPL, fs=9)
    _box(ax, 80, 70, 17, 16,
         "CONSUMPTION\nConversational\npodcast", C_CONS, fs=9)
    _arrow(ax, (21, 78), (28, 78))
    _arrow(ax, (47, 78), (54, 78))
    _arrow(ax, (73, 78), (80, 78))

    # storage layer
    ax.text(50, 56, "Three-store lakehouse (MongoDB deliberately dropped)",
            ha="center", fontsize=10, weight="bold", color=C_STORE)
    _box(ax, 8, 36, 24, 14,
         "MinIO\nraw bytes +\ncleaned book text", C_STORE, fs=9)
    _box(ax, 38, 36, 24, 14,
         "DuckDB\ntrusted.duckdb +\nexploit.duckdb (tabular)", C_STORE, fs=9)
    _box(ax, 68, 36, 24, 14,
         "Milvus\ncorpus_chunks\n(384-d vectors)", C_STORE, fs=9)
    for sx in (20, 50, 80):
        _arrow(ax, (sx, 70), (sx, 50), color=C_STORE, style="-", lw=1.2)

    # planned streaming
    _box(ax, 28, 12, 44, 13,
         "PLANNED — Kafka → Spark Structured Streaming → fact_mentions_1m\n"
         "(Santi's branch; integrates as a real-time fact table)",
         C_PLAN, fs=8.5)
    _arrow(ax, (50, 36), (50, 25), color=C_PLAN, style="-|>", lw=1.3, rad=0)
    ax.text(52, 30, "real-time fact joins dim_figure",
            fontsize=7.5, color=C_PLAN, style="italic")

    legend = [mpatches.Patch(color=c, label=l) for c, l in [
        (C_LAND, "Landing"), (C_TRUST, "Trusted"), (C_EXPL, "Exploitation"),
        (C_CONS, "Consumption"), (C_STORE, "Storage"), (C_PLAN, "Planned")]]
    ax.legend(handles=legend, loc="lower center", ncol=6, frameon=False,
              fontsize=7.5, bbox_to_anchor=(0.5, -0.04))
    return _save(fig, "d1_pipeline.png")


# ─── Diagram 2 — exploitation star schema ────────────────────────────────────
def diagram_star():
    fig, ax = _canvas(12, 6.6)
    ax.text(50, 97, "Exploitation Zone — Star Schema + Vector Corpus",
            ha="center", fontsize=13, weight="bold", color=INK)

    _box(ax, 38, 44, 24, 16,
         "dim_figure\nPK figure_slug\n9 figures · 3 domains", C_EXPL, fs=9.5)

    facts = [
        (6, 74, "fact_works\n166 rows\nGutenberg books"),
        (38, 78, "fact_quotes\n1,996 rows\nWikiquote by/about"),
        (70, 74, "fact_se_qa\n500 rows\nQ+A, mentioned_figures[]"),
        (6, 14, "fact_news_articles\n180 rows · NO FK\n(figure link at LLM time)"),
        (70, 14, "corpus_chunks (Milvus)\n~87,437 vectors\nbooks/wiki/quote/SE"),
    ]
    for x, y, t in facts:
        _box(ax, x, y, 24, 14, t, C_TRUST, fs=8.5)
    # FK edges
    _arrow(ax, (18, 74), (44, 60), color=INK, style="-", lw=1.2)
    _arrow(ax, (50, 78), (50, 60), color=INK, style="-", lw=1.2)
    _arrow(ax, (82, 74), (56, 60), color=INK, style="-", lw=1.2)
    _arrow(ax, (18, 28), (44, 46), color=INK, style="-", lw=1.2)
    _arrow(ax, (82, 28), (56, 46), color=C_STORE, style="-", lw=1.2)
    ax.text(30, 36, "figure_slug FK", fontsize=7.5, color=INK, style="italic")
    ax.text(60, 33, "figure_slug filter\n(metadata, not FK)",
            fontsize=7, color=C_STORE, style="italic")
    ax.text(50, 6,
            "fact_news_articles carries no figure_slug by design — GNews is "
            "category-broad; the Interviewer reasons figure↔news at prompt time.",
            ha="center", fontsize=7.5, color="#52606d", style="italic")
    return _save(fig, "d2_star.png")


# ─── Diagram 3 — consumption agent flow ──────────────────────────────────────
def diagram_agents():
    fig, ax = _canvas(12, 6.6)
    ax.text(50, 97, "Consumption Zone — Conversational Podcast Agents",
            ha="center", fontsize=13, weight="bold", color=INK)

    _box(ax, 36, 80, 28, 12,
         "INTERVIEWER (host)\ntopic curator, not news-reader", C_CONS, fs=9)
    _box(ax, 4, 50, 26, 16,
         "REASONER\nidentity card (DuckDB)\n+ Milvus RAG\n→ grounded", C_EXPL,
         fs=8.5)
    _box(ax, 70, 50, 26, 16,
         "VOICE\nblended persona\nmaxims + prose\n(Milvus filter)", C_TRUST,
         fs=8.5)
    _box(ax, 36, 50, 28, 14,
         "EPISODE COMPOSER\ncold open · turns · close", "#b07aa1", fs=8.5)
    _box(ax, 30, 20, 40, 13,
         "Claude (Anthropic) llm_fn — provider-swappable\n"
         "≈12 calls / 6-Q episode ≈ $0.30 (Sonnet)", INK, fs=8.5)
    _box(ax, 4, 78, 24, 13,
         "fact_news_articles\n(loose inspiration)", C_STORE, fs=8)
    _box(ax, 72, 78, 24, 13,
         "bio context\nera · lifespan ·\nvoice_descriptor", C_STORE, fs=8)

    _arrow(ax, (28, 84), (36, 85), color=C_STORE)
    _arrow(ax, (50, 80), (50, 64))
    _arrow(ax, (36, 57), (30, 57))                 # composer → reasoner
    _arrow(ax, (64, 57), (70, 57))                 # composer → voice
    _arrow(ax, (70, 58), (30, 58), color=C_TRUST, style="-", lw=1.1, rad=-.25)
    ax.text(50, 70, "style block", fontsize=7, color=C_TRUST,
            ha="center", style="italic")
    _arrow(ax, (84, 78), (84, 66), color=C_STORE)  # bio → voice/reasoner
    _arrow(ax, (50, 50), (50, 33))
    _arrow(ax, (17, 50), (40, 33), color=INK, style="-", lw=1.0)
    ax.text(50, 44, "grounded + voiced prompt → answer", fontsize=7.5,
            color=INK, ha="center", style="italic")
    ax.text(50, 12,
            "Curator design steers to evergreen intellectual themes — "
            "structurally defuses corpus-bias surfacing on charged news.",
            ha="center", fontsize=7.5, color="#52606d", style="italic")
    return _save(fig, "d3_agents.png")


# ─── Diagram 4 — streaming integration seam ──────────────────────────────────
def diagram_stream():
    fig, ax = _canvas(12, 4.4)
    ax.text(50, 95, "Planned Integration — Real-Time Mentions",
            ha="center", fontsize=13, weight="bold", color=INK)
    _box(ax, 2, 52, 17, 22, "Kafka\ncharacter-\nmentions", C_PLAN, fs=8.5)
    _box(ax, 24, 52, 21, 22,
         "Spark Structured\nStreaming\n1-min window\n30 s watermark", C_PLAN,
         fs=8.5)
    _box(ax, 50, 52, 19, 22,
         "Parquet\n/app/trusted/\nstreaming/\nfact_mentions_1m", C_PLAN, fs=8)
    _box(ax, 74, 52, 23, 22,
         "Exploitation\nview/batch →\nfact_mentions_1m\n⋈ dim_figure", C_EXPL,
         fs=8)
    _arrow(ax, (19, 63), (24, 63), color=C_PLAN)
    _arrow(ax, (45, 63), (50, 63), color=C_PLAN)
    _arrow(ax, (69, 63), (74, 63), color=INK)
    ax.text(50, 34, "OPEN SEAM DECISIONS (Santi sync)", ha="center",
            fontsize=9, weight="bold", color="#a4453a")
    ax.text(50, 22,
            "1.  character_name  ↔  figure_slug  naming convention\n"
            "2.  Parquet view (read_parquet) vs periodic batch load into "
            "exploit.duckdb\n"
            "3.  Branch merge strategy — origin/santi forked pre-Trusted; "
            "needs rebase, not fast-forward",
            ha="center", va="center", fontsize=8, color=INK,
            linespacing=1.5)
    return _save(fig, "d4_stream.png")


# ─── document assembly ───────────────────────────────────────────────────────
def styles():
    s = getSampleStyleSheet()
    s.add(ParagraphStyle("Cover", parent=s["Title"], fontSize=26,
                          textColor=colors.HexColor(INK), leading=30))
    s.add(ParagraphStyle("Sub", parent=s["Normal"], fontSize=12,
                          alignment=TA_CENTER, textColor=colors.HexColor("#52606d")))
    s.add(ParagraphStyle("H1", parent=s["Heading1"], fontSize=16,
                          textColor=colors.HexColor(C_TRUST), spaceBefore=14,
                          spaceAfter=8))
    s.add(ParagraphStyle("H2", parent=s["Heading2"], fontSize=12.5,
                          textColor=colors.HexColor(INK), spaceBefore=10,
                          spaceAfter=5))
    s.add(ParagraphStyle("Body", parent=s["Normal"], fontSize=10,
                          leading=15, alignment=TA_JUSTIFY, spaceAfter=6))
    s.add(ParagraphStyle("Cap", parent=s["Normal"], fontSize=8.5,
                          alignment=TA_CENTER, textColor=colors.HexColor("#7b8794"),
                          spaceBefore=3, spaceAfter=10))
    s.add(ParagraphStyle("Bul", parent=s["Normal"], fontSize=10, leading=14,
                          leftIndent=12, spaceAfter=3))
    return s


def P(t, st):
    return Paragraph(t, st)


def fig_img(path, w=16.5 * cm):
    from PIL import Image as PILImage
    iw, ih = PILImage.open(path).size
    return Image(path, width=w, height=w * ih / iw)


def tbl(data, widths, head=True):
    t = Table(data, colWidths=widths, hAlign="CENTER")
    cmds = [
        ("FONTSIZE", (0, 0), (-1, -1), 8.3),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd2d9")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f5f7fa")]),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    if head:
        cmds += [
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(C_TRUST)),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ]
    t.setStyle(TableStyle(cmds))
    return t


def build():
    d1, d2, d3, d4 = (diagram_pipeline(), diagram_star(),
                      diagram_agents(), diagram_stream())
    s = styles()
    doc = SimpleDocTemplate(
        str(OUT), pagesize=A4, title="BDM P2 — Technical Report",
        author="Albert Puiggròs", leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=1.8 * cm, bottomMargin=1.8 * cm)
    e = []

    # ── cover ──
    e += [Spacer(1, 3.5 * cm),
          P("Historical Conversational AI", s["Cover"]),
          Spacer(1, 4 * mm),
          P("BDM Project 2 — Technical Report", s["Sub"]),
          Spacer(1, 2 * mm),
          P("Exploitation &amp; Consumption Zones · pipeline logic and design",
            s["Sub"]),
          Spacer(1, 1.2 * cm), fig_img(d1, 15 * cm),
          Spacer(1, 8 * mm),
          P("Author: Albert Puiggròs &nbsp;·&nbsp; Status: pipeline green "
            "end-to-end &nbsp;·&nbsp; Streaming: planned integration",
            s["Sub"]),
          PageBreak()]

    # ── 1. overview ──
    e += [P("1 · What we built", s["H1"]),
          P("The product is a <b>generated conversational podcast</b>: for any "
            "of nine historical figures (5 philosophers — Plato, Descartes, "
            "Kant, Hegel, Nietzsche; 2 scientists — Einstein, Darwin; 2 authors "
            "— Wilde, Twain) the system produces a host-interview episode where "
            "the figure answers <i>in character</i>, grounded in what they "
            "actually wrote, in a voice blended from their own maxims and "
            "prose, on an intellectually rich topic loosely inspired by recent "
            "news.", s["Body"]),
          P("Every architectural choice is forced by a downstream consumer — "
            "the grading criterion the course rewards. The pipeline is a "
            "<b>three-store lakehouse</b>: MinIO (raw bytes + cleaned book "
            "text), DuckDB (all tabular data — Trusted and Exploitation), "
            "Milvus (sentence-embedding vectors). MongoDB was deliberately "
            "dropped after the Trusted Zone proved DuckDB handles the "
            "semi-structured sources cleanly — adding it would have been a "
            "tool with no consumer to justify it.", s["Body"]),
          fig_img(d1),
          P("Figure 1 — The four zones and the three stores. Streaming "
            "(bottom) is designed and pending integration with Santi's "
            "branch.", s["Cap"]),
          PageBreak()]

    # ── 2. trusted recap ──
    e += [P("2 · Trusted Zone (recap)", s["H1"]),
          P("The Trusted Zone is upstream of this report's focus, so only its "
            "contract matters here: nine heterogeneous sources are ingested to "
            "MinIO and cleaned by Spark jobs into conformed, deduplicated, "
            "type-safe sets. Spark is justified here — it reads JSON/text from "
            "MinIO in parallel at volume. The outputs land as "
            "<b>trusted_*</b> tables in <font face='Courier'>trusted.duckdb"
            "</font> plus boilerplate-stripped book text back in MinIO. These "
            "are the only inputs the Exploitation Zone consumes.", s["Body"]),
          tbl([["Source", "Trusted output", "Feeds (Exploitation)"],
               ["Philosophers API", "trusted_philosophers", "dim_figure (bio)"],
               ["Wikipedia", "trusted_wikipedia", "dim_figure (summary/desc)"],
               ["Wikiquote", "trusted_wikiquote", "fact_quotes, corpus_chunks"],
               ["Gutenberg catalog", "trusted_gutenberg_*", "fact_works"],
               ["Gutenberg books", "cleaned text in MinIO", "corpus_chunks"],
               ["GNews", "trusted_news_articles", "fact_news_articles"],
               ["Philosophy Stack Exch.", "trusted_se_*", "fact_se_qa, corpus"],
               ["Figure images", "trusted_philosopher_images", "dim_figure"],
               ["Mentions stream", "(Santi — planned)", "fact_mentions_1m"]],
              [4.5 * cm, 5.2 * cm, 6 * cm]),
          PageBreak()]

    # ── 3. exploitation ──
    e += [P("3 · Exploitation Zone", s["H1"]),
          P("The Exploitation Zone reshapes the trusted sets into a "
            "<b>star schema</b> plus a <b>vector corpus</b>, each table "
            "purpose-built for a named consumer (dashboard KPIs or the "
            "podcast agents). All tabular work is pure DuckDB SQL — source and "
            "target are both DuckDB and the joins are tiny, so a Spark JVM "
            "would add startup cost with zero throughput gain. Spark is used "
            "in exactly one place: the embedding job (§3.3).", s["Body"]),
          fig_img(d2),
          P("Figure 2 — dim_figure is the single conformed dimension; every "
            "fact joins it on figure_slug, except news (by design) and the "
            "Milvus corpus (slug is a metadata filter, not a SQL FK).",
            s["Cap"]),
          P("3.1 · The conformed dimension", s["H2"]),
          P("<font face='Courier'>dim_figure</font> anchors on the 9-figure "
            "character registry and LEFT JOINs the trusted sets, so the 5 "
            "philosophers get school/dates/interests while scientists and "
            "authors simply carry NULL there — one honest row per figure. It "
            "also carries the Wikipedia <i>summary</i> and one-line "
            "<i>description</i>, the factual anchor every agent reads.",
            s["Body"]),
          P("3.2 · The fact tables (derivation logic)", s["H2"]),
          tbl([["Table", "Grain", "Key derivation logic"],
               ["fact_works", "book × figure",
                "INNER JOIN dim_figure; keeps all books, has_local_text flags "
                "the embeddable subset"],
               ["fact_quotes", "quote",
                "quote_type by_figure / about_figure — by_figure feeds Voice, "
                "about_figure feeds Reasoner"],
               ["fact_news_articles", "article",
                "Pass-through, NO figure_slug — GNews is category-broad; "
                "figure relevance decided at LLM prompt time"],
               ["fact_se_qa", "question",
                "Top answer inlined (accepted &gt; score); mentioned_figures[] "
                "via tag-prefix + word-boundary regex on title/body"]],
              [3.4 * cm, 3 * cm, 9.3 * cm]),
          Spacer(1, 4 * mm),
          P("Live counts: 9 figures (5 philosophy / 2 science / 2 "
            "literature), 166 works, 1,996 quotes, 180 news articles, 500 "
            "Stack-Exchange Q&amp;A.", s["Body"]),
          P("3.3 · corpus_chunks — the Spark scalability proof", s["H2"]),
          P("The embedding job is the project's deliberate Spark "
            "scalability demonstration. It reads ~162 books from MinIO, "
            "windows each into 220-word chunks with 40-word overlap, embeds "
            "them with all-MiniLM-L6-v2 (384-d, normalized for COSINE) and "
            "writes them to the Milvus <font face='Courier'>corpus_chunks</font>"
            " collection (HNSW, M=16, efConstruction=200), with the chunk text "
            "inlined for single-call retrieval. The verified collection holds "
            "<b>≈87,437 vectors</b> across books, Wikipedia, Wikiquote and "
            "figure-linked Stack Exchange.", s["Body"]),
          P("The scaling lesson is in the execution model: Spark parallelizes "
            "only the embarrassingly-parallel MinIO read+chunk; embedding and "
            "insertion are then <i>streamed to the driver</i> via "
            "toLocalIterator() in 1,000-row micro-batches with a single model "
            "load — constant memory regardless of corpus size. A naive "
            "collect() with a per-partition model is what does not scale, and "
            "showing that contrast is the point.", s["Body"]),
          PageBreak()]

    # ── 4. consumption ──
    e += [P("4 · Consumption Zone — the product", s["H1"]),
          P("Four cooperating agents turn the Exploitation stores into a "
            "single-figure interview episode. Generation is injected as a "
            "provider-swappable <font face='Courier'>llm_fn</font> (Anthropic "
            "Claude); with no key the pipeline still emits the full episode "
            "structure, so it is testable offline.", s["Body"]),
          fig_img(d3),
          P("Figure 3 — Interviewer frames; Composer drives the loop; "
            "Reasoner grounds; Voice styles. Facts and style stay strictly "
            "separated.", s["Cap"]),
          P("4.1 · Reasoner — substance", s["H2"]),
          P("Loads the DuckDB identity card and retrieves the top-k "
            "figure-filtered passages from Milvus, then assembles one grounded "
            "prompt that forbids invention beyond the evidence. If Milvus is "
            "unreachable it degrades to identity-card-only rather than "
            "failing.", s["Body"]),
          P("4.2 · Voice — style (blended)", s["H2"]),
          P("Reuses the Reasoner's retrieval twice: the figure's own "
            "Wikiquote maxims (cadence) and one Gutenberg prose excerpt "
            "(register), both queried with the interview question so the "
            "exemplars are topically adjacent. The result is injected as a "
            "style block — it changes how the answer <i>sounds</i>, never what "
            "it asserts.", s["Body"]),
          P("4.3 · Interviewer — a topic curator", s["H2"]),
          P("Crucially <i>not</i> a news-reader. RAG over complete historical "
            "corpora will surface period bigotry if charged news is matched "
            "against it (an observed failure). The Interviewer instead curates "
            "evergreen, intellectually rich themes (AI, scientific truth, "
            "social behaviour, ethics), using headlines only as loose "
            "inspiration when they genuinely fit the figure, and avoids raw "
            "geopolitics. This structurally defuses the failure mode rather "
            "than filtering after the fact. It opens with a curated question, "
            "then generates each follow-up from the transcript so far — a real "
            "adaptive interview, not a questionnaire.", s["Body"]),
          P("4.4 · Bio-grounded delivery (new)", s["H2"]),
          P("The identity card now also derives, from curated data only, a "
            "<font face='Courier'>voice_descriptor</font>: full birth/death "
            "dates when ingested, an exact lifespan via signed-year arithmetic "
            "(Plato −428→−348 = 80 yrs), and the Wikipedia one-line "
            "description — which already encodes origin and role (e.g. "
            "&ldquo;German-born theoretical physicist&rdquo;). No native "
            "language or accent is invented; the model is told to let period "
            "and stated origin colour the register, nothing more. The same "
            "string is the intended seed for the future text-to-speech voice.",
            s["Body"]),
          P("4.5 · Episode composer", s["H2"]),
          P("Stitches a themed cold-open quote, the host framing, the "
            "conversational turn loop (history threaded through every answer "
            "for coherence and non-repetition), and a sign-off into JSON + a "
            "readable Markdown transcript. A 6-question episode is ≈12 Claude "
            "calls, ≈ $0.30 on Sonnet.", s["Body"]),
          PageBreak()]

    # ── 5. orchestration ──
    e += [P("5 · Orchestration &amp; infrastructure", s["H1"]),
          P("Two Airflow DAGs (LocalExecutor, manual-trigger). "
            "<font face='Courier'>bdm_p2_trusted_zone</font> cleans the nine "
            "sources and, on completion, auto-triggers "
            "<font face='Courier'>bdm_p2_exploitation_zone</font> via "
            "TriggerDagRunOperator. The Exploitation DAG gates on MinIO and "
            "Milvus health, builds dim_figure, then the facts. Fact builds are "
            "<b>serialized with chain()</b> because DuckDB is single-writer — "
            "parallel writers collide on its file lock. corpus_chunks then "
            "embeds, and two verifier tasks (DuckDB row sanity, per-source "
            "Milvus existence) gate &lsquo;complete&rsquo;.", s["Body"]),
          P("Milvus runs as a standalone Docker stack (milvus + etcd + an "
            "internal-only MinIO, kept portless so it cannot collide with the "
            "lakehouse MinIO). The embedding deps (CPU torch + "
            "sentence-transformers + pymilvus) and the Anthropic SDK are baked "
            "into the Airflow image; the consumption package is mounted in so "
            "it has the deps and can reach Milvus.", s["Body"]),
          P("6 · Data products &amp; status", s["H1"]),
          tbl([["Component", "Consumer", "Status"],
               ["dim_figure + 4 facts", "Dashboard KPIs, agents", "Green"],
               ["corpus_chunks (Milvus)", "Reasoner/Voice RAG",
                "Green (≈87k)"],
               ["Conversational podcast", "End user (demo)", "Green, live"],
               ["Bio voice_descriptor", "Text register + future TTS",
                "Done"],
               ["Streamlit dashboard", "Project demo", "Planned (#20)"],
               ["fact_mentions_1m", "Real-time KPI", "Planned (Santi)"],
               ["Text-to-speech", "Audio episode", "Planned"]],
              [5 * cm, 5.5 * cm, 5 * cm]),
          PageBreak()]

    # ── 7. streaming ──
    e += [P("7 · Planned integration — streaming (Santi sync)", s["H1"]),
          P("Santi's branch runs a Spark Structured Streaming job: it reads "
            "the Kafka <font face='Courier'>character-mentions</font> topic, "
            "applies a 1-minute tumbling window with a 30-second watermark, "
            "aggregates to (window_start, window_end, character_name, domain, "
            "mention_count, avg_sentiment) and writes Parquet to "
            "<font face='Courier'>/app/trusted/streaming/fact_mentions_1m"
            "</font>. It slots into our Exploitation Zone as a real-time fact "
            "table joining dim_figure.", s["Body"]),
          fig_img(d4),
          P("Figure 4 — The streaming seam and the three decisions to settle "
            "in tomorrow's sync.", s["Cap"]),
          P("Decisions for the meeting:", s["H2"]),
          P("• <b>Naming</b> — his <font face='Courier'>character_name</font> "
            "must reconcile with our <font face='Courier'>figure_slug</font> "
            "key, or the join silently drops rows.", s["Bul"]),
          P("• <b>Landing form</b> — expose the Parquet as a DuckDB "
            "<font face='Courier'>read_parquet</font> view (always fresh, "
            "zero copy) versus a periodic batch load into exploit.duckdb "
            "(stable, queryable offline). Recommendation: a view for the live "
            "KPI, materialized on DAG run for reproducibility.", s["Bul"]),
          P("• <b>Merge</b> — origin/santi forked <i>before</i> our Trusted "
            "commits, so its diff shows our work as deleted; integration is a "
            "<b>rebase</b>, never a fast-forward.", s["Bul"]),
          P("Once joined, fact_mentions_1m gives the dashboard a live "
            "&ldquo;who is being talked about now&rdquo; signal and could feed "
            "the Interviewer a real-time topical cue — closing the loop "
            "between the streaming and consumption ends of the pipeline.",
            s["Body"])]

    doc.build(e)
    return OUT


if __name__ == "__main__":
    out = build()
    print(f"written: {out}  ({os.path.getsize(out)/1024:.0f} KB)")
