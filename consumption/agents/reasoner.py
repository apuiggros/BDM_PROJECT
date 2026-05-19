"""
consumption/agents/reasoner.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consumption Zone — Reasoner agent

The Reasoner is the substance layer of the podcast pipeline. Given a figure
and an interview question, it answers GROUNDED in what the figure actually
wrote/argued — never free-floating LLM trivia. It does two things and only
two things:

  1. Identity card  — pulls the figure's factual anchor from DuckDB
     `dim_figure` (name, domain, era, Wikipedia summary; plus school /
     dates / interests for the philosophers, which are NULL for the
     scientists & authors and handled accordingly).
  2. Evidence       — retrieves the most relevant passages from the Milvus
     `corpus_chunks` collection, filtered to THIS figure, across their
     books / Wikipedia / Wikiquote / figure-linked Stack Exchange.

It then assembles a single grounded prompt. Generation itself is injected
(`llm_fn`) so this module has no API-key dependency and is fully testable
offline: with no `llm_fn` it returns the grounding + the exact prompt.

Seams for the other two agents (built later):
  • Voice agent      → `persona_style` argument of `render_prompt`: style
    exemplars (Gutenberg / Wikiquote by_figure) get injected there so the
    answer *sounds* like the figure without changing the grounding.
  • Interviewer agent → supplies the `question` (derived from recent news);
    the Reasoner is deliberately agnostic to where the question came from.

CLI (demoable the moment Milvus `corpus_chunks` is populated):
    python -m consumption.agents.reasoner --figure einstein \
        --query "What would you say about quantum computing?"
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import duckdb

# ─── Configuration ────────────────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parents[2]
DUCKDB_DIR = Path(os.getenv("DUCKDB_DIR", ROOT / "duckdb"))
EXPLOIT_DB = DUCKDB_DIR / "exploit.duckdb"

MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = os.getenv("MILVUS_PORT", "19530")
COLLECTION  = "corpus_chunks"

EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
EMBED_DIM   = 384
DEFAULT_TOP_K = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("reasoner")


# ─── Data structures ──────────────────────────────────────────────────────────
@dataclass
class IdentityCard:
    figure_slug: str
    name: str
    domain: str
    description: Optional[str]          # "German-born theoretical physicist…"
    summary: Optional[str]             # Wikipedia extract — universal anchor
    school: Optional[str] = None       # philosophers only (else NULL)
    born: Optional[int] = None
    died: Optional[int] = None
    interests: Optional[str] = None
    wikipedia_link: Optional[str] = None

    def as_brief(self) -> str:
        """Compact factual block for the prompt header."""
        bits = [f"{self.name} — {self.description or self.domain}"]
        if self.school:
            bits.append(f"School: {self.school}")
        if self.born or self.died:
            bits.append(f"Era: {self.born or '?'}–{self.died or '?'}")
        if self.interests:
            bits.append(f"Interests: {self.interests}")
        if self.summary:
            bits.append(self.summary)
        return "\n".join(bits)


@dataclass
class Passage:
    text: str
    source: str          # gutenberg | wikipedia | wikiquote | stackexchange
    source_id: str
    subtype: str          # '' | by_figure | about_figure
    score: float

    def cite(self) -> str:
        tag = f"{self.source}:{self.source_id}"
        if self.subtype:
            tag += f"/{self.subtype}"
        return tag


@dataclass
class Grounding:
    figure_slug: str
    question: str
    card: IdentityCard
    passages: list[Passage] = field(default_factory=list)


@dataclass
class GroundedAnswer:
    grounding: Grounding
    prompt: str
    text: Optional[str] = None   # None when no llm_fn was supplied (offline)


# ─── Identity card (DuckDB) ───────────────────────────────────────────────────
def load_identity_card(figure_slug: str) -> IdentityCard:
    if not EXPLOIT_DB.exists():
        raise FileNotFoundError(f"exploit.duckdb not found at {EXPLOIT_DB}")
    con = duckdb.connect(str(EXPLOIT_DB), read_only=True)
    row = con.execute(
        """
        SELECT figure_slug, name, domain, wikipedia_description,
               wikipedia_summary, school, born, died, interests, wikipedia_link
        FROM dim_figure WHERE figure_slug = ?
        """,
        [figure_slug],
    ).fetchone()
    con.close()
    if row is None:
        raise KeyError(f"Unknown figure_slug '{figure_slug}' — not in dim_figure")
    return IdentityCard(
        figure_slug=row[0], name=row[1], domain=row[2],
        description=row[3], summary=row[4], school=row[5],
        born=row[6], died=row[7], interests=row[8], wikipedia_link=row[9],
    )


# ─── Evidence (Milvus) ────────────────────────────────────────────────────────
_EMBEDDER = None


def _embedder():
    """Lazy singleton. Imported here so the identity-card path works in
    environments without sentence-transformers installed."""
    global _EMBEDDER
    if _EMBEDDER is None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:  # pragma: no cover
            raise RuntimeError(
                "sentence-transformers not installed — needed for retrieval. "
                "Install it or run the identity-card-only path."
            ) from e
        _EMBEDDER = SentenceTransformer(EMBED_MODEL)
    return _EMBEDDER


def retrieve(
    figure_slug: str,
    query: str,
    top_k: int = DEFAULT_TOP_K,
    sources: Optional[list[str]] = None,
    subtype: Optional[str] = None,
) -> list[Passage]:
    """
    Top-k Milvus passages for this figure. `sources` / `subtype` are the
    metadata filters the Voice agent will use (e.g. sources=['gutenberg'],
    subtype='by_figure' for authentic-voice exemplars). Returns [] — never
    raises — if Milvus is unreachable or the collection is empty, so the
    pipeline degrades to identity-card-only rather than failing.
    """
    try:
        from pymilvus import Collection, connections, utility
    except ImportError:
        log.warning("pymilvus not installed — skipping retrieval.")
        return []

    try:
        connections.connect(alias="default",
                             host=MILVUS_HOST, port=MILVUS_PORT)
        if not utility.has_collection(COLLECTION):
            log.warning("Collection %s missing — identity-card-only.", COLLECTION)
            return []
        col = Collection(COLLECTION)
        col.load()

        expr = f'figure_slug == "{figure_slug}"'
        if sources:
            quoted = ", ".join(f'"{s}"' for s in sources)
            expr += f" and source in [{quoted}]"
        if subtype is not None:
            expr += f' and subtype == "{subtype}"'

        vec = _embedder().encode([query], normalize_embeddings=True)[0]
        res = col.search(
            [vec.tolist()], "embedding",
            {"metric_type": "COSINE", "params": {"ef": 64}},
            limit=top_k, expr=expr,
            output_fields=["chunk_text", "source", "source_id", "subtype"],
        )
        out: list[Passage] = []
        for hit in res[0]:
            e = hit.entity
            out.append(Passage(
                text=e.get("chunk_text"), source=e.get("source"),
                source_id=str(e.get("source_id")),
                subtype=e.get("subtype") or "",
                score=float(hit.distance),
            ))
        return out
    except Exception as e:  # noqa: BLE001 — degrade, don't crash the podcast
        log.warning("Retrieval failed (%s) — identity-card-only.", e)
        return []


# ─── Grounding + prompt ───────────────────────────────────────────────────────
def build_grounding(figure_slug: str, question: str,
                     top_k: int = DEFAULT_TOP_K) -> Grounding:
    card = load_identity_card(figure_slug)
    passages = retrieve(figure_slug, question, top_k=top_k)
    return Grounding(figure_slug, question, card, passages)


# A turn is (question, answer). History is the conversation BEFORE this turn.
History = list[tuple[str, str]]
_HIST_ANSWER_CAP = 500   # chars of each prior answer kept in the prompt


def render_prompt(g: Grounding,
                  persona_style: Optional[str] = None,
                  history: Optional[History] = None) -> str:
    """
    Assemble the grounded interview prompt. `persona_style` is the Voice
    agent's seam (style, kept separate from facts). `history` is the prior
    Q/A of THIS episode so the figure answers in a coherent thread — builds
    on what it already said and does not repeat itself. Prior answers are
    truncated so the prompt grows slowly across a long interview.
    """
    evidence = (
        "\n\n".join(
            f"[{i+1}] ({p.cite()}) {p.text}"
            for i, p in enumerate(g.passages)
        )
        if g.passages
        else "(no retrieved passages — answer only from the identity card "
             "and stay cautious; do not invent specifics)"
    )
    style_block = (
        f"\n\nSPEAK IN THIS VOICE:\n{persona_style}\n"
        if persona_style else ""
    )
    history_block = ""
    if history:
        convo = "\n".join(
            f"INTERVIEWER: {q}\n{g.card.name}: {a[:_HIST_ANSWER_CAP]}"
            for q, a in history
        )
        history_block = (
            f"\n\nCONVERSATION SO FAR (continue it naturally; do NOT repeat "
            f"points you already made):\n{convo}\n"
        )
    return (
        f"You are {g.card.name}, being interviewed on a podcast. Answer in "
        f"the first person, in character, concise and quotable.\n\n"
        f"WHO YOU ARE:\n{g.card.as_brief()}\n\n"
        f"GROUND YOUR ANSWER ONLY IN THESE PASSAGES FROM YOUR OWN WORK "
        f"(cite them inline as [n]); if they don't cover the question, say "
        f"so in character rather than inventing:\n{evidence}"
        f"{style_block}{history_block}\n\n"
        f"INTERVIEWER'S QUESTION:\n{g.question}\n\n"
        f"{g.card.name}:"
    )


def answer(
    figure_slug: str,
    question: str,
    llm_fn: Optional[Callable[[str], str]] = None,
    top_k: int = DEFAULT_TOP_K,
    persona_style: Optional[str] = None,
    history: Optional[History] = None,
) -> GroundedAnswer:
    """
    Full Reasoner turn. `llm_fn` maps prompt→completion (inject your provider
    of choice). Without it, returns the grounding + prompt so the pipeline is
    testable with no API key. `history` carries the prior turns of the
    episode for conversational coherence.
    """
    g = build_grounding(figure_slug, question, top_k=top_k)
    prompt = render_prompt(g, persona_style=persona_style, history=history)
    text = llm_fn(prompt) if llm_fn else None
    return GroundedAnswer(grounding=g, prompt=prompt, text=text)


# ─── CLI ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Reasoner agent — grounded figure answer.")
    ap.add_argument("--figure", required=True, help="figure_slug (e.g. einstein)")
    ap.add_argument("--query",  required=True, help="interview question")
    ap.add_argument("--top-k",  type=int, default=DEFAULT_TOP_K)
    ap.add_argument("--json",   action="store_true", help="emit grounding as JSON")
    args = ap.parse_args()

    ga = answer(args.figure, args.query, top_k=args.top_k)
    g = ga.grounding
    if args.json:
        print(json.dumps({
            "figure": g.figure_slug,
            "question": g.question,
            "card": g.card.as_brief(),
            "passages": [
                {"cite": p.cite(), "score": round(p.score, 4),
                 "text": p.text[:300]} for p in g.passages
            ],
        }, indent=2, ensure_ascii=False))
    else:
        print("=" * 70)
        print(f"FIGURE   : {g.card.name} ({g.figure_slug})")
        print(f"QUESTION : {g.question}")
        print(f"PASSAGES : {len(g.passages)} retrieved")
        for i, p in enumerate(g.passages):
            print(f"  [{i+1}] {p.cite():28s} score={p.score:.3f}  "
                  f"{p.text[:90].strip()}…")
        print("=" * 70)
        print("PROMPT THAT WOULD GO TO THE LLM:\n")
        print(ga.prompt)
        if ga.text is None:
            print("\n(no --llm configured: grounding + prompt only)")
