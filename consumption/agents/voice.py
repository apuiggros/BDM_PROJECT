"""
consumption/agents/voice.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consumption Zone — Voice agent

Makes a podcast answer *sound* like the figure without touching what it
asserts. It produces a `persona_style` block; the Reasoner injects that into
its prompt via the seam already built into `render_prompt`. Grounding (facts)
and voice (style) stay strictly separate — the LLM gets its claims from the
Reasoner's evidence and its cadence from here.

Blended style (decided 2026-05-19):
  • Maxims        — the figure's OWN Wikiquote lines (source=wikiquote,
                    subtype=by_figure): short, gives cadence/diction.
  • Prose register — one excerpt from the figure's actual book
                    (source=gutenberg): gives sentence rhythm/register.

Both are retrieved with the interview question as the query, so the
exemplars are not just authentic but also topically adjacent — the figure
"sounds like themselves, on this subject."

Reuses `reasoner.retrieve()` (already supports the source/subtype filters),
so there is no duplicated Milvus/embedder code and the two agents cannot
drift apart.

CLI:
    python -m consumption.agents.voice --figure nietzsche \
        --query "What is the meaning of suffering?"
"""
from __future__ import annotations

import argparse
import re
from typing import Optional

from consumption.agents.reasoner import (
    GroundedAnswer, Passage, answer as reason_answer, retrieve,
)

MAX_MAXIM_CHARS = 220
MAX_PROSE_CHARS = 600


def _clean(text: str) -> str:
    """Collapse whitespace/newlines so an exemplar is one tidy line."""
    return re.sub(r"\s+", " ", (text or "").strip())


def build_persona_style(
    figure_slug: str,
    query: str,
    n_maxims: int = 4,
    n_prose: int = 1,
) -> Optional[str]:
    """
    Assemble the style block, or None if Milvus yields nothing (the Reasoner
    treats None as "no style guidance" and still answers from the identity
    card — the pipeline degrades, never breaks).
    """
    maxims = retrieve(
        figure_slug, query, top_k=n_maxims,
        sources=["wikiquote"], subtype="by_figure",
    )
    prose = retrieve(
        figure_slug, query, top_k=n_prose, sources=["gutenberg"],
    )

    lines: list[str] = []

    seen: set[str] = set()
    maxim_bullets: list[str] = []
    for p in maxims:
        q = _clean(p.text)[:MAX_MAXIM_CHARS]
        if q and q.lower() not in seen:
            seen.add(q.lower())
            maxim_bullets.append(f'- "{q}"')
    if maxim_bullets:
        lines.append("Maxims (the figure's own words):")
        lines.extend(maxim_bullets)

    if prose:
        excerpt = _clean(prose[0].text)[:MAX_PROSE_CHARS]
        if excerpt:
            if lines:
                lines.append("")
            lines.append("Prose register (from their own writing):")
            lines.append(f'"{excerpt}"')

    return "\n".join(lines) if lines else None


def voiced_answer(
    figure_slug: str,
    question: str,
    llm_fn=None,
    top_k: int = 6,
    history=None,
) -> GroundedAnswer:
    """
    Integration seam toward the episode composer: grounding from the Reasoner,
    cadence from here, in one call. `history` (prior episode turns) is passed
    through for conversational coherence. Without `llm_fn` it returns the
    assembled prompt so it stays testable offline.
    """
    style = build_persona_style(figure_slug, question)
    return reason_answer(
        figure_slug, question, llm_fn=llm_fn,
        top_k=top_k, persona_style=style, history=history,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Voice agent — persona style block.")
    ap.add_argument("--figure", required=True, help="figure_slug (e.g. nietzsche)")
    ap.add_argument("--query",  required=True, help="interview question")
    args = ap.parse_args()

    style = build_persona_style(args.figure, args.query)
    print("=" * 70)
    print(f"PERSONA STYLE for {args.figure!r}")
    print("=" * 70)
    print(style if style else "(no style exemplars retrieved — Milvus empty "
          "or unreachable; Reasoner will fall back to identity card)")
