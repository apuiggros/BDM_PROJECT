"""
consumption/agents/interviewer.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consumption Zone — Interviewer agent (the podcast host)

The Interviewer is a TOPIC CURATOR, not a news reader. Recent headlines are
ambient inspiration — a signal of "what's in the air" — never a script:

  • It favours timeless, intellectually rich angles the figure can truly
    illuminate: the meaning of AI, the nature of scientific truth, human
    social behaviour, art & technology, freedom, knowledge, ethics.
  • It may take a cue from a current headline ONLY when it maps naturally
    onto that figure's genuine concerns; it never just summarises the news.
  • It deliberately steers away from raw geopolitics — both because that
    rarely yields the figure's best thinking and because matching charged
    news against a complete historical corpus can surface that corpus's
    prejudices instead of its insight. (This is a real, observed failure
    mode, not a hypothetical — see the project notes.)

The figure ANSWERS via Reasoner+Voice; the Interviewer only frames + asks.

`fact_news_articles` (DuckDB) remains the news layer's consumer — it just
feeds the curator as inspiration rather than as content.

CLI:
    python -m consumption.agents.interviewer --figure kant            # offline
    python -m consumption.agents.interviewer --figure kant --live     # + Claude
"""
from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import duckdb

from consumption.agents.reasoner import load_identity_card

ROOT       = Path(__file__).resolve().parents[2]
DUCKDB_DIR = Path(os.getenv("DUCKDB_DIR", ROOT / "duckdb"))
EXPLOIT_DB = DUCKDB_DIR / "exploit.duckdb"

HEADLINE_POOL = 15   # ambient headlines shown to the curator

# Offline fallback themes — chosen to be evergreen and figure-appropriate so
# even with no LLM the episode is interesting AND safe (no geopolitical match).
DOMAIN_THEME = {
    "philosophy": "Artificial intelligence that reasons and speaks: what "
                  "should we make of a mind we built?",
    "science":    "In an age of vast data and machines, what still makes a "
                  "scientific theory true?",
    "literature": "The artist in a society obsessed with technology: what is "
                  "still worth saying?",
}


@dataclass
class Topic:
    figure_slug: str
    theme: str                       # the curated angle (drives everything)
    framing: str                     # host's spoken setup
    questions: list[str]
    news_inspired: bool = False
    news_title: Optional[str] = None
    news_url: Optional[str] = None


def recent_news(limit: int = HEADLINE_POOL) -> list[dict]:
    if not EXPLOIT_DB.exists():
        raise FileNotFoundError(f"exploit.duckdb not found at {EXPLOIT_DB}")
    con = duckdb.connect(str(EXPLOIT_DB), read_only=True)
    rows = con.execute(
        f"""
        SELECT title, url, category
        FROM fact_news_articles
        ORDER BY published_at DESC
        LIMIT {int(limit)}
        """
    ).fetchall()
    con.close()
    return [{"title": r[0], "url": r[1], "category": r[2]} for r in rows]


def _curate_with_llm(card, headlines: list[dict],
                     llm_fn: Callable[[str], str], n: int) -> Optional[dict]:
    listing = "\n".join(
        f"- [{h['category']}] {h['title']}" for h in headlines
    )
    prompt = (
        f"You produce a thoughtful interview podcast. Today's guest is "
        f"{card.name} ({card.description or card.domain}).\n\n"
        f"Choose ONE rich, intellectually engaging theme this figure can "
        f"genuinely illuminate — e.g. the meaning of artificial "
        f"intelligence, the nature of scientific truth, human social "
        f"behaviour, art and technology, freedom, knowledge, ethics of "
        f"modern life. You MAY draw a cue from a headline below ONLY if it "
        f"maps naturally onto {card.name}'s real concerns; otherwise ignore "
        f"the news entirely. Avoid raw geopolitics and any angle that would "
        f"mainly provoke prejudice rather than insight.\n\n"
        f"Ambient headlines (inspiration only, do NOT summarise them):\n"
        f"{listing}\n\n"
        f"Respond with ONLY a JSON object:\n"
        f'{{"theme": "<one engaging sentence>", '
        f'"news_inspired": true|false, '
        f'"news_title": "<exact headline if inspired, else empty>", '
        f'"questions": ["<{n} probing in-character questions>"]}}'
    )
    try:
        raw = llm_fn(prompt)
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if not m:
            return None
        data = json.loads(m.group())
        qs = [q.strip() for q in data.get("questions", []) if q.strip()][:n]
        if not data.get("theme") or not qs:
            return None
        return {
            "theme": data["theme"].strip(),
            "news_inspired": bool(data.get("news_inspired")),
            "news_title": (data.get("news_title") or "").strip() or None,
            "questions": qs,
        }
    except Exception:  # noqa: BLE001 — any failure → template fallback
        return None


def _template(card, n: int) -> dict:
    theme = DOMAIN_THEME.get(card.domain, DOMAIN_THEME["philosophy"])
    qs = [
        f"{card.name}, here is the question on the table: {theme} "
        f"Where do you begin?",
        "Which of your own ideas speaks most directly to this?",
        "What do you think people today get most wrong about it?",
        "What would you have us do differently?",
    ][:n]
    return {"theme": theme, "news_inspired": False,
            "news_title": None, "questions": qs}


def follow_up_question(card, theme: str,
                       history: list[tuple[str, str]],
                       llm_fn: Callable[[str], str],
                       is_last: bool = False) -> str:
    """
    The host's NEXT question, reacting to what the figure just said. This is
    what makes it an interview rather than a questionnaire: it reads the
    conversation so far and probes, challenges, or deepens — and on the final
    turn lands a strong closing question. Falls back to a safe generic probe
    if the model misbehaves.
    """
    convo = "\n".join(f"INTERVIEWER: {q}\n{card.name}: {a}"
                      for q, a in history)
    closing = (
        "This is the FINAL question — make it a memorable closing one that "
        "invites a reflective last word."
        if is_last else
        "Ask ONE incisive follow-up that builds on the guest's last answer — "
        "press a tension, ask for a concrete example, or go deeper. Do not "
        "repeat earlier ground."
    )
    prompt = (
        f"You are a sharp podcast host interviewing {card.name} "
        f"({card.description or card.domain}). Episode theme: {theme}\n\n"
        f"Conversation so far:\n{convo}\n\n"
        f"{closing} Reply with ONLY the question, no preamble, no quotes."
    )
    try:
        q = llm_fn(prompt).strip().strip('"').splitlines()[0].strip()
        if q:
            return q
    except Exception:  # noqa: BLE001
        pass
    return ("What would you most want a listener to take away from this?"
            if is_last else
            "Can you take that one step further with a concrete example?")


def pick_topic(figure_slug: str,
               llm_fn: Optional[Callable[[str], str]] = None,
               n_questions: int = 3) -> Topic:
    card = load_identity_card(figure_slug)

    data = None
    if llm_fn:
        try:
            headlines = recent_news()
        except FileNotFoundError:
            headlines = []
        data = _curate_with_llm(card, headlines, llm_fn, n_questions)
    if data is None:
        data = _template(card, n_questions)

    news_url = None
    if data["news_inspired"] and data["news_title"]:
        try:
            for h in recent_news(40):
                if h["title"] == data["news_title"]:
                    news_url = h["url"]
                    break
        except FileNotFoundError:
            pass

    cue = (f" (prompted by today's news: \"{data['news_title']}\")"
           if data["news_inspired"] and data["news_title"] else "")
    framing = (f"My guest today is {card.name}. The theme: "
               f"{data['theme']}{cue}")

    return Topic(
        figure_slug=figure_slug,
        theme=data["theme"],
        framing=framing,
        questions=data["questions"],
        news_inspired=data["news_inspired"],
        news_title=data["news_title"],
        news_url=news_url,
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Interviewer — curate topic + Qs.")
    ap.add_argument("--figure", required=True)
    ap.add_argument("--questions", type=int, default=3)
    ap.add_argument("--live", action="store_true",
                    help="Use Claude to curate the theme + questions.")
    args = ap.parse_args()

    fn = None
    if args.live:
        from consumption.llm import get_llm
        fn = get_llm()

    topic = pick_topic(args.figure, llm_fn=fn, n_questions=args.questions)
    print(json.dumps(topic.__dict__, indent=2, ensure_ascii=False))
