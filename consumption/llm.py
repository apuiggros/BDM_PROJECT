"""
consumption/llm.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Consumption Zone — LLM client

A single provider-swappable `llm_fn(prompt: str) -> str` used by every agent
(Interviewer / Reasoner / Voice). The agents never import a vendor SDK
directly — they only ever receive this callable — so the backend can change
without touching agent code.

Backend: Anthropic Claude (decided 2026-05-19). The API key is read from
`ANTHROPIC_API_KEY`; the model from `LLM_MODEL` (default a current Claude
Sonnet — strong enough for in-voice generation, far cheaper than Opus for
the dozens of turns an episode needs).

Failure is deliberate and late: if the key/SDK is missing, the callable
raises a clear, actionable error WHEN CALLED — never at import — so every
agent's prompt-only path keeps working offline for tests and demos.
"""
from __future__ import annotations

import os
from typing import Callable

DEFAULT_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-4-6")
MAX_TOKENS    = int(os.getenv("LLM_MAX_TOKENS", "1024"))


def get_llm(model: str | None = None,
            temperature: float = 0.8) -> Callable[[str], str]:
    """
    Return an `llm_fn(prompt) -> str`. Temperature defaults high-ish: podcast
    answers should sound alive, not like a citation dump (grounding already
    keeps them factual).
    """
    model = model or DEFAULT_MODEL

    def llm_fn(prompt: str) -> str:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set — export it in the consumption "
                "runtime to generate episodes (agents still work prompt-only "
                "without it)."
            )
        try:
            import anthropic
        except ImportError as e:
            raise RuntimeError(
                "the 'anthropic' package is not installed — `pip install "
                "anthropic` in the consumption runtime."
            ) from e

        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=model,
            max_tokens=MAX_TOKENS,
            temperature=temperature,
            messages=[{"role": "user", "content": prompt}],
        )
        # Concatenate text blocks (Claude may return several).
        return "".join(
            b.text for b in msg.content if getattr(b, "type", None) == "text"
        ).strip()

    return llm_fn
