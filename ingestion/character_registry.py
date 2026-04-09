"""
character_registry.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single source of truth for ALL historical figures this pipeline targets.

All ingestion scripts import TARGET_FIGURES from here. Adding a new figure
to the entire pipeline — across Gutenberg, Wikipedia, and every other source —
requires editing ONLY this file. Nothing else changes.

Each entry is a dict with the following fields:
  - domain          : Broad category ('philosophy', 'science', 'literature', …)
  - api_name        : Display name (used in logs and where APIs need an exact match)
  - gutenberg_search: Keyword that works best in a Gutendex title/author search
  - gutenberg_author_slug: Lowercase author identifier for S3 paths and filtering
  - wikidata_label  : Exact Wikipedia article title for the REST API

To add a new figure, append one dict to TARGET_FIGURES. Done.
"""

TARGET_FIGURES: list[dict] = [

    # ── Philosophy ────────────────────────────────────────────────────────────
    {
        "domain":                "philosophy",
        "api_name":              "Plato",
        "gutenberg_search":      "Plato",
        "gutenberg_author_slug": "plato",
        "wikidata_label":        "Plato",
    },
    {
        "domain":                "philosophy",
        "api_name":              "René Descartes",
        "gutenberg_search":      "Descartes",
        "gutenberg_author_slug": "descartes",
        "wikidata_label":        "René Descartes",
    },
    {
        "domain":                "philosophy",
        "api_name":              "Immanuel Kant",
        "gutenberg_search":      "Kant",
        "gutenberg_author_slug": "kant",
        "wikidata_label":        "Immanuel Kant",
    },
    {
        "domain":                "philosophy",
        "api_name":              "Georg Wilhelm Friedrich Hegel",
        "gutenberg_search":      "Hegel",
        "gutenberg_author_slug": "hegel",
        "wikidata_label":        "Georg Wilhelm Friedrich Hegel",
    },
    {
        "domain":                "philosophy",
        "api_name":              "Friedrich Nietzsche",
        "gutenberg_search":      "Nietzsche",
        "gutenberg_author_slug": "nietzsche",
        "wikidata_label":        "Friedrich Nietzsche",
    },

    # ── Science ───────────────────────────────────────────────────────────────
    {
        "domain":                "science",
        "api_name":              "Albert Einstein",
        "gutenberg_search":      "Einstein",
        "gutenberg_author_slug": "einstein",
        "wikidata_label":        "Albert Einstein",
    },
    {
        "domain":                "science",
        "api_name":              "Charles Darwin",
        "gutenberg_search":      "Darwin",
        "gutenberg_author_slug": "darwin",
        "wikidata_label":        "Charles Darwin",
    },

    # ── Literature ────────────────────────────────────────────────────────────
    {
        "domain":                "literature",
        "api_name":              "Oscar Wilde",
        "gutenberg_search":      "Wilde",
        "gutenberg_author_slug": "wilde",
        "wikidata_label":        "Oscar Wilde",
    },
    {
        "domain":                "literature",
        "api_name":              "Mark Twain",
        "gutenberg_search":      "Mark Twain",
        "gutenberg_author_slug": "twain",
        "wikidata_label":        "Mark Twain",
    },

]

# ─── Convenience helpers ──────────────────────────────────────────────────────
def figures_by_domain(domain: str) -> list[dict]:
    """Return only the figures belonging to a given domain."""
    return [f for f in TARGET_FIGURES if f["domain"] == domain]

BY_SLUG      = {f["gutenberg_author_slug"]: f for f in TARGET_FIGURES}
BY_API_NAME  = {f["api_name"]: f for f in TARGET_FIGURES}
ALL_API_NAMES = [f["api_name"] for f in TARGET_FIGURES]

# ── Backward-compatible alias so old imports still work during the transition ─
TARGET_PHILOSOPHERS = figures_by_domain("philosophy")
