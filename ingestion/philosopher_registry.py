"""
philosopher_registry.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Single source of truth for which philosophers this pipeline targets.

All ingestion scripts import TARGET_PHILOSOPHERS from here.
Each entry maps the exact API name to the search terms each source needs.

To add a new philosopher, add one dict to the list — nothing else changes.
"""

TARGET_PHILOSOPHERS: list[dict] = [
    {
        # ── Exact name as it appears in the Philosophers API ──────────────────
        "api_name": "Plato",
        # ── Search term that works best on Gutendex ───────────────────────────
        "gutenberg_search": "Plato",
        # ── Author name as it appears in the Kaggle philosophy-quotes CSV ─────
        "kaggle_author": "Plato",
        # ── Wikidata label (used for SPARQL filter) ───────────────────────────
        "wikidata_label": "Plato",
        # ── Gutenberg author format (Last, First) for precise matching ────────
        "gutenberg_author_slug": "plato",
    },
    {
        "api_name": "René Descartes",
        "gutenberg_search": "Descartes",
        "kaggle_author": "René Descartes",
        "wikidata_label": "René Descartes",
        "gutenberg_author_slug": "descartes",
    },
    {
        "api_name": "Immanuel Kant",
        "gutenberg_search": "Kant",
        "kaggle_author": "Immanuel Kant",
        "wikidata_label": "Immanuel Kant",
        "gutenberg_author_slug": "kant",
    },
    {
        "api_name": "Georg Wilhelm Friedrich Hegel",
        "gutenberg_search": "Hegel",
        "kaggle_author": "Georg Wilhelm Friedrich Hegel",
        "wikidata_label": "Georg Wilhelm Friedrich Hegel",
        "gutenberg_author_slug": "hegel",
    },
    {
        "api_name": "Friedrich Nietzsche",
        "gutenberg_search": "Nietzsche",
        "kaggle_author": "Friedrich Nietzsche",
        "wikidata_label": "Friedrich Nietzsche",
        "gutenberg_author_slug": "nietzsche",
    },
]

# Convenience lookups
BY_API_NAME   = {p["api_name"]:   p for p in TARGET_PHILOSOPHERS}
BY_SLUG       = {p["gutenberg_author_slug"]: p for p in TARGET_PHILOSOPHERS}
ALL_API_NAMES = [p["api_name"] for p in TARGET_PHILOSOPHERS]
