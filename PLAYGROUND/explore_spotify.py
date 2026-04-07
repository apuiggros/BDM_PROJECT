"""
explore_spotify.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PURPOSE: Interactively explore the Spotify Web API to understand:
  - Show object structure (name, publisher, total_episodes, languages …)
  - Episode object structure (id, name, duration_ms, preview_url, …)
  - Which episodes have preview audio vs. which don't (preview_url = None)
  - Audio preview availability rate across a sample of episodes

REQUIREMENTS: Fill in SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in your .env
Run with:  python PLAYGROUND/explore_spotify.py
"""

import json
import os
import pprint
import sys

from dotenv import load_dotenv

load_dotenv()

try:
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
except ImportError:
    sys.exit("Install spotipy first:  pip install spotipy")

CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

# ── Philosophy podcast show IDs to explore ────────────────────────────────────
SHOW_IDS = {
    "Philosophize This!":                    "1KtEMtZiJAU4Yz38c2VJDB",
    "History of Philosophy (Peter Adamson)": "5N6LumMFsBKrMHliCoqr9e",
    "Mindscape (Sean Carroll)":              "2hmkzUtix0qTqvfBIqRFkr",
}

EPISODES_TO_SAMPLE = 5   # How many episodes to inspect per show


def get_client() -> spotipy.Spotify:
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit(
            "\n❌  SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET not set in .env\n"
            "    Get credentials at: https://developer.spotify.com/dashboard\n"
        )
    return spotipy.Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=CLIENT_ID, client_secret=CLIENT_SECRET
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: Show-level metadata
# ─────────────────────────────────────────────────────────────────────────────
def inspect_show(sp: spotipy.Spotify, label: str, show_id: str):
    print(f"\n{'═'*60}")
    print(f"SECTION 1 — Show Object: '{label}'  (id={show_id})")
    print("═"*60)
    show = sp.show(show_id, market="US")
    print("\n  Top-level keys:", list(show.keys()))
    print("\n  Key fields:")
    for field in ["id", "name", "publisher", "total_episodes", "languages",
                  "explicit", "media_type", "description"]:
        print(f"    {field:20s}: {repr(show.get(field, '—'))[:100]}")
    print("\n  Full show object (first 2000 chars of JSON):")
    print(json.dumps(show, indent=2)[:2000])


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2: Episode-level metadata for first N episodes
# ─────────────────────────────────────────────────────────────────────────────
def inspect_episodes(sp: spotipy.Spotify, label: str, show_id: str):
    print(f"\n{'═'*60}")
    print(f"SECTION 2 — Episode Objects: '{label}'  (first {EPISODES_TO_SAMPLE})")
    print("═"*60)
    page = sp.show_episodes(show_id, limit=EPISODES_TO_SAMPLE, market="US")
    episodes = page.get("items", [])

    audio_available = 0
    for i, ep in enumerate(episodes, 1):
        has_preview = ep.get("preview_url") is not None
        if has_preview:
            audio_available += 1
        print(f"\n  Episode {i}: {ep.get('name', '?')[:70]}")
        print(f"    id            : {ep.get('id')}")
        print(f"    duration_ms   : {ep.get('duration_ms')}  ({ep.get('duration_ms',0)//60000} min)")
        print(f"    release_date  : {ep.get('release_date')}")
        print(f"    language      : {ep.get('language')}")
        print(f"    preview_url   : {'✓ AVAILABLE' if has_preview else '✗ None (no free audio)'}")
        print(f"    explicit      : {ep.get('explicit')}")

    print(f"\n  ── Audio Preview Summary ──")
    print(f"  {audio_available}/{len(episodes)} episodes have a preview_url")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3: Episode object full schema dump (field types)
# ─────────────────────────────────────────────────────────────────────────────
def episode_schema(sp: spotipy.Spotify, show_id: str):
    print(f"\n{'═'*60}")
    print("SECTION 3 — Episode Schema (all fields + types)")
    print("═"*60)
    page = sp.show_episodes(show_id, limit=1, market="US")
    ep = page.get("items", [{}])[0]
    print("\n  All keys and value types:")
    for key, val in ep.items():
        print(f"  {key:25s}: {type(val).__name__:10s}  →  {repr(val)[:80]}")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4: Search — find philosophy podcasts by keyword
# ─────────────────────────────────────────────────────────────────────────────
def search_philosophy_shows(sp: spotipy.Spotify, query: str = "philosophy podcast", n: int = 5):
    print(f"\n{'═'*60}")
    print(f"SECTION 4 — Search: '{query}' (top {n} shows)")
    print("═"*60)
    results = sp.search(q=query, type="show", limit=n, market="US")
    shows = results.get("shows", {}).get("items", [])
    for show in shows:
        print(f"\n  ► {show.get('name')}")
        print(f"    id           : {show.get('id')}")
        print(f"    publisher    : {show.get('publisher')}")
        print(f"    total_episodes: {show.get('total_episodes')}")
        print(f"    language     : {show.get('languages')}")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    sp = get_client()

    # Use the first show for detailed inspection
    first_label, first_id = next(iter(SHOW_IDS.items()))

    inspect_show(sp, first_label, first_id)
    inspect_episodes(sp, first_label, first_id)
    episode_schema(sp, first_id)
    search_philosophy_shows(sp)

    print("\n✓ Spotify exploration complete.\n")
