"""
spotify_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : Spotify Web API  (https://developer.spotify.com/)
Data Type     : Unstructured (audio .mp3/.wav) + Semi-structured (JSON metadata)
Landing Zone  : s3://landing-zone/spotify/audio/
                s3://landing-zone/spotify/metadata/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
Uses the `spotipy` library to authenticate against the Spotify Web API via
Client Credentials flow (no user login required).  For a curated list of
philosophy-focused podcast show IDs, it:
  1. Fetches rich episode metadata (JSON) and stores it in the metadata prefix.
  2. Attempts to download preview audio clips (30-second MP3s freely available
     for most episodes) and stores them in the audio prefix.

NOTE: Full episode audio requires Spotify Premium + additional download rights.
      Preview clips (preview_url field) are the only audio legally accessible
      via the free tier of the API.  If `preview_url` is None for an episode,
      that audio file is skipped and a warning is logged.
"""

import io
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests
import spotipy
from botocore.client import Config
from dotenv import load_dotenv
from spotipy.oauth2 import SpotifyClientCredentials

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("spotify_ingest")

SPOTIFY_CLIENT_ID     = os.getenv("SPOTIFY_CLIENT_ID", "")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET", "")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")

# Target shows — philosophy / history podcasts on Spotify
# Replace or extend with actual IDs from the Spotify dashboard/search
PHILOSOPHY_SHOW_IDS: list[str] = [
    "1KtEMtZiJAU4Yz38c2VJDB",  # "Philosophize This!" — Stephen West
    "5N6LumMFsBKrMHliCoqr9e",  # "The History of Philosophy Without Any Gaps"
    "2hmkzUtix0qTqvfBIqRFkr",  # "Mindscape" — Sean Carroll
]

# Max episodes to ingest per show per run (set None for unlimited)
MAX_EPISODES_PER_SHOW: int | None = 20


# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    """Return a boto3 client configured for the local MinIO instance."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client: boto3.client, bucket: str) -> None:
    """Create the MinIO bucket if it does not already exist."""
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)


# ─── Upload Helpers ───────────────────────────────────────────────────────────
def upload_json(client: boto3.client, data: dict | list, key: str) -> None:
    """Serialize and upload a Python object as JSON to MinIO."""
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=payload, ContentType="application/json")
    logger.info("Metadata uploaded → s3://%s/%s (%d bytes)", MINIO_BUCKET, key, len(payload))


def upload_audio(client: boto3.client, audio_bytes: bytes, key: str) -> None:
    """Upload raw audio bytes (MP3) to MinIO."""
    client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=io.BytesIO(audio_bytes), ContentType="audio/mpeg")
    logger.info("Audio uploaded   → s3://%s/%s (%d bytes)", MINIO_BUCKET, key, len(audio_bytes))


# ─── Spotify Logic ────────────────────────────────────────────────────────────
def get_spotify_client() -> spotipy.Spotify:
    """
    Authenticate with Spotify using the Client Credentials OAuth flow.
    Credentials are read from environment variables (SPOTIFY_CLIENT_ID /
    SPOTIFY_CLIENT_SECRET) which should be set in your .env file.
    """
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        raise EnvironmentError(
            "SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET must be set in .env"
        )
    auth_manager = SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET,
    )
    return spotipy.Spotify(auth_manager=auth_manager)


def fetch_show_episodes(sp: spotipy.Spotify, show_id: str) -> list[dict]:
    """
    Retrieve episode objects for a given Spotify show.

    Paginates through the API until `MAX_EPISODES_PER_SHOW` is reached or
    all episodes have been fetched.
    """
    episodes: list[dict] = []
    offset = 0
    limit  = 50  # Spotify max per page

    while True:
        page = sp.show_episodes(show_id, limit=limit, offset=offset, market="US")
        items = page.get("items", [])
        if not items:
            break
        episodes.extend(items)
        logger.info("Show %s: fetched %d episodes (offset=%d)", show_id, len(items), offset)

        if MAX_EPISODES_PER_SHOW and len(episodes) >= MAX_EPISODES_PER_SHOW:
            episodes = episodes[:MAX_EPISODES_PER_SHOW]
            break
        if not page.get("next"):
            break
        offset += limit

    return episodes


def download_preview(preview_url: str) -> bytes | None:
    """
    Download the 30-second MP3 preview clip from Spotify's CDN.
    Returns raw bytes on success, None if unavailable.
    """
    try:
        resp = requests.get(preview_url, timeout=60)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as exc:
        logger.warning("Preview download failed (%s): %s", preview_url, exc)
        return None


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Full ingestion cycle:
    1. Authenticate with Spotify.
    2. For each target show, fetch episode metadata and upload as JSON.
    3. For each episode with a preview_url, download and upload the MP3.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    sp = get_spotify_client()

    for show_id in PHILOSOPHY_SHOW_IDS:
        logger.info("Processing show: %s", show_id)
        episodes = fetch_show_episodes(sp, show_id)

        # --- Upload episode metadata (JSON) ---
        meta_key = f"spotify/metadata/show_{show_id}_{timestamp}.json"
        upload_json(client, episodes, meta_key)

        # --- Download and upload preview audio (MP3) ---
        for ep in episodes:
            ep_id      = ep.get("id", "unknown")
            preview_url = ep.get("preview_url")

            if not preview_url:
                logger.warning("No preview available for episode %s — skipping audio.", ep_id)
                continue

            audio_bytes = download_preview(preview_url)
            if audio_bytes:
                audio_key = f"spotify/audio/show_{show_id}_ep_{ep_id}_{timestamp}.mp3"
                upload_audio(client, audio_bytes, audio_key)

    logger.info("✓ Spotify ingestion complete.")


if __name__ == "__main__":
    run()
