"""
youtube_transcript_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : YouTube (via youtube-transcript-api)
Data Type     : Text (JSON transcripts)
Landing Zone  : s3://landing-zone/youtube_transcripts/raw_json/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
This script uses the youtube-transcript-api to download closed captions
and transcriptions from a list of predefined YouTube video IDs (such as
podcasts, interviews, and archival philosophy footage).

The transcripts are saved as timestamped JSON files into the MinIO landing zone.
It implements idempotent checks: if a transcript already exists in the bucket,
it will be skipped to conserve bandwidth and processing power.
"""

import logging
import os
import sys
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import JSONFormatter

# ── Allow import from the same /ingestion/ package (if needed later) ──────────
sys.path.insert(0, str(Path(__file__).parent))

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("youtube_ingest")

# MinIO connection parameters (read from .env)
MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

# S3 prefix inside the bucket that maps to the landing zone folder
S3_PREFIX = "youtube_transcripts/raw_json"

# YouTube API configuration
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY")


# ─── YouTube Data API Search ──────────────────────────────────────────────────
def get_video_ids_from_search(query: str, target_channels: list[str] = None, max_results: int = 5) -> list[str]:
    """Search for YouTube videos that explicitly have closed captions."""
    if not YOUTUBE_API_KEY:
        logger.error("YOUTUBE_API_KEY is not set in .env")
        return []
        
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "type": "video",
        "videoCaption": "closedCaption",  # CRITICAL: Only retrieve videos with CC
        "videoDuration": "long",          # CRITICAL: Filters to videos > 20 mins
        "maxResults": max_results,
        "q": query,
        "key": YOUTUBE_API_KEY,
    }
    
    logger.info("Searching YouTube: '%s'", query)
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    
    data = resp.json()
    video_ids = []
    for item in data.get("items", []):
        vid_id = item.get("id", {}).get("videoId")
        channel_title = item.get("snippet", {}).get("channelTitle", "")
        
        # Enforce channel name restriction
        if target_channels and vid_id:
            # Case-insensitive substring match
            is_valid_channel = any(tc.lower() in channel_title.lower() for tc in target_channels)
            if not is_valid_channel:
                continue
                
        if vid_id:
            video_ids.append(vid_id)
            
    logger.info("  Found %d videos with captions for query '%s'", len(video_ids), query)
    return video_ids


# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    """
    Build and return a boto3 S3 client configured to talk to the local
    MinIO instance.
    """
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
    else:
        logger.info("Bucket already exists: %s", bucket)


def upload_to_minio(client: boto3.client, payload: bytes, object_key: str) -> None:
    """Upload raw bytes (JSON encoded) to MinIO."""
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json",
    )
    logger.info("Uploaded %d bytes → s3://%s/%s", len(payload), MINIO_BUCKET, object_key)


# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    """
    Orchestrate the full ingestion cycle:
    1. Connect to MinIO and ensure the bucket exists.
    2. Search YouTube API dynamically for target queries.
    3. Check if transcript already exists (Idempotency).
    4. Fetch transcript and upload as JSON.
    """
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Starting dynamic YouTube transcripts ingestion...")
    formatter = JSONFormatter()
    
    # ── Strict Channel Restriction Filter ─────────────────────────────
    # If populated, only videos from these channels will be downloaded.
    # Leave empty [] to accept any channel that matches the query.
    TARGET_CHANNELS = []
    
    search_queries = [
        "The Wild Project Filosofía"
    ]
    
    all_video_ids = set()
    for query in search_queries:
        try:
            ids = get_video_ids_from_search(query, target_channels=TARGET_CHANNELS)
            all_video_ids.update(ids)
        except Exception as e:
            logger.warning("  ✗ Failed search for query '%s': %s", query, e)

    logger.info("Total unique video IDs discovered: %d", len(all_video_ids))

    for video_id in all_video_ids:
        obj_key = f"{S3_PREFIX}/transcript_{video_id}.json"

        # Idempotency check: Skip if already exists
        try:
            client.head_object(Bucket=MINIO_BUCKET, Key=obj_key)
            logger.info("  ✓ Skipped Video ID: %s (already exists in MinIO)", video_id)
            continue
        except Exception:
            pass  # Object does not exist, safe to proceed

        logger.info("  Fetching transcript for Video ID: %s", video_id)
        try:
            # Fetch the transcript (prioritizing Spanish, then English fallback)
            api_client = YouTubeTranscriptApi()
            transcript_list = api_client.fetch(video_id, languages=['es', 'en'])
            
            # Convert list of dicts to a raw JSON string using the formatter
            json_formatted = formatter.format_transcript(transcript_list)
            payload = json_formatted.encode("utf-8")

            upload_to_minio(client, payload, obj_key)
        except Exception as e:
            logger.warning("  ✗ Failed to fetch transcript for %s: %s", video_id, e)

    logger.info("✓ YouTube transcripts ingestion complete.")


if __name__ == "__main__":
    run()
