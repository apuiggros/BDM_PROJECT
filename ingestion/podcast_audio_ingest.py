"""
podcast_audio_ingest.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Source        : iTunes API + Podcast RSS Feeds
Data Type     : Unstructured (Audio .mp3) + Semi-structured (Metadata JSON)
Landing Zone  : s3://landing-zone/podcasts/raw_audio/ & metadata/
Pipeline Stage: P1 — Cold-Path Batch Ingestion

Description
-----------
This script uses a Discovery-Based approach to find unstructured audio.
Instead of hardcoding specific channels, it searches the iTunes API for 
broad topics (e.g., "philosophy interviews"). It discovers the top-ranking 
podcast channels for those topics, extracts their RSS feeds, and downloads 
their latest episodes into the Landing Zone.
"""

import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
import re

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("podcast_ingest")

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "landing-zone")

S3_AUDIO_PREFIX    = "podcasts/raw_audio"
S3_METADATA_PREFIX = "podcasts/metadata"

# ─── Discovery Targets ────────────────────────────────────────────────────────
# Broad topics to search for, rather than specific channels
TARGET_TOPICS = [
    "podcast"
]

PODCASTS_PER_TOPIC = 10    # How many distinct channels to discover per topic
MAX_EPISODES_PER_FEED = 2   # Limit downloads per channel to save disk space

# ─── iTunes API & RSS Parsing ─────────────────────────────────────────────────
def discover_podcasts(topic: str, limit: int) -> list[dict]:
    """Queries iTunes for broad topics and returns the top podcast feeds."""
    logger.info("Discovering podcasts for topic: '%s'", topic)
    url = "https://itunes.apple.com/search"
    params = {
        "term": topic,
        "media": "podcast",
        "limit": limit
    }
    
    discovered = []
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        for result in data.get("results", []):
            if result.get("feedUrl"):
                discovered.append({
                    "podcast_id": str(result.get("collectionId")),
                    "podcast_name": result.get("collectionName"),
                    "author": result.get("artistName"),
                    "rss_url": result.get("feedUrl"),
                    "topic_source": topic
                })
        return discovered
    except Exception as e:
        logger.warning("  ✗ Failed to query iTunes API for '%s': %s", topic, e)
        return []

def fetch_latest_episodes(rss_url: str, limit: int) -> list[dict]:
    """Downloads the RSS XML and extracts the latest audio episodes."""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        resp = requests.get(rss_url, headers=headers, timeout=15)
        resp.raise_for_status()
        
        root = ET.fromstring(resp.content)
        episodes = []
        
        for item in root.findall('.//item'):
            if len(episodes) >= limit:
                break
                
            title = item.findtext('title') or "Unknown Title"
            pub_date = item.findtext('pubDate') or ""
            
            enclosure = item.find('enclosure')
            if enclosure is not None and enclosure.get('url'):
                audio_url = enclosure.get('url')
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', title)[:30]
                
                episodes.append({
                    "episode_id": safe_id,
                    "title": title,
                    "published_at": pub_date,
                    "audio_url": audio_url
                })
                
        return episodes
    except Exception as e:
        logger.warning("  ✗ Failed to parse RSS feed %s: %s", rss_url, e)
        return []

# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ensure_bucket(client: boto3.client, bucket: str) -> None:
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

# ─── Main Entry Point ─────────────────────────────────────────────────────────
def run() -> None:
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Starting Discovery-Based Podcast Audio ingestion...")
    
    # Track unique podcast IDs to avoid downloading the same channel twice 
    # if it ranks for multiple topics
    seen_podcast_ids = set()
    
    for topic in TARGET_TOPICS:
        discovered_channels = discover_podcasts(topic, PODCASTS_PER_TOPIC)
        
        for podcast_info in discovered_channels:
            pid = podcast_info["podcast_id"]
            if pid in seen_podcast_ids:
                continue
            seen_podcast_ids.add(pid)
            
            logger.info("  ✓ Investigating Channel: '%s' (via topic: %s)", 
                        podcast_info["podcast_name"], topic)
                        
            episodes = fetch_latest_episodes(podcast_info["rss_url"], MAX_EPISODES_PER_FEED)
            
            for ep in episodes:
                podcast_slug = re.sub(r'[^a-zA-Z0-9]', '_', podcast_info["podcast_name"]).lower()
                ep_id = ep["episode_id"]
                
                audio_ext = ".m4a" if ".m4a" in ep["audio_url"].lower() else ".mp3"
                audio_key = f"{S3_AUDIO_PREFIX}/{podcast_slug}/ep_{ep_id}{audio_ext}"
                meta_key  = f"{S3_METADATA_PREFIX}/{podcast_slug}/ep_{ep_id}_meta.json"
                
                # Idempotency check
                try:
                    client.head_object(Bucket=MINIO_BUCKET, Key=audio_key)
                    logger.info("    ✓ Skipped: %s (Already exists in MinIO)", ep["title"])
                    continue
                except Exception:
                    pass  
                    
                logger.info("    Downloading audio: [%s]", ep["title"])
                try:
                    headers = {"User-Agent": "Mozilla/5.0"}
                    with requests.get(ep["audio_url"], headers=headers, stream=True, timeout=30) as r:
                        r.raise_for_status()
                        client.upload_fileobj(
                            r.raw, 
                            MINIO_BUCKET, 
                            audio_key, 
                            ExtraArgs={'ContentType': 'audio/mpeg'}
                        )
                    logger.info("      ✓ Audio uploaded to %s", audio_key)
                except Exception as e:
                    logger.error("      ✗ Audio download failed: %s", e)
                    continue 
                    
                # Upload JSON Metadata Envelope
                envelope = {
                    "_metadata": {
                        "topic_source": podcast_info["topic_source"],
                        "podcast_id": podcast_info["podcast_id"],
                        "podcast_name": podcast_info["podcast_name"],
                        "author": podcast_info["author"],
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    },
                    "episode": ep
                }
                
                payload = json.dumps(envelope, ensure_ascii=False, indent=2).encode("utf-8")
                client.put_object(Bucket=MINIO_BUCKET, Key=meta_key, Body=payload, ContentType="application/json")
            
    logger.info("✓ Podcast Audio ingestion complete.")

if __name__ == "__main__":
    run()