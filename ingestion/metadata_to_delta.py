import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.client import Config
from deltalake import write_deltalake
from dotenv import load_dotenv

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] delta — %(message)s",
)
logger = logging.getLogger("metadata_to_delta")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")

# Delta Storage Location (using S3 protocol for deltalake library)
# Note: delta-rs needs environment variables for S3 access
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_ENDPOINT_URL"] = f"http://{MINIO_ENDPOINT}"
os.environ["AWS_ALLOW_HTTP"] = "true"
os.environ["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true"  # Required for MinIO/local storage
os.environ["AWS_REGION"] = "us-east-1"

DELTA_ROOT = f"s3://{MINIO_BUCKET}/bronze_tables"

# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

# ─── Delta Conversion Logic ───────────────────────────────────────────────────
def convert_philosophers_to_delta(client):
    """Converts the philosophers catalog JSON to a Delta table."""
    logger.info("Converting Philosophers Catalog to Delta...")
    obj_key = "philosophers_api/raw_json/philosophy/philosophers_catalog.json"
    
    try:
        resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj_key)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        
        # Flatten and clean
        df = pd.json_normalize(data)
        
        # Save as Delta
        table_path = f"{DELTA_ROOT}/philosophers"
        write_deltalake(table_path, df, mode="overwrite")
        logger.info("  ✓ Delta table updated: %s", table_path)
    except Exception as e:
        logger.error("  ✗ Failed to convert philosophers: %s", e)

def convert_news_to_delta(client):
    """Converts the latest news snapshot to a Delta table (appended)."""
    logger.info("Converting News Snapshots to Delta...")
    prefix = "news_api/raw_json/"
    
    try:
        # Get latest snapshot
        objects = client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get('Contents', [])
        if not objects:
            logger.warning("  No news snapshots found.")
            return
            
        latest_obj = sorted(objects, key=lambda x: x['LastModified'])[-1]
        obj_key = latest_obj['Key']
        logger.info("  Processing latest snapshot: %s", obj_key)
        
        resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj_key)
        data = json.loads(resp['Body'].read().decode('utf-8'))
        
        df = pd.json_normalize(data)
        df['ingested_at'] = datetime.now(timezone.utc).isoformat()
        
        table_path = f"{DELTA_ROOT}/news_headlines"
        write_deltalake(table_path, df, mode="append")
        logger.info("  ✓ Delta table updated: %s", table_path)
    except Exception as e:
        logger.error("  ✗ Failed to convert news: %s", e)

def convert_wikipedia_to_delta(client):
    """Aggregates all character Wikipedia summaries into a single Delta table."""
    logger.info("Converting Wikipedia Summaries to Delta...")
    prefix = "wikipedia/raw_json/"
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        all_data = []
        
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('_wikipedia.json'):
                    resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj['Key'])
                    item = json.loads(resp['Body'].read().decode('utf-8'))
                    # Extract domain from path: wikipedia/raw_json/{domain}/...
                    parts = obj['Key'].split('/')
                    item['_domain'] = parts[2]
                    all_data.append(item)
        
        if all_data:
            df = pd.json_normalize(all_data)
            table_path = f"{DELTA_ROOT}/wikipedia_biographies"
            write_deltalake(table_path, df, mode="overwrite")
            logger.info("  ✓ Delta table updated: %s (%d records)", table_path, len(all_data))
    except Exception as e:
        logger.error("  ✗ Failed to convert wikipedia: %s", e)

def convert_gutenberg_metadata_to_delta(client):
    """Aggregates all Gutenberg catalogs into a single Delta table."""
    logger.info("Converting Gutenberg Library to Delta...")
    prefix = "gutenberg/raw_text/"
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        all_data = []
        
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('_catalog.json'):
                    resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj['Key'])
                    item = json.loads(resp['Body'].read().decode('utf-8'))
                    all_data.append(item)
        
        if all_data:
            df = pd.json_normalize(all_data)
            table_path = f"{DELTA_ROOT}/gutenberg_library"
            write_deltalake(table_path, df, mode="overwrite")
            logger.info("  ✓ Delta table updated: %s (%d records)", table_path, len(all_data))
    except Exception as e:
        logger.error("  ✗ Failed to convert gutenberg: %s", e)

def convert_podcast_metadata_to_delta(client):
    """Aggregates all podcast episode metadata into a single Delta table."""
    logger.info("Converting Podcast Episodes to Delta...")
    prefix = "podcasts/metadata/"
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        all_data = []
        
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('_meta.json'):
                    resp = client.get_object(Bucket=MINIO_BUCKET, Key=obj['Key'])
                    item = json.loads(resp['Body'].read().decode('utf-8'))
                    all_data.append(item)
        
        if all_data:
            df = pd.json_normalize(all_data)
            table_path = f"{DELTA_ROOT}/podcast_episodes"
            write_deltalake(table_path, df, mode="overwrite")
            logger.info("  ✓ Delta table updated: %s (%d records)", table_path, len(all_data))
    except Exception as e:
        logger.error("  ✗ Failed to convert podcasts: %s", e)

# ─── Main Execution ───────────────────────────────────────────────────────────
def run():
    client = get_minio_client()
    convert_philosophers_to_delta(client)
    convert_news_to_delta(client)
    convert_wikipedia_to_delta(client)
    convert_gutenberg_metadata_to_delta(client)
    convert_podcast_metadata_to_delta(client)
    logger.info("✓ Delta Lake conversion complete.")

if __name__ == "__main__":
    run()
