"""
philosophers.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Cleans and standardizes philosopher data. Writes to trusted zone. 
"""

import io
import json
import logging
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import re


# load environment
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",)
logger = logging.getLogger("philosophers_trusted_zone")

#minIO config
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")

TRUSTED_BUCKET = "trusted-zone"

RAW_JSON_KEY = (
    "philosophers_api/raw_json/philosophy/philosophers_catalog.json"
)

TRUSTED_PARQUET_KEY = (
    "philosophers_api/trusted_json/philosophy/philosophers_cleaned.parquet"
)

# ─── Ensure Bucket ───────────────────────────────────────────────
def ensure_bucket(client, bucket: str):
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]

    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
        logger.info("Created bucket: %s", bucket)

# ─── MinIO / S3 Client ────────────────────────────────────────────────────────
def get_minio_client() -> boto3.client:
    """
    Build and return a boto3 S3 client configured to talk to the local
    MinIO instance.  The `path` addressing style is required for MinIO.
    """
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",          # MinIO ignores this, but boto3 needs it
    )

# ─── Read Raw Data ────────────────────────────────────────────────────────
def fetch_raw_json(client) -> list[dict]:
    logger.info("Fetching raw JSON from landing zone...")

    response = client.get_object(
        Bucket=MINIO_BUCKET,
        Key=RAW_JSON_KEY,
    )

    raw_bytes = response["Body"].read()
    raw_data = json.loads(raw_bytes.decode("utf-8"))

    logger.info("Loaded %d philosopher records", len(raw_data))

    return raw_data

# ─── Cleaning Logic ──────────────────────────────────────────
BASE_IMAGE_URL = os.getenv("IMAGE_BASE_URL", "https://your-cdn.com")



def normalize_year(value):
    if value is None or pd.isna(value):
        return np.nan

    value = str(value).strip().upper()

    if "BC" in value:
        num = re.findall(r"\d+", value)
        return -int(num[0]) if num else np.nan

    if "AD" in value:
        num = re.findall(r"\d+", value)
        return int(num[0]) if num else np.nan

    try:
        return int(re.findall(r"\d+", value)[0])
    except:
        return np.nan


def clean_philosophers(records: list[dict]) -> pd.DataFrame:
    logger.info("Starting full cleaning pipeline...")

    df = pd.DataFrame([
        {
            "name": r.get("name"),
            "school": r.get("school"),
            "born": r.get("birthYear"),
            "died": r.get("deathYear"),

            # optional field (kept but normalized later)
            "description": r.get("topicalDescription"),

            "nationality": r.get("nationality"),
            "wiki_title": r.get("wikiTitle"),

            # flatten + deduplicate links
            "sep_link": r.get("_academic_links", {}).get("stanford_sep"),
            "iep_link": r.get("_academic_links", {}).get("internet_iep"),

            "domain": r.get("_registry", {}).get("domain"),
            "slug": r.get("_registry", {}).get("gutenberg_author_slug"),

            # categorical field
            "interests": r.get("interests"),

            # images (may be relative path)
            "image": r.get("image"),
        }
        for r in records
    ])

    # ─────────────────────────────────────────────
    # 1. Missing values handling
    # ─────────────────────────────────────────────
    df = df.dropna(subset=["name"])  # name is mandatory

    df["description"] = df["description"].fillna("No description available")

    # ─────────────────────────────────────────────
    # 2. Standardize years (BC → negative)
    # ─────────────────────────────────────────────
    df["born"] = pd.to_numeric(df["born"], errors="coerce")
    df["died"] = pd.to_numeric(df["died"], errors="coerce")

    # ─────────────────────────────────────────────
    # 3. Lifespan (derived field)
    # ─────────────────────────────────────────────
    df["lifespan"] = np.where(
        df["born"].notna() & df["died"].notna(),
        df["died"] - df["born"],
        np.nan
    )

    # ─────────────────────────────────────────────
    # 4. Normalize categorical fields
    # ─────────────────────────────────────────────

    # interests: string → list
    def split_interests(x):
        if pd.isna(x):
            return []
        return [i.strip().lower() for i in str(x).split(",")]

    df["interests"] = df["interests"].apply(split_interests)

    # school normalization
    df["school"] = df["school"].str.strip().str.lower()

    # ─────────────────────────────────────────────
    # 5. Deduplication (better key than name alone)
    # ─────────────────────────────────────────────
    df = df.drop_duplicates(subset=["slug"])

    # ─────────────────────────────────────────────
    # 6. Link cleanup (remove duplicates / nulls)
    # ─────────────────────────────────────────────
    df["sep_link"] = df["sep_link"].fillna(df["iep_link"])
    df = df.drop(columns=["iep_link"])

    # ─────────────────────────────────────────────
    # 7. Image path normalization
    # ─────────────────────────────────────────────
    def fix_image_path(path):
        if pd.isna(path):
            return None
        if path.startswith("http"):
            return path
        return f"{BASE_IMAGE_URL}/{path.lstrip('/')}"

    df["image"] = df["image"].apply(fix_image_path)

    # ─────────────────────────────────────────────
    # 8. Final cleanup
    # ─────────────────────────────────────────────
    df = df.replace(["", "unknown", "N/A"], pd.NA)

    df = df.sort_values("name")

    # ─────────────────────────────────────────────
    # 9. Validation checks (VERY important in DAGs)
    # ─────────────────────────────────────────────
    assert df["name"].notna().all(), "Missing names detected"
    assert len(df) > 0, "Dataset became empty after cleaning"

    logger.info("Final cleaned shape: %s", df.shape)

    return df

# ─── Write Trusted Data ────────────────────────────────────────────────────
def upload_trusted_parquet(client, df: pd.DataFrame):
    logger.info("Uploading trusted parquet...")

    parquet_buffer = io.BytesIO()

    df.to_parquet(
        parquet_buffer,
        engine="pyarrow",
        index=False,
    )

    parquet_buffer.seek(0)

    client.put_object(
        Bucket=TRUSTED_BUCKET,
        Key=TRUSTED_PARQUET_KEY,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    logger.info(
        "Uploaded trusted parquet → s3://%s/%s",
        TRUSTED_BUCKET,
        TRUSTED_PARQUET_KEY,
    )

# ─── Main ──────────────────────────────────────────────────────────────────
def run():
    client = get_minio_client()
    
    ensure_bucket(client, TRUSTED_BUCKET)

    raw_records = fetch_raw_json(client)

    cleaned_df = clean_philosophers(raw_records)

    upload_trusted_parquet(client, cleaned_df)

    logger.info("Trusted philosophers complete")


if __name__ == "__main__":
    run()