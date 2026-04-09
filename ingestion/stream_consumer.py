import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from kafka import KafkaConsumer
from dotenv import load_dotenv

# ─── Configuration ────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] consumer — %(message)s",
)
logger = logging.getLogger("stream_consumer")

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME   = "character-mentions"
GROUP_ID     = "landing-zone-capture"

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "landing-zone")

S3_STREAM_PREFIX = "podcasts/raw_stream"

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

def ensure_bucket(client, bucket):
    existing = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)

# ─── Main Logic ───────────────────────────────────────────────────────────────
def run_consumer():
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)

    logger.info("Connecting to Kafka broker at %s for topic '%s'...", KAFKA_BROKER, TOPIC_NAME)
    
    consumer = None
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            break
        except Exception:
            logger.warning("  ... Kafka not ready yet, retrying (%d/10)", i+1)
            time.sleep(5)

    if not consumer:
        logger.error("Could not connect to Kafka. Aborting.")
        return

    logger.info("Consumer started. Listening for messages...")

    # We will accumulate messages and flush them periodically to S3
    buffer = []
    MAX_BUFFER_SIZE = 10
    
    try:
        for message in consumer:
            mention = message.value
            logger.info("  << Received mention for: %s", mention["character_name"])
            buffer.append(mention)
            
            if len(buffer) >= MAX_BUFFER_SIZE:
                # Flush to S3
                timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                obj_key = f"{S3_STREAM_PREFIX}/mentions_{timestamp_str}.json"
                
                payload = json.dumps(buffer, indent=2).encode('utf-8')
                client.put_object(
                    Bucket=MINIO_BUCKET,
                    Key=obj_key,
                    Body=payload,
                    ContentType="application/json"
                )
                logger.info("  ✓ Flushed %d messages to s3://%s/%s", len(buffer), MINIO_BUCKET, obj_key)
                buffer = []
                
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()
