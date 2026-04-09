import json
import logging
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from character_registry import TARGET_FIGURES

# ─── Configuration ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer — %(message)s",
)
logger = logging.getLogger("stream_producer")

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME   = "character-mentions"

# ─── Simulated Mention Logic ──────────────────────────────────────────────────
MENTION_TEMPLATES = [
    "Just read a fascinating study on {name}'s influence on modern AI.",
    "Is {name} still relevant in the age of neural networks?",
    "A new biography of {name} just hit the shelves!",
    "Comparing {name}'s ethics with today's social media algorithms.",
    "Debating {name} in the comments section again...",
    "Why '{name}' is trending on social media today."
]

def generate_mention() -> dict:
    figure = random.choice(TARGET_FIGURES)
    template = random.choice(MENTION_TEMPLATES)
    
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "character_name": figure["api_name"],
        "domain": figure["domain"],
        "message": template.format(name=figure["api_name"]),
        "sentiment_score": round(random.uniform(-1, 1), 2),
        "source": random.choice(["Twitter", "Reddit", "NewsTicker", "Forum"])
    }

# ─── Main Logic ───────────────────────────────────────────────────────────────
def run_producer():
    logger.info("Connecting to Kafka broker at %s...", KAFKA_BROKER)
    
    # Retry logic for Kafka connection (it might take a few seconds to warm up)
    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            break
        except Exception:
            logger.warning("  ... Kafka not ready yet, retrying (%d/10)", i+1)
            time.sleep(5)
            
    if not producer:
        logger.error("Could not connect to Kafka. Aborting.")
        return

    logger.info("Producer started. Sending simulated mentions to topic: %s", TOPIC_NAME)
    
    try:
        while True:
            mention = generate_mention()
            producer.send(TOPIC_NAME, value=mention)
            logger.info("  >> Sent mention for: %s", mention["character_name"])
            
            # Sleep for a random interval to simulate real-time traffic
            time.sleep(random.uniform(1.0, 5.0))
    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
