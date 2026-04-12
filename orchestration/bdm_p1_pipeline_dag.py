"""
bdm_p1_pipeline_dag.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Project       : BDM P1 — Big Data Management, Deliverable 1
Stage         : Orchestration
Description   : Apache Airflow DAG that schedules and sequences the four
                cold-path batch ingestion scripts.  The pipeline runs daily
                and ingests data from all four sources in parallel where
                possible, with MinIO availability verified first.

DAG Graph
---------
  check_minio_health
        │
        ├──> ingest_philosophers_api
        ├──> ingest_gutenberg
        ├──> ingest_podcast_audio
        ├──> ingest_wikipedia_biographies
        ├──> ingest_news_api
        ├──> ingest_wikiquote
        └──> ingest_philosophy_se
                │
                └──> convert_raw_to_delta
                        │
                        └──> pipeline_complete
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ─── Default DAG Arguments ────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "bdm-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Path to the ingestion scripts inside the Airflow container.
# The /opt/airflow/ingestion volume mount must be added to docker-compose.yml
INGESTION_DIR = os.getenv("INGESTION_DIR", "/opt/airflow/ingestion")

logger = logging.getLogger(__name__)


# ─── Task Functions ───────────────────────────────────────────────────────────
def check_minio_health() -> None:
    """
    Verify that MinIO is reachable before starting any ingestion.
    Uses boto3 to list buckets; raises an exception if the service is down
    which will cause the DAG run to fail fast rather than have partial loads.
    """
    import boto3
    from botocore.client import Config

    endpoint   = os.getenv("MINIO_ENDPOINT", "minio:9000")  # service name inside Docker network
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "password")

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    buckets = client.list_buckets()
    logger.info("MinIO health OK — buckets: %s", [b["Name"] for b in buckets.get("Buckets", [])])


def _run_ingestion_script(script_name: str) -> None:
    """
    Helper that executes a named ingestion script using the same Python
    interpreter running Airflow (so site-packages are shared).
    Raises CalledProcessError on non-zero exit so Airflow marks the task failed.
    """
    script_path = os.path.join(INGESTION_DIR, script_name)
    logger.info("Running ingestion script: %s", script_path)
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        check=True,
    )
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)


def run_philosophers() -> None:
    _run_ingestion_script("philosophers_ingest.py")


def run_gutenberg() -> None:
    _run_ingestion_script("gutenberg_ingest.py")


def run_podcasts() -> None:
    _run_ingestion_script("podcast_audio_ingest.py")

def run_news() -> None:
    _run_ingestion_script("news_ingest.py")


def run_wikipedia() -> None:
    _run_ingestion_script("wikipedia_ingest.py")


def run_wikiquote() -> None:
    _run_ingestion_script("wikiquote_ingest.py")


def run_philosophy_se() -> None:
    _run_ingestion_script("philosophyse_ingest.py")


def run_delta_conversion() -> None:
    _run_ingestion_script("metadata_to_delta.py")


# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="bdm_p1_cold_path_ingestion",
    description=(
        "BDM P1 — Daily cold-path batch ingestion and Delta Lake Lakehouse "
        "transformation into the MinIO landing zone."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",           # Run once per day at midnight UTC
    start_date=days_ago(1),
    catchup=False,                        # Do not backfill historical runs
    max_active_runs=1,                    # Prevent overlapping executions
    tags=["bdm", "p1", "ingestion", "cold-path", "delta-lake", "lakehouse"],
) as dag:

    # ── Health check (upstream gate) ──────────────────────────────────────────
    health_check = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
        doc_md=(
            "Validates that MinIO is reachable by listing buckets via boto3. "
            "All downstream ingestion tasks depend on this gate."
        ),
    )

    # ── Data Source Tasks (run in parallel once MinIO is healthy) ─────────────
    ingest_philosophers = PythonOperator(
        task_id="ingest_philosophers_api",
        python_callable=run_philosophers,
        doc_md="Fetches philosopher records & concepts from philosophersapi.com and stores JSON in MinIO.",
    )

    ingest_gutenberg = PythonOperator(
        task_id="ingest_gutenberg",
        python_callable=run_gutenberg,
        doc_md="Downloads canonical historical texts in plain-text format from Project Gutenberg via Gutendex API.",
    )

    ingest_podcasts = PythonOperator(
        task_id="ingest_podcast_audio",
        python_callable=run_podcasts,
        doc_md="Discovers and downloads high-quality podcast audio related to academic/historical topics via iTunes RSS feeds.",
    )

    ingest_news = PythonOperator(
        task_id="ingest_news_api",
        python_callable=run_news,
        doc_md="Fetches daily news snapshots based on specific philosophical/technology queries via GNews API.",
    )

    ingest_wikipedia = PythonOperator(
        task_id="ingest_wikipedia_biographies",
        python_callable=run_wikipedia,
        doc_md="Fetches structured biography summaries from the Wikipedia REST API for all target figures.",
    )

    ingest_wikiquote = PythonOperator(
        task_id="ingest_wikiquote",
        python_callable=run_wikiquote,
        doc_md="Fetches verified quotes for all target figures from the Wikiquote MediaWiki API.",
    )

    ingest_philosophy_se = PythonOperator(
        task_id="ingest_philosophy_se",
        python_callable=run_philosophy_se,
        doc_md="Downloads top-voted philosophy Q&A pairs from the Philosophy Stack Exchange API.",
    )

    # ── Delta Lake Transformation (Runs after batch sources are captured) ────
    ingest_delta_lake = PythonOperator(
        task_id="convert_raw_to_delta",
        python_callable=run_delta_conversion,
        doc_md="Transforms raw JSON metadata catalogs into structured Delta Tables for the Data Lakehouse layer.",
    )

    # ── Summary task (downstream gate) ───────────────────────────────────────
    pipeline_done = EmptyOperator(
        task_id="pipeline_complete",
        doc_md="All ingestion and Delta Lake conversions have finished successfully.",
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    # health_check → [parallel ingestion tasks] → delta_conversion → pipeline_done
    health_check >> [
        ingest_philosophers,
        ingest_gutenberg,
        ingest_podcasts,
        ingest_news,
        ingest_wikipedia,
        ingest_wikiquote,
        ingest_philosophy_se,
    ]
    [
        ingest_philosophers,
        ingest_gutenberg,
        ingest_podcasts,
        ingest_news,
        ingest_wikipedia,
        ingest_wikiquote,
        ingest_philosophy_se,
    ] >> ingest_delta_lake >> pipeline_done

