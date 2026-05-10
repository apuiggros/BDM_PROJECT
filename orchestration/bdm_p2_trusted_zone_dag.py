"""
bdm_p2_trusted_zone_dag.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Project       : BDM P2 — Big Data Management, Deliverable 2
Stage         : Orchestration — Trusted Zone
Description   : Apache Airflow DAG that runs every Trusted-Zone cleaning
                job in `cleaning/structured/` and `cleaning/unstructured/`,
                producing the typed `trusted_*` tables in
                `duckdb/trusted.duckdb` and the boilerplate-stripped
                Gutenberg texts under `s3://landing-zone/gutenberg/cleaned_text/`.

DAG graph
---------
    check_minio_health
        │
        ├─> clean_philosophers
        ├─> clean_wikipedia
        ├─> clean_wikiquote
        ├─> clean_news
        ├─> clean_stack_exchange
        ├─> clean_podcast_episodes
        ├─> clean_philosopher_images
        └─> clean_gutenberg_catalog ──> clean_gutenberg_texts
                                        (texts ALTERs the catalog table)
                │
                ▼
        verify_trusted_zone ──> trusted_zone_complete

The verifier is run as the final task so a green DAG run is also a green
data-quality run.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ─── Defaults ────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "bdm-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

CLEANING_DIR = os.getenv("CLEANING_DIR", "/opt/airflow/cleaning")
SCRIPTS_DIR  = os.getenv("SCRIPTS_DIR",  "/opt/airflow/scripts")

logger = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────
def _run_python(script_relpath: str, base_dir: str = CLEANING_DIR) -> None:
    """
    Run a Python script under `base_dir` using the Airflow worker's own
    interpreter. Streams stdout/stderr into the task log so any Spark or
    DuckDB error is visible from the Airflow UI.
    """
    script_path = os.path.join(base_dir, script_relpath)
    logger.info("Running %s", script_path)
    proc = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.stdout:
        for line in proc.stdout.splitlines():
            logger.info(line)
    if proc.stderr:
        for line in proc.stderr.splitlines():
            logger.info("stderr: %s", line)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{script_relpath} exited with code {proc.returncode}"
        )


def check_minio_health() -> None:
    import boto3
    from botocore.client import Config
    endpoint   = os.getenv("MINIO_ENDPOINT", "minio:9000")
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
    buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
    logger.info("MinIO OK — buckets=%s", buckets)
    if "landing-zone" not in buckets:
        raise RuntimeError("landing-zone bucket missing from MinIO")


# ─── Task callables ───────────────────────────────────────────────────────────
def clean_philosophers():            _run_python("structured/philosophers.py")
def clean_wikipedia():                _run_python("structured/wikipedia.py")
def clean_wikiquote():                _run_python("structured/wikiquote.py")
def clean_news():                     _run_python("structured/news.py")
def clean_stack_exchange():           _run_python("structured/stack_exchange.py")
def clean_gutenberg_catalog():        _run_python("structured/gutenberg_catalog.py")
def clean_gutenberg_texts():          _run_python("unstructured/gutenberg_texts.py")
def clean_podcast_episodes():         _run_python("unstructured/podcast_episodes.py")
def clean_philosopher_images():       _run_python("unstructured/philosopher_images.py")


def verify_trusted_zone():
    """Run the trusted-zone verifier as the gate for `trusted_zone_complete`."""
    _run_python("verify_trusted_zone.py", base_dir=SCRIPTS_DIR)


# ─── DAG ─────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="bdm_p2_trusted_zone",
    description=(
        "BDM P2 — Trusted-Zone cleaning of every batch source: structured "
        "tables to DuckDB, cleaned Gutenberg texts to MinIO, plus a "
        "data-quality verifier as the final gate."
    ),
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # manual trigger; the cold-path P1 DAG is daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    # Each cleaning task spawns its own local-mode Spark JVM. Running too many
    # concurrently starves the Airflow scheduler heartbeat in this single-VM
    # docker stack, so cap concurrent tasks at 3.
    max_active_tasks=3,
    tags=["bdm", "p2", "trusted-zone", "spark", "duckdb"],
) as dag:

    health_check = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
        doc_md="Confirms MinIO is reachable and the landing-zone bucket exists.",
    )

    structured_tasks = [
        PythonOperator(
            task_id="clean_philosophers",
            python_callable=clean_philosophers,
            doc_md="philosophers-api → trusted_philosophers (5 rows; BC dates → signed ints).",
        ),
        PythonOperator(
            task_id="clean_wikipedia",
            python_callable=clean_wikipedia,
            doc_md="Wikipedia REST summary → trusted_wikipedia (one row per figure).",
        ),
        PythonOperator(
            task_id="clean_wikiquote",
            python_callable=clean_wikiquote,
            doc_md="Wikiquote HTML → trusted_wikiquote_quotes (by_figure + about_figure).",
        ),
        PythonOperator(
            task_id="clean_news",
            python_callable=clean_news,
            doc_md="GNews snapshots → trusted_news_articles (deduped on article id).",
        ),
        PythonOperator(
            task_id="clean_stack_exchange",
            python_callable=clean_stack_exchange,
            doc_md="Philosophy SE → trusted_se_questions + trusted_se_answers (HTML stripped).",
        ),
        PythonOperator(
            task_id="clean_podcast_episodes",
            python_callable=clean_podcast_episodes,
            doc_md="Podcast audio listing → trusted_podcast_episodes (manifest only).",
        ),
        PythonOperator(
            task_id="clean_philosopher_images",
            python_callable=clean_philosopher_images,
            doc_md="Philosopher images → trusted_philosopher_images (with is_primary).",
        ),
    ]

    gutenberg_catalog_task = PythonOperator(
        task_id="clean_gutenberg_catalog",
        python_callable=clean_gutenberg_catalog,
        doc_md="Gutendex catalog → trusted_gutenberg_books (with has_local_text).",
    )

    gutenberg_texts_task = PythonOperator(
        task_id="clean_gutenberg_texts",
        python_callable=clean_gutenberg_texts,
        doc_md=(
            "Strips PG header/footer from each .txt, uploads cleaned bytes to "
            "MinIO, and ALTERs trusted_gutenberg_books with cleaned_text_key + "
            "boilerplate_stripped. Must run AFTER clean_gutenberg_catalog."
        ),
    )

    verify_task = PythonOperator(
        task_id="verify_trusted_zone",
        python_callable=verify_trusted_zone,
        doc_md="Runs scripts/verify_trusted_zone.py; fails the DAG if any check fails.",
    )

    pipeline_done = EmptyOperator(task_id="trusted_zone_complete")

    # Dependencies
    health_check >> structured_tasks
    health_check >> gutenberg_catalog_task >> gutenberg_texts_task

    (structured_tasks + [gutenberg_texts_task]) >> verify_task >> pipeline_done
