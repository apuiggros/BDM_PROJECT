# 🏛️ Historical Conversational AI — Data Lakehouse Pipeline
### Big Data Management (BDM) — P1: Cold-Path Ingestion & Bronze Landing Zone

---

## 📋 Table of Contents

1. [End Goal & Motivation](#-end-goal--motivation)
2. [Architecture Overview](#-architecture-overview)
3. [Infrastructure Deep Dive](#-infrastructure-deep-dive)
4. [Data Sources & Ingestion Scripts](#-data-sources--ingestion-scripts)
5. [DAG Orchestration](#-dag-orchestration)
6. [Data Organization in the Landing Zone](#-data-organization-in-the-landing-zone)
7. [Project Structure](#-project-structure)
8. [Requirements & Pre-requisites](#-requirements--pre-requisites)
9. [Step-by-Step Setup Tutorial](#-step-by-step-setup-tutorial)
10. [Running the Pipeline](#-running-the-pipeline)
11. [Testing Scripts Locally](#-testing-scripts-locally)
12. [Key Engineering Decisions](#-key-engineering-decisions)

---

## 🎯 End Goal & Motivation

The ultimate objective of this project is to power a **multimodal AI system capable of generating realistic, podcast-style interviews with famous historical figures** — starting with classical philosophers.

The challenge is that such an AI needs to answer a deceptively complex question: *"How would Immanuel Kant react to today's news on artificial intelligence?"*. To answer this credibly, the AI needs at least four distinct categories of raw knowledge:

| Pillar | What the AI Learns | Where We Get It |
|---|---|---|
| **Core Biographical Facts** | Names, schools of thought, dates, concepts, portraits | Philosophers REST API |
| **Authoritative Writings** | The actual vocabulary, reasoning style, and syntax of the philosopher | Project Gutenberg (Public Domain books) |
| **Conversational Dynamics** | How an interview or debate flows — tone, pacing, turn-taking | YouTube Transcript API |
| **Current Events Awareness** | Top trending daily news so the historical figure can "react" to the modern world | GNews API (Top Headlines) |

This P1 deliverable focuses on **Phase 1**: Building and automating a fully containerized, self-healing Bronze Layer pipeline to **extract and store all this raw data at scale**, creating the foundation from which the future Trusted Zone (data cleansing) and Exploitation Zone (AI model training) can be built.

---

## 🏗️ Architecture Overview

The pipeline follows a **Registry-Driven, Micro-Ingestion architecture** organized around a central `philosopher_registry.py` — a single source of truth for all target entities. Every ingestion script reads from this registry, ensuring that adding a new philosopher to the pipeline only requires editing one file.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES (External)                             │
│                                                                             │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐        │
│  │ philosophersapi  │   │  gutendex.com    │   │  YouTube Data    │        │
│  │ .com REST API    │   │  (Gutenberg API) │   │  API v3 +        │        │
│  │                  │   │                  │   │  Transcript API  │        │
│  │ • 114 records    │   │ • Public domain  │   │                  │        │
│  │ • JSON metadata  │   │ • .txt books     │   │ • CC Search      │        │
│  │ • Images (JPEG)  │   │ • Author catalog │   │ • JSON Transcripts│       │
│  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘        │
│           │                      │                      │                   │
│           │             ┌────────┴──────┐               │                   │
│           │             │  gnews.io     │               │                   │
│           │             │  Top Headlines│               │                   │
│           │             └────────┬──────┘               │                   │
└───────────┼─────────────────────┼──────────────────────┼───────────────────┘
            │                     │                      │
┌───────────▼─────────────────────▼──────────────────────▼───────────────────┐
│                    ORCHESTRATION LAYER (Docker Container)                   │
│                                                                             │
│                         Apache Airflow 2.9.0                                │
│                      (LocalExecutor | PostgreSQL Backend)                   │
│                                                                             │
│  DAG: bdm_p1_cold_path_ingestion  [schedule: @daily]                        │
│                                                                             │
│                    ┌─────────────────────┐                                  │
│                    │  check_minio_health │  ← Health gate (fail-fast)       │
│                    └──────────┬──────────┘                                  │
│           ┌───────────────────┼───────────────────┐                         │
│           ▼                   ▼                   ▼              ▼          │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐  ┌─────────┐ │
│  │ ingest_        │  │ ingest_        │  │ ingest_youtube_  │  │ ingest_ │ │
│  │ philosophers   │  │ gutenberg      │  │ transcripts      │  │ news_api│ │
│  │ _api           │  │                │  │                  │  │         │ │
│  └───────┬────────┘  └───────┬────────┘  └────────┬─────────┘  └────┬────┘ │
│          └───────────────────┴───────────────────┬─┘                │       │
│                                                  ▼                  │       │
│                                       ┌──────────────────┐          │       │
│                                       │ pipeline_complete│ ◄────────┘       │
│                                       └──────────────────┘                  │
└──────────────────────────────────────────────────────────────────────────── ┘
            │                     │                      │              │
┌───────────▼─────────────────────▼──────────────────────▼──────────────▼────┐
│                     STORAGE LAYER — Bronze Landing Zone                     │
│                                                                             │
│                  MinIO (S3-compatible) — Local Object Store                 │
│                                                                             │
│  Bucket: landing-zone                                                       │
│  ├── philosophers_api/                                                      │
│  │   ├── raw_json/          ← philosophers_catalog.json                     │
│  │   └── raw_images/        ← {slug}/{category}/{image_key}.jpg             │
│  ├── gutenberg/                                                             │
│  │   └── raw_text/          ← {author}_{book_id}_{title}.txt                │
│  ├── youtube_transcripts/                                                   │
│  │   └── raw_json/          ← transcript_{video_id}.json                    │
│  └── news_api/                                                              │
│      └── raw_json/          ← news_snapshot_{YYYYMMDD}.json                 │
│                                                                             │
│  Host volume bind: ./landing_zone/ → /data (inside MinIO container)         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🐳 Infrastructure Deep Dive

The entire infrastructure is defined in `docker-compose.yml` and spins up **5 containers**:

### `minio` — The Landing Zone
- **Image:** `minio/minio:latest`
- **Role:** S3-compatible local object store that serves as the Bronze Layer of the Data Lakehouse.
- **Ports:** `9000` (S3 API) and `9001` (Web Console UI).
- **Volume Bind:** Your local `./landing_zone/` folder is mounted directly into the container at `/data`. This means every file uploaded via boto3 is immediately visible on your host machine.
- **Health Check:** Pings `http://localhost:9000/minio/health/live` every 30 seconds. All other containers **depend on this health check** before starting.

### `minio-init` — Bucket Bootstrap
- **Image:** `minio/mc:latest` (MinIO Client CLI)
- **Role:** Runs **once at startup** to create the `landing-zone` bucket using the `mc mb --ignore-existing` command. This makes the pipeline fully idempotent from its very first boot — no manual bucket creation needed.
- **Dependency:** Waits for `minio` to be healthy before running.

### `postgres` — Airflow Metadata Database
- **Image:** `postgres:13`
- **Role:** Persistent relational database that stores all Airflow metadata (DAG runs, task states, logs references, connections, variables). This is what allows Airflow to resume gracefully after restarts.
- **Why Not SQLite?** SQLite has file locking issues that cause deadlocks with the LocalExecutor's parallel task execution. PostgreSQL is the production-grade, race-condition-free alternative.
- **Volume:** `postgres_data` (named Docker volume, persisted across `docker compose down` cycles).

### `airflow-webserver` — The Control Panel
- **Image:** `apache/airflow:2.9.0`
- **Role:** The Airflow Web UI for monitoring, triggering, and debugging DAG runs.
- **Port:** `8081` on your host maps to `8080` inside the container.
- **Startup Sequence:** Runs `airflow db migrate` to apply schema migrations, then creates the default `admin` user, then starts the server.
- **Dependencies:** Both `minio` (healthy) and `postgres` (healthy) must be ready before this starts.

### `airflow-scheduler` — The Automation Engine
- **Image:** `apache/airflow:2.9.0`
- **Role:** Monitors all DAGs, detects when their schedule triggers (e.g., `@daily`), and dispatches tasks to the LocalExecutor for execution.
- **Dependencies:** Waits for `airflow-webserver` to be healthy (ensuring the DB is already migrated) before starting.

### Shared Airflow Configuration
Both Airflow services share a base configuration defined via the YAML anchor `x-airflow-common`:
- `AIRFLOW__CORE__EXECUTOR=LocalExecutor` — Enables true parallelism within a single machine.
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` — Points to the PostgreSQL container.
- **Volume Mounts:** `./orchestration/` → `/opt/airflow/dags/` and `./ingestion/` → `/opt/airflow/ingestion/`. This means every file you edit locally is instantly picked up by the running containers — **no rebuilds required**.
- **Dynamic pip installs:** `_PIP_ADDITIONAL_REQUIREMENTS=pandas boto3 python-dotenv deltalake` installs these packages at container startup.
- **`.env` file injection:** `env_file: - .env` forwards your `.env` secrets directly into both Airflow containers.

---

## 📂 Data Sources & Ingestion Scripts

All scripts live in `ingestion/` and follow a strict, consistent design pattern:
1. Load configuration from `.env` via `python-dotenv`.
2. Create a `boto3` S3 client pointed at the local MinIO instance.
3. Ensure the target bucket exists.
4. Perform an **Idempotency Check** (`head_object`) before downloading.
5. Upload raw data as-is (no transformation — that is for the Trusted Zone).

---

### 1. `philosophers_ingest.py` — Historical Metadata & Portraits
**Source:** [philosophersapi.com](https://philosophersapi.com/) — A public, no-authentication-required REST API.

**What it does:**
1. Fetches the entire catalog of 114 philosophers in a single flat JSON list (`GET /api/philosophers`).
2. Filters to only the 5 target figures defined in `philosopher_registry.py`.
3. Enriches each record with academic deep-links (`stanford_sep`, `internet_iep`, `wikipedia` URLs) for future enrichment tasks.
4. Uploads the filtered JSON to `s3://landing-zone/philosophers_api/raw_json/philosophers_catalog.json`.
5. Iterates through all image URLs in the `images` dictionary for each philosopher and downloads every portrait, thumbnail, and illustration.
6. Uses an idempotency check per image — already downloaded portraits are skipped.

**Storage path:** `s3://landing-zone/philosophers_api/raw_images/{slug}/{category}/{key}.jpg`

---

### 2. `gutenberg_ingest.py` — Canonical Philosophical Texts
**Source:** [gutendex.com](https://gutendex.com/) — A community REST API wrapping Project Gutenberg's catalog of public domain books.

**What it does:**
1. Iterates through every philosopher in `philosopher_registry.py`.
2. Calls the Gutendex `search` endpoint with the philosopher's name.
3. Filters results to only books where the philosopher is confirmed as an **author** (not just mentioned in the title) by matching the author slug in the response.
4. Uploads a `{slug}_catalog.json` provenance record listing all matched books.
5. Resolves the best plain-text download URL from the `formats` dictionary (UTF-8 → ASCII → any `plain` type, in order of preference).
6. Downloads each `.txt` file and uploads it raw to MinIO.
7. Enforces a **1.5-second mandatory delay** between downloads to comply with Project Gutenberg's robot policy and avoid IP banning.
8. Idempotency: Skips books already uploaded by checking for the S3 key first.

**Storage path:** `s3://landing-zone/gutenberg/raw_text/{slug}_{book_id}_{title}.txt`

---

### 3. `youtube_transcript_ingest.py` — Podcast Interview Transcripts
**Sources:** YouTube Data API v3 (search) + `youtube-transcript-api` (download).

**What it does:**
1. Queries the YouTube Data API v3 `search` endpoint with configured `search_queries`.
2. **Critical filter 1:** `videoCaption="closedCaption"` — This ensures we only discover videos that have an actual text track available to download, preventing wasted API quota.
3. **Critical filter 2:** `videoDuration="long"` — This restricts results to videos over 20 minutes, filtering out short clips and YouTube Shorts. Only long-form podcasts and interviews pass through.
4. **Critical filter 3:** Channel name validation — Results are checked against a `TARGET_CHANNELS` whitelist to restrict downloads to known, high-quality channels.
5. Aggregates all discovered video IDs into a single Python `set()` for automatic deduplication.
6. Uses `youtube-transcript-api` version 1.2.4 (requires instantiation via `YouTubeTranscriptApi()`) to download each transcript with `languages=['es', 'en']` (Spanish preferred, English fallback).
7. Uploads each transcript as a JSON file to MinIO.
8. Idempotency: Checks for the existence of `transcript_{video_id}.json` in S3 before attempting any download.

**Storage path:** `s3://landing-zone/youtube_transcripts/raw_json/transcript_{video_id}.json`

> **Note on API Key:** You need a YouTube Data API v3 key in your `.env` file under `YOUTUBE_API_KEY=`. The transcript download itself (via `youtube-transcript-api`) does **not** require an API key.

---

### 4. `news_ingest.py` — Daily Trending News Snapshots
**Source:** [GNews API](https://gnews.io/) — Aggregates top stories from Google News.

**What it does:**
1. Calls the `GET /api/v4/top-headlines` endpoint, **not** a keyword search. This gives you the very top trending stories ranked by Google News's algorithm.
2. Queries three major categories: `world`, `technology`, and `science`.
3. Tags each article with its source category (`_source_category` field) for easier filtering in downstream tasks.
4. Aggregates all articles from all categories into a single list.
5. Uploads a single daily snapshot file to MinIO, timestamped by date.
6. **Idempotency via daily overwrite:** Files are named `news_snapshot_YYYYMMDD.json`. If the DAG fires twice in one day, it safely overwrites the existing file. This prevents data bloat while ensuring the latest articles are always captured.

**Storage path:** `s3://landing-zone/news_api/raw_json/news_snapshot_{YYYYMMDD}.json`

> **Note:** The free GNews tier allows 100 requests/day, which is more than sufficient for this daily batch pipeline.

---

### 5. `philosopher_registry.py` — The Single Source of Truth
This is **not** an ingestion script — it is the central configuration that all ingestion scripts import from.

It defines a `TARGET_PHILOSOPHERS` list where each philosopher is a dict with all the search terms needed for each source:
```python
{
    "api_name": "Friedrich Nietzsche",      # Exact match for philosophersapi.com
    "gutenberg_search": "Nietzsche",        # Keyword for Gutendex search
    "gutenberg_author_slug": "nietzsche",   # For author attribution filtering
    "kaggle_author": "Friedrich Nietzsche", # Reserved for future Kaggle integration
    "wikidata_label": "Friedrich Nietzsche",# Reserved for future Wikidata SPARQL
}
```

**To add a new philosopher to the entire pipeline, you only edit this one file.** All four ingestion scripts pick up the change automatically.

**Current targets:** Plato, René Descartes, Immanuel Kant, Georg Wilhelm Friedrich Hegel, Friedrich Nietzsche.

---

## ⏱️ DAG Orchestration

**File:** `orchestration/bdm_p1_pipeline_dag.py`

**DAG ID:** `bdm_p1_cold_path_ingestion`

**Schedule:** `@daily` (fires once per day at midnight UTC)

**Configuration:**
- `catchup=False` — Does **not** backfill historical missed runs.
- `max_active_runs=1` — Prevents two concurrent pipeline runs from colliding on the same data.
- `retries=2` with a 5-minute delay — Handles transient API errors gracefully.
- `execution_timeout=2h` — Protects against zombie tasks.

**Task Graph:**
```
check_minio_health
        │
        ├──► ingest_philosophers_api   ─────┐
        ├──► ingest_gutenberg          ─────┤
        ├──► ingest_youtube_transcripts─────┤
        └──► ingest_news_api          ─────┘
                                            │
                                            ▼
                                   pipeline_complete (EmptyOperator)
```

**How tasks execute:** Each ingestion task calls `_run_ingestion_script()`, which runs the Python script as a subprocess using `sys.executable` (the same Python interpreter as Airflow). Stdout/Stderr are captured and forwarded to the Airflow task log.

---

## 🗂️ Data Organization in the Landing Zone

After a full pipeline run, your `./landing_zone/` folder on the host machine (and equivalently your `s3://landing-zone/` bucket in MinIO) will look like this:

```
landing_zone/
└── landing-zone/                          ← MinIO bucket root
    ├── philosophers_api/
    │   ├── raw_json/
    │   │   └── philosophers_catalog.json        ← All 5 philosopher records + academic links
    │   └── raw_images/
    │       ├── plato/
    │       │   ├── thumbnails/thumb.jpg
    │       │   └── illustrations/portrait.jpg
    │       ├── descartes/
    │       ├── kant/
    │       ├── hegel/
    │       └── nietzsche/
    ├── gutenberg/
    │   └── raw_text/
    │       ├── plato_catalog.json               ← Provenance metadata
    │       ├── plato_1497_The_Republic.txt
    │       ├── plato_1616_Symposium.txt
    │       ├── kant_catalog.json
    │       ├── kant_4280_Critique_of_Pure_Reason.txt
    │       └── ...
    ├── youtube_transcripts/
    │   └── raw_json/
    │       ├── transcript_L08-xy6cyYY.json
    │       ├── transcript_XGVGkjZIp7U.json
    │       └── ...
    └── news_api/
        └── raw_json/
            ├── news_snapshot_20260408.json       ← Daily trending headlines snapshot
            └── news_snapshot_20260409.json
```

---

## 📁 Project Structure

```text
P1/
├── docker-compose.yml             # Full 5-container stack definition
├── requirements.txt               # Python deps for local dev & Airflow
├── .env                           # Secrets & configuration (NOT committed to git)
├── .gitignore                     # Excludes .env, .venv, landing_zone data, etc.
│
├── ingestion/                     # Core ingestion scripts
│   ├── philosopher_registry.py    # ← Single source of truth for target entities
│   ├── philosophers_ingest.py     # Philosophers API → JSON + Images → MinIO
│   ├── gutenberg_ingest.py        # Project Gutenberg → Plain Text Books → MinIO
│   ├── youtube_transcript_ingest.py # YouTube API → Transcripts JSON → MinIO
│   └── news_ingest.py             # GNews API → Daily Headlines JSON → MinIO
│
├── orchestration/                 # Airflow DAG definitions
│   └── bdm_p1_pipeline_dag.py     # Daily batch DAG (4 parallel tasks)
│
├── landing_zone/                  # Host-side persistent data directory
│   └── landing-zone/              # Mirrors the MinIO bucket structure
│
└── PLAYGROUND/                    # Experimental scripts & API exploration
```

---

## ⚙️ Requirements & Pre-requisites

### System Requirements
- **Operating System:** Linux, macOS, or Windows (WSL2 recommended)
- **Docker Engine:** >= 24.x with Docker Compose plugin (or `docker-compose` v2)
- **Python:** 3.10 or higher (only needed for local testing outside Airflow)
- **Disk Space:** ~5 GB recommended for Docker images and landing zone data

### API Keys Required

| Service | Key Variable | How to Get |
|---|---|---|
| YouTube Data API v3 | `YOUTUBE_API_KEY` | [Google Cloud Console](https://console.cloud.google.com/) → APIs & Services → Enable YouTube Data API v3 → Create Credentials |
| GNews API | `NEWS_API_KEY` | Register at [gnews.io](https://gnews.io/) → Free tier gives 100 req/day |

> The Philosophers API and Project Gutenberg/Gutendex are completely **public and require no authentication**.

### Python Dependencies (`requirements.txt`)
```
requests>=2.31.0             # HTTP client for all API calls
youtube-transcript-api>=0.6.2# YouTube CC transcript downloader (no key needed)
boto3>=1.34.0                # AWS SDK — used to talk to MinIO (S3-compatible)
python-dotenv>=1.0.0         # Loads .env into os.environ
apache-airflow>=2.9.0        # Workflow orchestration
deltalake>=0.17.0            # Delta Lake (future-proof for Trusted Zone writes)
```

---

## 🚀 Step-by-Step Setup Tutorial

### Step 1: Clone the Repository
```bash
git clone <your-repo-url>
cd P1
```

### Step 2: Create the `.env` File
Create a file named `.env` in the root of the project. This is the only manual configuration step required:

```ini
# ─── MinIO Object Store ────────────────────────────────────────────────────
# Use localhost:9000 for local testing; Airflow uses minio:9000 internally
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password
MINIO_BUCKET=landing-zone

# ─── External API Keys ─────────────────────────────────────────────────────
YOUTUBE_API_KEY=YOUR_YOUTUBE_DATA_API_V3_KEY_HERE
NEWS_API_KEY=YOUR_GNEWS_API_KEY_HERE
```

> ⚠️ **Never commit this file to Git.** It is already listed in `.gitignore`.

### Step 3: Launch the Full Stack
```bash
docker compose up -d
```

This single command boots:
- PostgreSQL (Airflow metadata DB)
- MinIO (object store + auto-creates the `landing-zone` bucket)
- Apache Airflow Webserver & Scheduler

> 🕐 **First boot takes ~60-90 seconds** for the Airflow Webserver to run `db migrate`, create the admin user, and pass its health check before the Scheduler starts.

You can watch the health in real time with:
```bash
docker compose ps
```
All 5 services should show `healthy` or `exited (0)` (for `minio-init`, which finishes immediately after creating the bucket).

### Step 4: Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow Web UI | [http://localhost:8081](http://localhost:8081) | user: `admin` / pass: `admin` |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | user: `admin` / pass: `password` |

---

## 🔄 Running the Pipeline

### Via the Airflow UI (Automated)
1. Open [http://localhost:8081](http://localhost:8081) and log in.
2. Find the DAG `bdm_p1_cold_path_ingestion` in the list.
3. **Unpause it** using the toggle on the left side.
4. Click the **▶ Run** button (the play icon) to trigger a manual execution.
5. Click on the DAG name → **Graph View** to see the tasks executing in parallel.

Each task will turn **green** on success and **red** on failure. Click any task → **Log** tab to see the full real-time output from the ingestion script.

### Via the Command Line (Manual, for testing)
You can trigger a DAG run directly:
```bash
docker exec airflow-scheduler airflow dags trigger bdm_p1_cold_path_ingestion
```

---

## 🧪 Testing Scripts Locally (without Airflow)

This is the fastest way to see real output logs and debug issues:

```bash
# 1. Create and activate the virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Make sure your containers are running (MinIO must be up)
docker compose up -d

# 4. Run the target script directly
python ingestion/philosophers_ingest.py
python ingestion/gutenberg_ingest.py
python ingestion/youtube_transcript_ingest.py
python ingestion/news_ingest.py
```

After a successful run, you can verify the files were created by:
- Browsing to the [MinIO Console](http://localhost:9001) and exploring the `landing-zone` bucket.
- Or checking your host filesystem directly at `./landing_zone/landing-zone/`.

---

## 🔬 Key Engineering Decisions

### Why MinIO instead of AWS S3?
MinIO is a drop-in S3-compatible replacement. Every single `boto3` call in this codebase is identical to what would be used on real AWS S3. This makes the migration to a cloud provider a zero-code-change operation — just swap the `MINIO_ENDPOINT` environment variable.

### Why PostgreSQL for Airflow instead of SQLite?
The default SQLite backend creates file-level locks that cause deadlocks when the `LocalExecutor` tries to run multiple tasks in parallel. PostgreSQL is the industry standard for production Airflow deployments and resolves all parallelism issues.

### Why the `philosopher_registry.py` pattern?
Rather than hardcoding philosopher names differently in each of four scripts, every search term for every source is centralized in one dictionary. Adding "Aristotle" to the pipeline is a **single-line edit** to the registry file — all four scripts automatically pick it up on the next run.

### Why `youtube-transcript-api` instead of downloading audio?
Audio files are large, difficult to process, and would quickly consume disk space. Pure text transcripts give us the exact same conversational data with a fraction of the storage footprint, and they can be directly fed into a language model without requiring a separate speech-to-text step.

### Why Top Headlines news instead of keyword search?
Our AI does not need to know specific facts about AI ethics covered in academic papers—that is what the Philosophers API and Gutenberg cover. What it needs for the interview format is **whatever people are currently talking about** so it can simulate a real-time reaction. Top headlines from `world`, `technology`, and `science` categories provide this ambient awareness of the zeitgeist.

### Idempotency Strategy
Each script uses a different idempotency model appropriate for its data type:
- **Images & Transcripts:** `head_object()` pre-check — file is skipped entirely if it exists.
- **News snapshots:** Daily filename overwrite — the latest run always wins for the current day.
- **Philosopher metadata catalog:** Always overwritten — ensures the latest API truth is stored.
- **Gutenberg books:** `head_object()` pre-check — books do not change, so once downloaded they never need refreshing.
