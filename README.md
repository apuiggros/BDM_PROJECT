# 🏛️ Historical Conversational AI — Data Lakehouse Pipeline
### Big Data Management (BDM) — P1 (Landing Zone) + P2 (Trusted, Exploitation, Consumption)

> **Project status — 2026-06-01 · P2 COMPLETE** &nbsp;·&nbsp; P1 graded **9.5/10** &nbsp;·&nbsp; P2 deadline 2026-06-09
>
> **All four zones plus the BI seam are wired end-to-end and running.** The whole stack stands up with a single `make up` (see [§ Deploy the stack](#-deploy-the-stack)).
>
> - **Landing** — MinIO. **8 batch sources** (Philosophers, Wikipedia, Wikiquote, Gutenberg, GNews, Stack Exchange, Podcast audio, **Hacker News Algolia** — new in P2) plus a **Kafka streaming topic**.
> - **Trusted** — DuckDB `trusted.duckdb`. 12 cleaned tables. PySpark for the heavy reads; pure DuckDB+boto3 for the small ones (tool-justified by data volume).
> - **Exploitation** — DuckDB star schema `exploit.duckdb` (`dim_figure` + **5 fact tables + 1 streaming view**) plus a Milvus `corpus_chunks` collection (**87,437 vectors**, HNSW/COSINE, sentence-transformers/all-MiniLM-L6-v2).
> - **Consumption** — **(a)** Conversational podcast interview (Reasoner / Voice / Interviewer / Episode composer, Claude `claude-sonnet-4-6`); **(b)** **Streamlit BI dashboard** at `:8501` with figure cards, custom SQL, and a Milvus semantic-search playground.
> - **Streaming** — live Kafka → Spark Structured Streaming → Parquet → DuckDB view, joined to `dim_figure` and surfaced on the dashboard.
>
> **Full technical report:** [`documents/P2_TECHNICAL_REPORT.pdf`](documents/P2_TECHNICAL_REPORT.pdf) (12 pages, 12 sections).
>
> **Note on the P1 Delta tables:** the `bronze_tables/` Delta Lake layer described later in this README is **deprecated** as of P2. All tabular state lives in DuckDB (trusted + exploitation); all vector state lives in Milvus. MongoDB was deliberately dropped — every tool is justified by a downstream consumer (see the report §9 design-decision register, esp. D1 and D13).

---

## 📋 Table of Contents

**P2 (current)**
0. [P2 Status & Zone Overview](#-p2-status--zone-overview)
   - [Trusted Zone](#trusted-zone)
   - [Exploitation Zone](#exploitation-zone)
   - [Consumption Zone](#consumption-zone)

**P1 (landing zone — still in production for cold-path ingestion)**
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

## 🚦 P2 Status & Zone Overview

```
   MinIO (landing)  ──►  DuckDB (trusted)  ──►  DuckDB star (exploit)  ──►  Consumption agents
                            │                       │
                            │                       └──►  Milvus (corpus_chunks, 384-d, HNSW/COSINE)
                            │
                            └──►  Spark job (chunker+embedder) writes Milvus
```

| Zone | Storage | Built by | Status |
|---|---|---|---|
| Landing | MinIO `landing-zone/` | `bdm_p1_cold_path_ingestion` (P1 DAG, daily) | ✅ Done (P1) |
| Trusted | DuckDB `duckdb/trusted.duckdb` (11 tables) | `bdm_p2_trusted_zone_dag` | ✅ Done |
| Exploitation (tabular) | DuckDB `duckdb/exploit.duckdb` (`dim_figure` + 6 facts: works/quotes/news/SE/HN/mentions) | `bdm_p2_exploitation_zone_dag` | ✅ Done |
| Exploitation (vector) | Milvus `corpus_chunks` collection | Spark job inside the same DAG | ✅ Done |
| Consumption | `consumption/agents/*.py` + Markdown episodes | Manual / on-demand for now | ✅ Done (batch) |
| Streaming seam | Kafka → Spark Structured Streaming → parquet → `fact_mentions_1m` view | `ingestion/spark_stream_mentions_1m.py` + `exploitation/structured/fact_mentions_1m.py` | ✅ Done |
| Dashboard (BI) | Streamlit on port 8501, all six zones tabbed | `streamlit_app/app.py` | ✅ Done |

> **Full P2 walkthrough:** see `documents/P2_TECHNICAL_REPORT.pdf` (10 pages — architecture diagram, datasource usage matrix, full star schema, consumption sequence diagram, design-decision register).

### Trusted Zone
- **Code:** `cleaning/structured/` (6 SQL-based cleaners) + `cleaning/unstructured/` (3 byte-level cleaners).
- **Output:** `duckdb/trusted.duckdb` — one cleaned table per source (e.g. `trusted_wiki_pages`, `trusted_works`, `trusted_quotes`, `trusted_news_articles`, `trusted_se_qa`, `trusted_philosophers`, `trusted_gutenberg_catalog`, `trusted_podcast_episodes`, plus image/text content tables).
- **Contract:** typed columns, deduped on a stable natural key, `figure_slug` joined back to the registry where applicable.
- **Schemas documented in:** `cleaning/SCHEMAS.md`.
- **DAG:** `orchestration/bdm_p2_trusted_zone_dag.py`. On success, triggers the exploitation DAG via `TriggerDagRunOperator`.

### Exploitation Zone
- **Code:** `exploitation/structured/` — `dim_figure.py`, `fact_works.py`, `fact_quotes.py`, `fact_news_articles.py`, `fact_se_qa.py`, `fact_hn_stories.py`, `fact_mentions_1m.py`, `corpus_chunks.py`.
- **Tabular output:** `duckdb/exploit.duckdb` — classic star schema with `dim_figure` (9 figures) at the center and six fact tables/views: `fact_works`, `fact_quotes`, `fact_news_articles`, `fact_se_qa`, `fact_hn_stories` (P2 addition — Hacker News discourse signal), and `fact_mentions_1m` (streaming view). Fact builds are serialized via `chain()` because DuckDB is a single-writer engine.
- **Vector output:** Milvus `corpus_chunks` collection — 384-d `sentence-transformers/all-MiniLM-L6-v2` embeddings, HNSW index (`M=16`, `efConstruction=200`), COSINE metric, ~220-word chunks with 40-word overlap. **Built by a Spark job** (the only place we use Spark — justified by per-row chunking + embedding fan-out).
- **DAG:** `orchestration/bdm_p2_exploitation_zone_dag.py`.

### Consumption Zone
- **Code:** `consumption/agents/`
  - `reasoner.py` — RAG retrieval against Milvus + DuckDB joins; builds the identity card with `voice_descriptor()` derived from the curated Wikipedia summary (no fabricated columns).
  - `voice.py` — character-as-themselves response generation.
  - `interviewer.py` — adaptive follow-ups, history threading, content-safety curation.
  - `episode.py` — composes the final Markdown episode under `consumption/episodes/`.
- **LLM:** `consumption/llm.py` — provider-swappable `llm_fn`; default is Anthropic Claude (`claude-sonnet-4-6`).
- **Status:** runs end-to-end for any of the 9 figures from a curated opener through ~10 turns. TTS / audio rendering is the next deferred milestone.

### Hacker News Source (P2 addition)
- **Why HN, not Reddit:** Reddit's unauthenticated search endpoint started returning browser-check HTML pages (HTTP 403) for all programmatic clients in 2026, and OAuth requires per-account "Responsible Builder Policy" gating that several team accounts couldn't pass. We pivoted to the HN Algolia API: open, no auth, no key, and — most importantly — HN's audience genuinely discusses the 9 figures with much higher signal-to-noise (the top "Immanuel Kant" result is a 242-point philosophy primer on ralphammer.com; the top Reddit result was a Spanish "chimichanga" food post).
- **Ingester:** `ingestion/hackernews_ingest.py` — quoted-phrase search per figure (`q="Immanuel Kant"` etc.), `tags=story`, 25 hits each, daily snapshot to `s3://landing-zone/hackernews/raw_json/YYYY-MM-DD.json`. Registry-driven; no per-figure code paths.
- **Trusted cleaner:** `cleaning/structured/hackernews.py` — pure DuckDB + boto3 (the tool-justification rule applies: ~169 rows per snapshot does not need a Spark JVM). Dedupes on `object_id`, derives `host` from `url` for the dashboard.
- **Fact:** `fact_hn_stories` joins `dim_figure` on `figure_slug`. Live count: **169 stories, 8 figures, 105 distinct hosts, 18 years of HN history (2008→2026)**.
- **Dashboard:** the HN panel in the Exploitation tab shows stories-per-figure, top hosts (New Yorker, Paris Review, Guardian, NYT, …), and top stories by points.

### Streaming Seam (Hot Path)
- **Producer:** `ingestion/stream_producer.py` — synthetic generator that emits `character-mentions` events to Kafka (`character_name`, `domain`, `message`, `sentiment_score`, `source`) on a randomized 1–5s cadence. Not backed by a real Reddit/Twitter API; it simulates social-media traffic for the streaming demo.
- **Aggregator:** `ingestion/spark_stream_mentions_1m.py` — Spark Structured Streaming job, 1-minute tumbling windows keyed by `(character_name, domain)`, watermark 30s, writes parquet to `streaming/fact_mentions_1m/`.
- **Exposure:** `exploitation/structured/fact_mentions_1m.py` registers a DuckDB VIEW in `exploit.duckdb` that reads the parquet on the fly and LEFT JOINs `dim_figure` on `name` to attach `figure_slug` (NULL on unmapped names = visible drift signal).
- **Run locally:**
  ```bash
  # 1) start the producer in the airflow-scheduler container
  docker compose exec -d airflow-scheduler python /opt/airflow/ingestion/stream_producer.py
  # 2) start the Spark streaming job
  docker compose exec -d airflow-scheduler spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/airflow/ingestion/spark_stream_mentions_1m.py
  # 3) re-register the view so it sees the new files
  docker compose exec airflow-scheduler python /opt/airflow/exploitation/structured/fact_mentions_1m.py
  ```

### Dashboard (BI seam)
- **Code:** `streamlit_app/app.py` — single-page Streamlit app, port **8501**.
- **Six tabs:** Landing (MinIO objects) · Trusted (DuckDB table row counts) · Exploitation (star-schema queries + custom SQL, including the HN discourse-signal panel: top hosts, stories-per-figure, top stories by points) · Streaming (live `fact_mentions_1m` window rows) · Milvus (`corpus_chunks` stats) · Episodes (rendered podcast Markdown).
- **Source of truth:** reads `duckdb/trusted.duckdb` + `duckdb/exploit.duckdb` (read-only mounts) and `streaming/fact_mentions_1m/*.parquet` directly. Each tab degrades gracefully if its source isn't populated yet.
- **Open:** `http://localhost:8501` once `docker compose up` is settled.

---

## P1 Reference Documentation

Everything below documents the **P1 landing zone**, which is still the canonical cold-path ingestion layer feeding the Trusted Zone. The text is preserved as-delivered for P1; treat the Delta Lake / `bronze_tables/` references as historical — that layer is no longer wired into downstream consumers.

---

## 🎯 End Goal & Motivation

The ultimate objective of this project is to power a **multimodal AI system capable of generating realistic, podcast-style interviews with famous historical figures** — spanning philosophy, science, and literature.

The challenge is that such an AI needs to answer a deceptively complex question: *"How would Immanuel Kant react to today's news on artificial intelligence?"*. To answer this credibly, the AI needs at least six distinct categories of raw knowledge:

| Pillar | What the AI Learns | Where We Get It |
|---|---|---|
| **Core Biographical Facts** | Names, schools of thought, dates, concepts, portraits | Philosophers REST API & Wikipedia |
| **Authoritative Writings** | The actual vocabulary, reasoning style, and syntax of the figure | Project Gutenberg (Public Domain books) |
| **Verified Quotes** | Authentic historical quotes and citations | Wikiquote MediaWiki API |
| **Community Q&A** | Modern philosophical debates, clarifications, and community Q&A | Philosophy Stack Exchange API |
| **Conversational Dynamics** | How an interview or debate flows — tone, pacing, turn-taking | Podcast Audio (iTunes RSS) |
| **Current Events Awareness** | Top trending daily news so the historical figure can "react" to the modern world | GNews API (Top Headlines) |
| **Public Discourse Signal** | Where and how each figure is being discussed today (high-quality long-form posts) | Hacker News Algolia API (P2 addition) |

This P1 deliverable focuses on **Phase 1**: Building and automating a fully containerized, self-healing Bronze Layer pipeline to **extract and store all this raw data at scale**, creating the foundation from which the future Trusted Zone (data cleansing) and Exploitation Zone (AI model training) can be built.

---

## 🏗️ Architecture Overview

The pipeline follows a **Registry-Driven, Micro-Ingestion architecture** organized around a central `character_registry.py` — a single source of truth for all target entities. Every ingestion script reads from this registry, ensuring that adding a new historical figure to the pipeline only requires editing one file.

The pipeline currently targets **9 historical figures** across three domains (philosophy, science, literature), ingesting data from **8 external sources** (P2 added Hacker News via the Algolia API) plus a Kafka-based streaming pipeline.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES (External)                             │
│                                                                             │
│  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐        │
│  │ philosophersapi  │   │  gutendex.com    │   │  iTunes API      │        │
│  │ .com REST API    │   │  (Gutenberg API) │   │  Podcast RSS     │        │
│  │                  │   │                  │   │                  │        │
│  │ • 114 records    │   │ • Public domain  │   │ • Global Topics  │        │
│  │ • JSON metadata  │   │ • .txt books     │   │ • .mp3 Audio     │        │
│  │ • Images (JPEG)  │   │ • Author catalog │   │ • JSON Metadata  │        │
│  └────────┬─────────┘   └────────┬─────────┘   └────────┬─────────┘        │
│           │                      │                      │                   │
│  ┌────────▼─────────┐   ┌────────┴──────┐      ┌────────┴──────┐         │
│  │ Wikipedia API    │   │ Wikiquote API │      │ StackExchange │         │
│  │ Biography Sums   │   │ Quotes/Facts  │      │ Q&A History   │         │
│  └────────┬─────────┘   └────────┬──────┘      └────────┬──────┘         │
│           │                      │                      │                   │
│  ┌────────▼─────────┐                                                      │
│  │ gnews.io         │                                                      │
│  │ Top Headlines    │                                                      │
│  └────────┬─────────┘                                                      │
└───────────┼─────────────────────────────────────────────────────────────────┘
            │
┌───────────▼─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER (Docker Containers)                   │
│                                                                             │
│                         Apache Airflow 2.9.0                                │
│                      (LocalExecutor | PostgreSQL Backend)                   │
│                                                                             │
│  DAG: bdm_p1_cold_path_ingestion  [schedule: @daily]                        │
│                                                                             │
│                    ┌─────────────────────┐                                  │
│                    │  check_minio_health │  ← Health gate (fail-fast)       │
│                    └──────────┬──────────┘                                  │
│           ┌───────────────────┼──────────────────┬─────────────────┐        │
│           ▼                   ▼                  ▼                 ▼        │
│  ┌────────────────┐  ┌────────────────┐  ┌───────────────┐  ┌──────────────┐│
│  │ ingest_        │  │ ingest_        │  │ ingest_       │  │ ingest_      ││
│  │ philosophers   │  │ gutenberg      │  │ podcast_audio │  │ wikipedia    ││
│  │ _api           │  │                │  │               │  │              ││
│  └───────┬────────┘  └───────┬────────┘  └───────┬───────┘  └──────┬───────┘│
│          │                   │                   │                 │        │
│  ┌───────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐        │        │
│  │ ingest_news_api│  │ ingest_        │  │ ingest_        │        │        │
│  │                │  │ wikiquote      │  │ philosophy_se  │        │        │
│  └───────┬────────┘  └───────┬────────┘  └───────┬────────┘        │        │
│          │                   │                   │                 │        │
│          └───────────────────┴─────────┬─────────┴─────────────────┘        │
│                                        ▼                                    │
│                     ┌──────────────────┐                                    │
│                     │ convert_to_delta │                                    │
│                     └────────┬─────────┘                                    │
│                               ▼                                             │
│                       ┌──────────────────┐                                  │
│                       │ pipeline_complete│                                  │
│                       └──────────────────┘                                  │
└─────────────────────────────────────────────────────────────────────────────┘
            │                     │                      │              │
┌───────────▼─────────────────────▼──────────────────────▼──────────────▼────┐
│                     STORAGE LAYER — Bronze Landing Zone                     │
│                                                                             │
│                  MinIO (S3-compatible) — Local Object Store                 │
│                                                                             │
│  Bucket: landing-zone                                                       │
│  ├── philosophers_api/  ← (domain filtered)                                 │
│  ├── gutenberg/         ← raw_text/{domain}/{slug}_{id}.txt                 │
│  ├── wikipedia/         ← raw_json/{domain}/{slug}_wikipedia.json           │
│  ├── wikiquote/         ← raw_json/{domain}/{slug}_wikiquote.json           │
│  ├── philosophy_se/     ← raw_json/philosophy_se_snapshot_{date}.json       │
│  ├── podcasts/          ← raw_audio/{podcast_slug}/ep_{id}.mp3              │
│  ├── hot_path/          ← raw_stream/mentions_{timestamp}.json              │
│  ├── news_api/          ← raw_json/news_snapshot_{date}.json                │
│  └── bronze_tables/     ← DELTA LAKE HOUSE (ACID Tables)                    │
│      ├── philosophers/  ← Unified metadata from Philosophers API            │
│      ├── news_headlines/← Daily aggregated news snapshots                   │
│      ├── wikipedia_biographies/ ← Structured character summaries (Facts)    │
│      ├── wikiquote_quotes/      ← Verified character quotes and citations   │
│      ├── philosophy_se_questions/ ← Philosophy Stack Exchange archive       │
│      ├── gutenberg_library/     ← Catalog of available texts                │
│      └── podcast_episodes/      ← Metadata for downloaded audio             │
│                                                                             │
│  Host volume bind: ./landing_zone/ → /data (inside MinIO container)         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🐳 Infrastructure Deep Dive

The entire infrastructure is defined in `docker-compose.yml` and spins up **8 containers**:

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
- **Port:** `8080` on your host.
- **Startup Sequence:** Runs `airflow db migrate` to apply schema migrations, then creates the default `admin` user, then starts the server.
- **Dependencies:** Both `minio` (healthy) and `postgres` (healthy) must be ready before this starts.

### `airflow-scheduler` — The Automation Engine
- **Image:** `apache/airflow:2.9.0`
- **Role:** Monitors all DAGs, detects when their schedule triggers (e.g., `@daily`), and dispatches tasks to the LocalExecutor for execution.
- **Dependencies:** Waits for `airflow-webserver` to be healthy (ensuring the DB is already migrated) before starting.

### Apache Kafka & Zookeeper — The Hot Path
- **Images:** `confluentinc/cp-kafka`, `confluentinc/cp-zookeeper`
- **Role:** Handles real-time events and streaming data.
- **Topic:** `character-mentions` — captures simulated real-time mentions of historical figures across the web.
- **Consumer:** Flushes stream data into the hot-path Landing Zone prefix in MinIO (`hot_path/raw_stream/`).

### Kafka UI — Stream Monitoring
- **URL:** [http://localhost:8085](http://localhost:8085)
- **Role:** Provides visibility into topic traffic, offsets, and consumer group health.

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
6. Organize data by **Domain** (philosophy, science, literature) where applicable.

---

### 1. `philosophers_ingest.py` — Historical Metadata & Portraits
**Source:** [philosophersapi.com](https://philosophersapi.com/) — A public, no-authentication-required REST API.

**What it does:**
1. Fetches the entire catalog of 114 philosophers in a single flat JSON list (`GET /api/philosophers`).
2. Filters to only the 5 philosophy-domain target figures defined in `character_registry.py`.
3. Enriches each record with academic deep-links (`stanford_sep`, `internet_iep`, `wikipedia` URLs) for future enrichment tasks.
4. Uploads the filtered JSON to `s3://landing-zone/philosophers_api/raw_json/philosophy/philosophers_catalog.json`.
5. Iterates through all image URLs in the `images` dictionary for each philosopher and downloads every portrait, thumbnail, and illustration.
6. Uses an idempotency check per image — already downloaded portraits are skipped.

**Storage path:** `s3://landing-zone/philosophers_api/raw_images/{domain}/{slug}/{category}/{key}.jpg`

---

### 2. `gutenberg_ingest.py` — Canonical Historical Texts
**Source:** [gutendex.com](https://gutendex.com/) — A community REST API wrapping Project Gutenberg's catalog of public domain books.

**What it does:**
1. Iterates through every figure in `character_registry.py` (all domains).
2. Calls the Gutendex `search` endpoint with the figure's name.
3. Filters results to only books where the figure is confirmed as an **author** (not just mentioned in the title) by matching the author slug in the response.
4. Uploads a `{slug}_catalog.json` provenance record listing all matched books.
5. Resolves the best plain-text download URL from the `formats` dictionary (UTF-8 → ASCII → any `plain` type, in order of preference).
6. Downloads each `.txt` file and uploads it raw to MinIO.
7. Enforces a **1.5-second mandatory delay** between downloads to comply with Project Gutenberg's robot policy and avoid IP banning.
8. Idempotency: Skips books already uploaded by checking for the S3 key first.

**Storage path:** `s3://landing-zone/gutenberg/raw_text/{domain}/{slug}_{book_id}_{title}.txt`

---

### 3. `podcast_audio_ingest.py` — Conversational Dynamics & Pacing
**Sources:** iTunes Search API + Podcast RSS Feeds.

**What it does:**
1. Uses a **Discovery-Based** approach to find unstructured audio examples of human conversation.
2. Queries the iTunes Search API for broad topics (configured in `TARGET_TOPICS`).
3. Discovers the top-ranking podcast channels for those topics.
4. Parses the RSS feeds of those channels to find the latest episodes.
5. Downloads the `.mp3` or `.m4a` audio files and uploads them raw to MinIO.
6. Generates a **JSON Metadata Envelope** for each episode containing provenance (podcast name, author, topic source).
7. Idempotency: Checks for the existence of the audio file in S3 before downloading.

**Storage path:** `s3://landing-zone/podcasts/raw_audio/{podcast_slug}/ep_{id}.mp3`

---

### 4. `wikipedia_ingest.py` — The Universal Biographical Backbone
**Source:** Wikipedia REST API (`en.wikipedia.org/api/rest_v1/page/summary`).

**What it does:**
1. Iterates through **every figure** in `character_registry.py` (all domains).
2. Fetches a structured biography summary, including a plain-text extract and normalized metadata.
3. Organizes files strictly by domain subdirectory (philosophy, science, literature).
4. This script ensures that even if other sources fail, every historical figure has a baseline of factual knowledge.

**Storage path:** `s3://landing-zone/wikipedia/raw_json/{domain}/{slug}_wikipedia.json`

---

### 5. `wikiquote_ingest.py` — Verified Quotes & Citations
**Source:** Wikiquote MediaWiki API (`en.wikiquote.org/w/api.php`).

**What it does:**
1. Iterates through every figure in `character_registry.py`.
2. Fetches verified quotes and citations using the `wikidata_label` as the page title.
3. Provides the "voice" of the historical figure through their own historically attributed words.
4. **Idempotency:** Applies a `head_object` check to avoid redundant API hits for static quotes.

**Storage path:** `s3://landing-zone/wikiquote/raw_json/{domain}/{slug}_wikiquote.json`

---

### 6. `philosophyse_ingest.py` — Community Q&A & Modern Debates
**Source:** [Stack Exchange API](https://api.stackexchange.com/) — Philosophy site.

**What it does:**
1. Downloads the top 500 highest-voted questions with accepted answers from the Philosophy Stack Exchange.
2. Captures modern community interpretations and common clarifications of philosophical concepts.
3. Each question is tagged with a `_ingested_at` timestamp for temporal tracking.
4. **Daily Snapshot:** Aggregates all Q&A into a single JSON snapshot per day.

**Storage path:** `s3://landing-zone/philosophy_se/raw_json/philosophy_se_snapshot_{YYYYMMDD}.json`

---

### 7. `news_ingest.py` — Daily Trending News Snapshots
**Source:** [GNews API](https://gnews.io/) — Aggregates top stories from Google News.

**What it does:**
1. Calls the `GET /api/v4/top-headlines` endpoint, **not** a keyword search. This gives you the very top trending stories ranked by Google News's algorithm.
2. Queries three major categories: `world`, `technology`, and `science`.
3. Tags each article with its source category (`_source_category` field) for easier filtering in downstream tasks.
4. Aggregates all articles from all categories into a single list.
5. Uploads a single daily snapshot file to MinIO, timestamped by date.
6. **Idempotency via daily overwrite:** Files are named `news_snapshot_YYYYMMDD.json`. If the DAG fires twice in one day, it safely overwrites the existing file.

**Storage path:** `s3://landing-zone/news_api/raw_json/news_snapshot_{YYYYMMDD}.json`

> **Note:** The free GNews tier allows 100 requests/day, which is more than sufficient for this daily batch pipeline.

---

### 8. `stream_producer.py` & `stream_consumer.py` — The Hot Path Ingestion
**Source:** Simulated Real-time Character Mentions (Kafka).

**What it does:**
1. **Producer:** Generates a real-time stream of JSON messages simulating mentions of characters in historical/academic context with sentiment scores.
2. **Kafka:** Broker manages the `character-mentions` topic.
3. **Consumer:** A background process that listens to the stream and flushes messages to MinIO once a buffer size is reached. On shutdown, remaining buffered messages are flushed to prevent data loss.
4. This implements the **Streaming Ingestion** requirement of the Data Lakehouse architecture.

The streaming pipeline operates continuously and independently of the daily DAG schedule, as batch orchestration is incompatible with long-running consumer processes.

**Storage path:** `s3://landing-zone/hot_path/raw_stream/mentions_{timestamp}.json`

---

### 9. `metadata_to_delta.py` — The Master Lakehouse Orchestrator
**Role:** Converts raw semi-structured JSON objects from all sources into a structured **Delta Lake** format.

**What it does:**
1. **Unified Aggregation:** Instead of hundreds of individual JSON files, it creates 7 consolidated "Master Tables."
2. **Delta Tables Created:**
   - `philosophers`: All core metadata from the Philosophers API.
   - `news_headlines`: A history of all daily news snapshots (append mode).
   - `wikipedia_biographies`: Factual summaries (biographies) for all characters.
   - `wikiquote_quotes`: Aggregated verified quotes and citations.
   - `philosophy_se_questions`: Archive of Stack Exchange Q&A history (append mode).
   - `gutenberg_library`: A searchable catalog of every text file available.
   - `podcast_episodes`: An index of all audio files with their durations and descriptions.
3. **Big Data Features:** Adds **Time Travel**, **Schema Enforcement**, and high-speed **Parquet** storage to the Bronze Layer.

**Storage path:** `s3://landing-zone/bronze_tables/{table_name}/`

---

### `character_registry.py` — The Single Source of Truth
This is **not** an ingestion script — it is the central configuration that all ingestion scripts import from.

It defines a `TARGET_FIGURES` list where each historical figure is a dict with all search terms needed for each source:
```python
{
    "domain":                "philosophy",
    "api_name":              "Friedrich Nietzsche",
    "gutenberg_search":      "Nietzsche",
    "gutenberg_author_slug": "nietzsche",
    "wikidata_label":        "Friedrich Nietzsche",
}
```

**To add a new figure (philosopher, scientist, author) to the entire pipeline, you only edit this one file.** All ingestion scripts pick up the change automatically.

**Current targets (9 figures across 3 domains):**

| Domain | Figures |
|---|---|
| Philosophy | Plato, René Descartes, Immanuel Kant, Georg Wilhelm Friedrich Hegel, Friedrich Nietzsche |
| Science | Albert Einstein, Charles Darwin |
| Literature | Oscar Wilde, Mark Twain |

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
        ├──► ingest_philosophers_api   ─────────┐
        ├──► ingest_gutenberg          ─────────┤
        ├──► ingest_podcast_audio      ─────────┤
        ├──► ingest_wikipedia_biog     ─────────┼──► convert_to_delta ──► pipeline_complete
        ├──► ingest_news_api           ─────────┤
        ├──► ingest_wikiquote          ─────────┤
        └──► ingest_philosophy_se      ─────────┘
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
    │   │   └── philosophy/
    │   │       └── philosophers_catalog.json    ← All 5 philosopher records + academic links
    │   └── raw_images/
    │       ├── philosophy/
    │       │   ├── plato/
    │       │   │   ├── thumbnails/thumb.jpg
    │       │   │   └── illustrations/portrait.jpg
    │       │   ├── descartes/
    │       │   ├── kant/
    │       │   ├── hegel/
    │       │   └── nietzsche/
    ├── gutenberg/
    │   └── raw_text/
    │       ├── philosophy/
    │       │   ├── plato_catalog.json               ← Provenance metadata
    │       │   ├── plato_1497_The_Republic.txt
    │       │   └── plato_1616_Symposium.txt
    │       ├── science/
    │       │   ├── darwin_catalog.json
    │       │   └── einstein_catalog.json
    │       └── literature/
    │           ├── wilde_catalog.json
    │           └── twain_catalog.json
    ├── wikipedia/
    │   └── raw_json/
    │       ├── philosophy/
    │       │   └── plato_wikipedia.json
    │       ├── science/
    │       │   └── einstein_wikipedia.json
    │       └── literature/
    │           └── wilde_wikipedia.json
    ├── wikiquote/
    │   └── raw_json/
    │       ├── philosophy/
    │       │   └── plato_wikiquote.json
    │       ├── science/
    │       │   └── einstein_wikiquote.json
    │       └── literature/
    │           └── wilde_wikiquote.json
    ├── philosophy_se/
    │   └── raw_json/
    │       └── philosophy_se_snapshot_20260411.json
    ├── podcasts/
    │   ├── raw_audio/
    │   │   └── philosophize_this/
    │   │       └── ep_kant_intro.mp3
    │   └── metadata/
    │       └── philosophize_this/
    │           └── ep_kant_intro_meta.json
    ├── news_api/
    │   └── raw_json/
    │       ├── news_snapshot_20260408.json       ← Daily trending headlines snapshot
    │       └── news_snapshot_20260409.json
    ├── hot_path/
    │   └── raw_stream/
    │       └── mentions_{timestamp}.json         ← Real-time Kafka stream flushes
    └── bronze_tables/                            ← DELTA LAKE (ACID Tables)
        ├── philosophers/
        ├── news_headlines/
        ├── wikipedia_biographies/
        ├── wikiquote_quotes/
        ├── philosophy_se_questions/
        ├── gutenberg_library/
        └── podcast_episodes/
```

---

## 📁 Project Structure

```text
P1/
├── docker-compose.yml             # Full 8-container stack definition
├── requirements.txt               # Python deps for local dev & Airflow
├── .env                           # Secrets & configuration (NOT committed to git)
├── .gitignore                     # Excludes .env, .venv, landing_zone data, etc.
│
├── ingestion/                     # Core ingestion scripts
│   ├── character_registry.py      # ← Single source of truth for all 9 target entities
│   ├── philosophers_ingest.py     # Philosophers API → JSON + Images → MinIO
│   ├── gutenberg_ingest.py        # Project Gutenberg → Plain Text Books → MinIO
│   ├── podcast_audio_ingest.py    # iTunes RSS → Audio .mp3 → MinIO
│   ├── wikipedia_ingest.py        # Wikipedia API → Bio JSON → MinIO
│   ├── wikiquote_ingest.py        # Wikiquote API → Quotes JSON → MinIO
│   ├── philosophyse_ingest.py     # StackExchange → Q&A JSON → MinIO
│   ├── news_ingest.py             # GNews API → Daily Headlines JSON → MinIO
│   ├── hackernews_ingest.py       # P2 — HN Algolia API → Landing JSON → MinIO
│   ├── spark_stream_mentions_1m.py # P2 — Spark Structured Streaming aggregator
│   ├── stream_producer.py         # SIMULATED trends → Kafka
│   ├── stream_consumer.py         # Kafka → MinIO (Hot Path)
│   └── metadata_to_delta.py       # JSON → Delta Lake (Lakehouse conversion)
│
├── orchestration/                 # Airflow DAG definitions
│   ├── bdm_p1_pipeline_dag.py     # P1: Daily landing-zone ingestion DAG
│   ├── bdm_p2_trusted_zone_dag.py # P2: Landing → Trusted (DuckDB) cleaners
│   └── bdm_p2_exploitation_zone_dag.py  # P2: Trusted → Star + Milvus (Spark)
│
├── cleaning/                      # P2 Trusted Zone — landing → DuckDB
│   ├── SCHEMAS.md
│   ├── structured/                # SQL-based cleaners (one per JSON source)
│   └── unstructured/              # Byte-level cleaners (texts, images, audio)
│
├── exploitation/                  # P2 Exploitation Zone — Trusted → star + vectors
│   └── structured/
│       ├── dim_figure.py
│       ├── fact_works.py
│       ├── fact_quotes.py
│       ├── fact_news_articles.py
│       ├── fact_se_qa.py
│       ├── fact_hn_stories.py     # P2 — HN discourse signal fact
│       ├── fact_mentions_1m.py    # Streaming view over parquet → exploit.duckdb
│       └── corpus_chunks.py       # Spark job → Milvus
│
├── consumption/                   # P2 Consumption Zone — conversational podcast
│   ├── agents/
│   │   ├── reasoner.py            # RAG + identity card + voice_descriptor
│   │   ├── voice.py
│   │   ├── interviewer.py
│   │   └── episode.py
│   ├── llm.py                     # Provider-swappable Claude wrapper
│   └── episodes/                  # Generated Markdown episodes (gitignored)
│
├── streamlit_app/                 # P2 BI seam — single-page lakehouse dashboard
│   ├── app.py                     # 6 tabs: Landing/Trusted/Exploit/Stream/Milvus/Episodes
│   └── requirements.txt
│
├── streaming/                     # Spark Structured Streaming output (gitignored)
│   └── fact_mentions_1m/          # 1-min character-mention parquet windows
│
├── duckdb/                        # P2 DuckDB files (gitignored — regenerable)
│   ├── trusted.duckdb
│   └── exploit.duckdb
│
├── documents/                     # Design docs + P2 technical report (PDF)
│
└── landing_zone/                  # Host-side persistent data directory
    └── landing-zone/              # Mirrors the MinIO bucket structure
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
| GNews API | `NEWS_API_KEY` | Register at [gnews.io](https://gnews.io/) → Free tier gives 100 req/day |

> The Philosophers API, Project Gutenberg/Gutendex, Wikipedia, Wikiquote, and Stack Exchange are completely **public and require no authentication**.

### Python Dependencies (`requirements.txt`)
```
requests>=2.31.0             # HTTP client for all API calls
pandas>=2.0.0                # Data analysis and manipulation
boto3>=1.34.0                # AWS SDK — used to talk to MinIO (S3-compatible)
python-dotenv>=1.0.0         # Loads .env into os.environ
apache-airflow>=2.9.0        # Workflow orchestration
deltalake>=0.17.0            # Delta Lake format support
pyarrow>=15.0.0              # Parquet storage underpinning
kafka-python-ng>=2.2.0       # Kafka client (Producer/Consumer)
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

# ─── Kafka ──────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# ─── External API Keys ─────────────────────────────────────────────────────
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
- Zookeeper, Kafka & Kafka UI (hot-path streaming)

> 🕐 **First boot takes ~60-90 seconds** for the Airflow Webserver to run `db migrate`, create the admin user, and pass its health check before the Scheduler starts.

You can watch the health in real time with:
```bash
docker compose ps
```
All 8 services should show `healthy` or `exited (0)` (for `minio-init`, which finishes immediately after creating the bucket).

### Step 4: Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow Web UI | [http://localhost:8080](http://localhost:8080) | user: `admin` / pass: `admin` |
| MinIO Console | [http://localhost:9001](http://localhost:9001) | user: `admin` / pass: `password` |
| Kafka UI | [http://localhost:8085](http://localhost:8085) | (no auth required) |

---

## 🔄 Running the Pipeline

### Via the Airflow UI (Automated)
1. Open [http://localhost:8080](http://localhost:8080) and log in.
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

# 3. Make sure your containers are running (MinIO + Kafka must be up)
docker compose up -d

# 4. Run cold-path scripts directly
python ingestion/philosophers_ingest.py
python ingestion/gutenberg_ingest.py
python ingestion/wikipedia_ingest.py
python ingestion/wikiquote_ingest.py
python ingestion/philosophyse_ingest.py
python ingestion/news_ingest.py
python ingestion/podcast_audio_ingest.py
python ingestion/metadata_to_delta.py

# 5. Run hot-path scripts (in separate terminals)
python ingestion/stream_producer.py
python ingestion/stream_consumer.py
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

### Why the `character_registry.py` pattern?
Rather than hardcoding names differently in each script, every search term for every source is centralized in one dictionary. Adding a new figure to the pipeline is a **single-line edit** to the registry file — all scripts automatically pick it up on the next run.

### Why Podcasts instead of downloading YouTube?
Audio files provide the raw conversational data needed for future voice-cloning and tone-analysis steps. While YouTube transcripts are pure text, podcasts provide both the content and the acoustic performance, making the AI's future generation more "human."

### Why Top Headlines news instead of keyword search?
Our AI does not need to know specific facts about AI ethics covered in academic papers — that is what the Philosophers API and Gutenberg cover. What it needs for the interview format is **whatever people are currently talking about** so it can simulate a real-time reaction. Top headlines from `world`, `technology`, and `science` categories provide this ambient awareness of the zeitgeist.

### Why is the Kafka streaming pipeline outside the Airflow DAG?
The hot-path producer and consumer are long-running processes that operate continuously, while Airflow is designed for batch tasks with defined start and end points. These are architecturally separate concerns: the DAG handles scheduled daily ingestion, while Kafka handles real-time event capture independently.

### Idempotency Strategy
Each script uses a different idempotency model appropriate for its data type:
- **Images, Books, Audio & Quotes:** `head_object()` pre-check — file is skipped entirely if it exists.
- **News & Stack Exchange snapshots:** Daily filename overwrite — the latest run always wins for the current day.
- **Philosopher metadata catalog:** Always overwritten — ensures the latest API truth is stored.
- **Wikipedia biographies:** Always overwritten — biographies can be edited; daily refresh is cheap.

### Data Volume Summary

| Source | Volume per run | Growth rate |
|---|---|---|
| Gutenberg texts | ~600 MB | Static (public domain, immutable) |
| Podcast audio | ~2 GB (40+ hours) | Per discovery run |
| News snapshots | ~150 KB/day | ~4.5 MB/month |
| Wikipedia biographies | ~100 KB | Static (daily overwrite) |
| Wikiquote quotes | ~50 KB | Static (idempotent) |
| Philosophy SE Q&A | ~2 MB/snapshot | ~60 MB/month (append mode) |
| Philosophers API | ~30 KB | Static (daily overwrite) |
| Hot path stream | Variable | Continuous (buffer-and-flush) |
