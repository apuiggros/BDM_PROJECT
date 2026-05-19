# Pipeline architecture — diagram brief

Hand the prompt below to Claude (or any diagramming AI) to produce the
architecture schema. It encodes the **implemented** pipeline, not the
original design.

---

You are a systems architect. Produce a clean, presentation-quality
ARCHITECTURE DIAGRAM for the data pipeline described below. Output an
editable diagram (Mermaid `flowchart LR`, and also an equivalent
Excalidraw/draw.io-style description). Group nodes into clearly bounded
zones with labeled containers, distinguish the batch path from the
streaming path with line styles, and put the technology name on every
box. Keep it readable on one slide.

PROJECT
"Historical Conversational AI" — a Big Data Management pipeline that powers
a chatbot able to converse in the voice of 9 historical figures: 5
philosophers (Plato, Descartes, Kant, Hegel, Nietzsche), 2 scientists
(Einstein, Darwin), 2 literary authors (Wilde, Twain).

ARCHITECTURE STYLE
A 3-store lakehouse following zone separation:
  • MinIO  (S3-compatible) — raw + cleaned UNSTRUCTURED bytes
  • DuckDB — ALL tabular data (Trusted + Exploitation)
  • Milvus — vector embeddings for RAG
(MongoDB was deliberately removed — do not show it.)

DATA SOURCES (batch unless noted)
  1. Philosophers API — biographies/dates (philosophers only)
  2. Wikipedia REST — figure summaries (all 9)
  3. Wikiquote — quotes by/about each figure
  4. GNews — news articles by category (world/science/technology)
  5. Philosophy Stack Exchange API — questions + answers
  6. Project Gutenberg (Gutendex) — book catalog + full book texts
  7. Podcast audio listing — episode manifest
  8. Philosopher images
  9. STREAMING: live figure-mention stream → Kafka (hot path)

ZONE 1 — LANDING ZONE
  • Store: MinIO bucket `landing-zone` (raw JSON snapshots, raw book .txt,
    images, audio)
  • Filled by an Airflow "P1 cold-path" ingestion DAG (scheduled daily)

ZONE 2 — TRUSTED ZONE   (Airflow DAG: bdm_p2_trusted_zone, manual trigger)
  • Spark (local mode) cleaning jobs read Landing Zone, write typed tables
    to DuckDB `trusted.duckdb`: trusted_philosophers, trusted_wikipedia,
    trusted_wikiquote_quotes, trusted_news_articles, trusted_se_questions,
    trusted_se_answers, trusted_gutenberg_books, trusted_podcast_episodes,
    trusted_philosopher_images
  • Gutenberg book texts: Project Gutenberg boilerplate stripped, cleaned
    bytes written BACK to MinIO under `gutenberg/cleaned_text/`
  • A data-quality verifier task gates the zone
  • STREAMING (hot path): Kafka → Spark Structured Streaming → Parquet
    table `fact_mentions_1m` (1-min windows: mention_count, avg_sentiment)

ZONE 3 — EXPLOITATION ZONE  (Airflow DAG: bdm_p2_exploitation_zone,
auto-triggered the moment the Trusted Zone DAG finishes green)
  • Pure DuckDB SQL STAR SCHEMA in `exploit.duckdb`:
      dim_figure (conformed dimension, 9 figures)
      └─ fact_works, fact_quotes, fact_news_articles, fact_se_qa
  • Milvus collection `corpus_chunks` built by a SPARK embedding job:
      parallel MinIO read + chunk of ~161 cleaned books, plus Wikipedia /
      Wikiquote / figure-linked Stack-Exchange text; embedded with
      all-MiniLM-L6-v2 (384-dim, COSINE/HNSW); chunk text stored inline
  • fact_mentions_1m surfaced from the streaming Parquet
  • Two verifier gates: DuckDB star schema + Milvus collection

ZONE 4 — CONSUMPTION
  • Streamlit dashboard: figure KPIs, news timeline, live streaming mentions
  • 3-agent chatbot:
      – Interviewer: recent news + LLM relevance reasoning
      – Reasoner: Milvus RAG + DuckDB identity card
      – Voice: style exemplars via Milvus metadata filter (Gutenberg /
        Wikiquote by_figure)

ORCHESTRATION & INFRA (show as a sidebar/footer band, not inline boxes)
  • Apache Airflow (LocalExecutor) — DAG chain:
      P1 cold-path (daily) → bdm_p2_trusted_zone (manual)
      → auto-trigger → bdm_p2_exploitation_zone
  • All services via docker-compose: MinIO, Airflow + Postgres,
    Kafka + Zookeeper + Kafka-UI, Milvus standalone (milvus + etcd +
    its own MinIO)

DIAGRAM REQUIREMENTS
  • Left-to-right flow: Sources → Landing → Trusted → Exploitation →
    Consumption, each in its own labeled rounded container.
  • Solid arrows = batch flow; dashed arrows = streaming (Kafka) path.
  • Color-code by store: one color for MinIO nodes, one for DuckDB, one
    for Milvus, so the 3-store lakehouse reads at a glance.
  • Tag each processing node with its engine (Spark / DuckDB SQL / Airflow).
  • Show the auto-trigger between the two Airflow DAGs explicitly.
  • Keep it clean and uncluttered — this goes on a project slide.
