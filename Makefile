# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BDM P1+P2 — common operations
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# `make help` for the full list.
#
# All targets are container-aware: nothing here assumes the host has Python,
# Spark or Java installed — every script runs inside the airflow-scheduler
# image (which bundles PySpark 3.5 + Java 17 + the embedding stack).

.PHONY: help env-check build up down ps logs \
        ingest-p1 trusted exploit episode \
        stream-up stream-down \
        verify-trusted verify-exploit verify-milvus \
        dashboard report clean

# ─── help ────────────────────────────────────────────────────────────────────
help:
	@echo "BDM P1+P2 — common operations"
	@echo
	@echo "FIRST-TIME SETUP"
	@echo "  make env-check          Verify .env exists and Docker is running"
	@echo "  make build              Build the custom airflow-spark image"
	@echo "  make up                 Bring up the full 12-container stack"
	@echo
	@echo "DAILY OPS"
	@echo "  make ps                 Show running containers"
	@echo "  make logs s=<service>   Tail logs for one service"
	@echo "  make down               Stop the stack (data persists)"
	@echo "  make clean              Stop + delete volumes (WIPES Milvus + Airflow DB)"
	@echo
	@echo "PIPELINE"
	@echo "  make ingest-p1          Run the P1 cold-path ingestion (all 8 sources)"
	@echo "  make trusted            Run the trusted-zone cleaning DAG"
	@echo "  make exploit            Run the exploitation-zone DAG (incl. Milvus embedding)"
	@echo "  make verify-trusted     Sanity-check trusted.duckdb"
	@echo "  make verify-exploit     Sanity-check exploit.duckdb"
	@echo "  make verify-milvus      Sanity-check Milvus corpus_chunks"
	@echo
	@echo "STREAMING"
	@echo "  make stream-up          Start producer + Spark Structured Streaming"
	@echo "  make stream-down        Stop them"
	@echo
	@echo "CONSUMPTION"
	@echo "  make episode FIG=kant   Generate one podcast episode (requires ANTHROPIC_API_KEY)"
	@echo "  make dashboard          Print the dashboard URL"
	@echo
	@echo "DOCS"
	@echo "  make report             Rebuild documents/P2_TECHNICAL_REPORT.pdf"

# ─── prerequisites ──────────────────────────────────────────────────────────
env-check:
	@test -f .env || (echo "❌ .env missing. Run: cp .env.example .env && edit the API keys." && exit 1)
	@docker info > /dev/null 2>&1 || (echo "❌ Docker is not running." && exit 1)
	@echo "✓ .env present, Docker running."

# ─── stack lifecycle ────────────────────────────────────────────────────────
build: env-check
	docker compose build airflow-webserver
	@echo "✓ Custom airflow-spark image built."

up: env-check
	docker compose up -d
	@echo
	@echo "Stack is starting. First boot needs ~60-90 s for health checks."
	@echo "  - Airflow:   http://localhost:8080  (admin / admin)"
	@echo "  - MinIO:     http://localhost:9001  (admin / password)"
	@echo "  - Kafka UI:  http://localhost:8085"
	@echo "  - Streamlit: http://localhost:8501"

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf duckdb/*.duckdb streaming/fact_mentions_1m streaming/checkpoints
	@echo "✓ Stack down, volumes removed, DuckDB and streaming artifacts deleted."
	@echo "  Note: landing_zone/ (MinIO data) is on the host — delete manually if needed."

ps:
	@docker compose ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

logs:
	@test -n "$(s)" || (echo "Usage: make logs s=<service>" && exit 1)
	docker compose logs -f --tail=100 $(s)

# ─── pipeline (each runs the corresponding DAG via Airflow CLI) ─────────────
ingest-p1:
	docker exec airflow-scheduler airflow dags trigger bdm_p1_cold_path_ingestion

trusted:
	docker exec airflow-scheduler airflow dags trigger bdm_p2_trusted_zone

exploit:
	docker exec airflow-scheduler airflow dags trigger bdm_p2_exploitation_zone

# ─── verifiers (idempotent sanity checks, run anytime) ──────────────────────
verify-trusted:
	docker exec --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && python scripts/verify_trusted_zone.py'

verify-exploit:
	docker exec --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && python scripts/verify_exploit_zone.py'

verify-milvus:
	docker exec --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && python scripts/verify_corpus_chunks.py'

# ─── streaming ──────────────────────────────────────────────────────────────
stream-up:
	docker exec -d --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
	   python ingestion/stream_producer.py > /tmp/producer.log 2>&1'
	docker exec -d --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && mkdir -p streaming/checkpoints && \
	   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
	   ingestion/spark_stream_mentions_1m.py > /tmp/spark_stream.log 2>&1'
	@echo "✓ Producer + Spark streaming started in background."
	@echo "  Refresh fact_mentions_1m view periodically:"
	@echo "    make exploit  (or run exploitation/structured/fact_mentions_1m.py directly)"

stream-down:
	@docker exec airflow-scheduler bash -lc \
	  'for pid in $$(ls /proc | grep -E "^[0-9]+$$"); do \
	     cmd=$$(cat /proc/$$pid/cmdline 2>/dev/null | tr "\0" " "); \
	     echo "$$cmd" | grep -qE "stream_producer|spark_stream|spark-submit" && \
	       kill $$pid 2>/dev/null && echo "killed $$pid"; \
	   done' 2>&1 || true
	@echo "✓ Streaming stopped."

# ─── consumption ────────────────────────────────────────────────────────────
episode:
	@test -n "$(FIG)" || (echo "Usage: make episode FIG=<figure_slug>" && exit 1)
	docker exec --user airflow airflow-scheduler bash -lc \
	  'cd /opt/airflow && python -m consumption.episode --figure $(FIG) --live'

dashboard:
	@echo "Streamlit dashboard:  http://localhost:8501"

# ─── docs ───────────────────────────────────────────────────────────────────
report:
	.venv/bin/python scripts/build_p2_report.py
