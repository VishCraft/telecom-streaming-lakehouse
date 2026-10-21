# Telecom Streaming Lakehouse

A portfolio project that demonstrates a streaming data pipeline for synthetic telecom network events. Kafka carries events, PySpark Structured Streaming validates and deduplicates them, and Delta Lake stores bronze, silver, quarantine, and gold data.

## Architecture

```text
Synthetic event producer
          │
          ▼
     Kafka topic
          │
          ├── Bronze: raw Kafka payloads and offsets in Delta
          │
          └── Parse and validate
                 ├── Silver: valid, deduplicated events in Delta
                 ├── Quarantine: invalid events with rejection reason
                 └── Gold: one-minute event-time KPIs by region and metric
```

The pipeline validates required fields, supported metric names, and metric-specific value ranges. Silver uses a two-minute event-time watermark and deduplicates by `event_id`. Gold reads the deduplicated silver Delta stream, applies event-time windows, and writes one-minute aggregates. Each streaming query has its own checkpoint directory.

## Tech stack

- Apache Kafka 4.3.1 in single-node KRaft mode
- PySpark Structured Streaming 4.2.0
- Delta Lake 4.4.1
- Docker Compose
- Python synthetic event generator

## Prerequisites

- Docker Desktop or Docker Engine with the Compose plugin
- Git, if you plan to publish the project
- Internet access for pulling the container image and downloading Spark connector packages on first start

The example pins Spark 3.5.6 with Delta Lake 4.4.1, a compatible pair. The Spark Kafka connector is resolved at the same Spark version.

## Run locally

From this repository directory:

```bash
docker compose up -d --build kafka topic-init pipeline
```

Wait for the `pipeline` container to start its queries, then generate synthetic events:

```bash
docker compose --profile tools run --rm producer
```

The producer sends 300 events over about 2.5 minutes by default. Some event IDs repeat intentionally, and event timestamps can arrive out of order. Gold output is written after the watermark closes event-time windows, so allow a few minutes for the first aggregate rows.

Follow logs:

```bash
docker compose logs -f pipeline
```

Stop the services:

```bash
docker compose down
```

Delta tables and checkpoints are written under `data/` on the host. To reset the demo and replay from a clean state, stop the services and remove that generated directory:

```bash
docker compose down
rm -rf data/
```

## Data layout

After the pipeline runs, the local `data/` directory contains:

```text
data/
├── bronze/events/       # Raw Kafka payload, topic, partition, offset, ingest timestamp
├── silver/events/       # Parsed, valid, deduplicated network events
├── quarantine/events/   # Rejected events and quality reason
├── gold/kpi_1m/         # One-minute aggregates by region and metric
└── checkpoints/         # Independent Spark streaming checkpoints
```

Each location is a Delta table. You can inspect the Delta transaction logs and Parquet files with Spark, or open the project with Databricks and adapt the paths to cloud object storage.

## Event schema

```json
{
  "event_id": "unique-or-repeated-id",
  "event_time": "2026-10-21T18:30:00Z",
  "region": "north",
  "node_id": "node-014",
  "metric": "latency_ms",
  "value": 42.7
}
```

Supported metrics and demo ranges:

| Metric            | Accepted range |
| ----------------- | -------------: |
| `rssi`            |      -200 to 0 |
| `latency_ms`      |    0 to 60,000 |
| `throughput_mbps` | 0 to 1,000,000 |

These ranges are illustrative; a real pipeline should use domain-approved definitions and thresholds.

## Project structure

```text
app/streaming_pipeline.py   # Kafka → Delta streaming pipeline
scripts/produce_events.py   # Synthetic event generator
Dockerfile                  # Spark application image
compose.yaml                # Kafka, topic setup, pipeline, producer
requirements.txt            # Pinned Python dependencies
.env.example                # Optional local configuration template
.gitignore                  # Excludes generated data and local files
LICENSE                     # MIT license
```

## Configuration

The defaults work with Docker Compose. Optional environment variables are documented in `.env.example`. The pipeline uses `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, and `DATA_DIR`.

## Publish to GitHub

1. Create a new empty repository on GitHub, for example `telecom-streaming-lakehouse`. Do not initialize it with a README or license because those files are already included here.
2. From this project directory, set your Git identity if it is not already configured, then initialize and commit:

   ```bash
   git init
   git add .
   git status
   git commit -m "Build telecom streaming lakehouse demo"
   git branch -M main
   ```

3. Connect the new GitHub repository and push. Replace the URL with the one GitHub shows for your account:

   ```bash
   git remote add origin https://github.com/YOUR-USERNAME/telecom-streaming-lakehouse.git
   git push -u origin main
   ```

4. In GitHub, add a short repository description such as: `Kafka, PySpark Structured Streaming, and Delta Lake demo for synthetic telecom network events.`
5. Add a README screenshot only after you have run the demo and captured your own output. Do not commit the generated `data/` folder; it is excluded by `.gitignore`.

## Resume wording after you build and verify it

**Telecom Streaming Lakehouse | Kafka, PySpark, Databricks, Delta Lake**  
Built a streaming pipeline for synthetic network events with bronze, silver, and gold layers; implemented event-time windows, watermark-based deduplication, data-quality validation, and quarantine handling.

Only use this bullet after you have run, understood, and can explain the project. In this repository, Databricks is an optional adaptation target; the local demo runs Spark in Docker.

## Possible next steps

- Adapt the pipeline to a Databricks workspace and use Unity Catalog for governed tables.
- Add automated checks for schema drift, duplicate rates, and late-arriving events.
- Add CI that runs Python linting and validates the Compose configuration.
- Add a dashboard that reads the gold Delta output.
