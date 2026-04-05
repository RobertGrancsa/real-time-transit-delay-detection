# Real-Time Public Transit Delay Detection for Bucharest

A smart-city stream-processing system that monitors the Bucharest public transit network in real time. It ingests live GPS vehicle telemetry from the TPBI open-data API, compares it against planned GTFS schedules, and surfaces delay metrics, route bunching events, service gap alerts, and speed analytics through auto-provisioned Grafana dashboards.

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Data Sources](#data-sources)
- [Grafana Dashboards](#grafana-dashboards)
- [Team Contributions](#team-contributions)

---

## Architecture

```
┌──────────────┐    JSON/10s     ┌──────────────┐   transit-live-   ┌──────────────────┐
│  TPBI mo-bi  │ ──────────────► │    Kafka      │   telemetry      │   Apache Flink   │
│  Live API    │   (aiohttp)     │  (KRaft 4.0)  │ ───────────────► │  (PyFlink 1.20)  │
└──────────────┘                 └──────────────┘                   └────────┬─────────┘
                                                                             │
┌──────────────┐    Bulk SQL     ┌──────────────┐    JDBC sinks     ┌────────▼─────────┐
│  TPBI GTFS   │ ──────────────► │ TimescaleDB  │ ◄──────────────── │  5 Analytics     │
│  Static Feed │   (psycopg2)    │  (PG 16)     │                   │  Hypertables     │
└──────────────┘                 └──────┬───────┘                   └──────────────────┘
                                        │
                                        │  SQL views +
                                        │  continuous aggregates
                                        ▼
                                 ┌──────────────┐
                                 │   Grafana     │
                                 │  3 Dashboards │
                                 └──────────────┘
```

**Data flow:** The live producer polls vehicle positions every 10 seconds from the TPBI REST API, sanitizes them, and publishes to a Kafka topic. Apache Flink consumes the stream, applies tumbling-window aggregations (1-min delay, 30-sec bunching, 10-min gap detection, 15-min speed stats), and sinks results into five TimescaleDB hypertables via JDBC. Grafana reads pre-built SQL views and continuous aggregates to power three dashboards.

---

## Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Message Broker | Apache Kafka (KRaft) | 4.0.2 | Reliable event streaming, no Zookeeper |
| Stream Processor | Apache Flink (PyFlink) | 1.20.3 | Event-time windowed analytics |
| Database | TimescaleDB | latest-pg16 | Time-series storage with hypertables & continuous aggregates |
| Visualization | Grafana | latest | Auto-provisioned dashboards |
| Language | Python | ≥ 3.10 | asyncio producer, GTFS loader, PyFlink jobs |
| Orchestration | Docker Compose | 3.9 | Single-command local deployment |

---

## Project Structure

```
public-transport-delay-detection/
├── docker-compose.yml              # Full infrastructure (5 services)
├── pyproject.toml                  # Python project metadata & dependencies
├── .env.example                    # Environment variable template
│
├── config/
│   └── settings.py                 # Centralized env-var configuration (frozen dataclasses)
│
├── services/
│   ├── gtfs_ingestion/
│   │   └── loader.py               # Downloads & bulk-loads TPBI GTFS into PostgreSQL
│   ├── live_producer/
│   │   └── producer.py             # Async Kafka producer polling live vehicle API
│   └── flink_jobs/
│       └── processing_job.py       # PyFlink pipeline: Kafka → windows → JDBC sinks
│
├── sql/
│   ├── init.sql                    # TimescaleDB extension + transit schema bootstrap
│   └── migrations/
│       ├── 001_gtfs_tables.sql     # 7 GTFS relational tables (agency → stop_times)
│       ├── 002_analytics_tables.sql# 5 hypertables with retention policies
│       ├── 003_grafana_views.sql   # 7 dashboard-ready views
│       └── 004_continuous_aggregates.sql  # 6 auto-refreshing materialized views
│
├── scripts/
│   ├── migrate.py                  # Migration runner with ledger tracking
│   ├── download_flink_jars.{sh,ps1}# Download Flink connector JARs
│   └── submit_flink_job.{sh,ps1}  # Submit PyFlink job to cluster
│
├── grafana/provisioning/
│   ├── datasources/postgres.yml    # Auto-configured TimescaleDB datasource
│   └── dashboards/
│       ├── dashboards.yml          # Dashboard provisioning config
│       └── json/
│           ├── fleet-overview.json     # Live map + KPIs + route status
│           ├── delay-analytics.json    # Delay trends + speed analysis
│           └── anomaly-detection.json  # Bunching + service gaps + reliability
│
└── flink/lib/                      # Flink connector JARs (downloaded via script)
```

---

## Prerequisites

- **Docker** ≥ 24.0 and **Docker Compose** v2
- **Python** ≥ 3.10 (for running services outside containers)
- ~4 GB free RAM (Kafka + Flink + TimescaleDB + Grafana)

---

## Getting Started

### 1. Clone and configure

```bash
git clone <repository-url>
cd public-transport-delay-detection
cp .env.example .env          # edit if you want custom credentials
```

### 2. Download Flink connector JARs

```bash
# Linux / macOS
./scripts/download_flink_jars.sh

# Windows (PowerShell)
.\scripts\download_flink_jars.ps1
```

This downloads the Kafka connector, JDBC connector, and PostgreSQL driver into `flink/lib/`.

### 3. Start the infrastructure

```bash
docker compose up -d
```

Wait for all services to become healthy:

```bash
docker compose ps
```

| Service | Port | URL |
|---------|------|-----|
| Kafka | 9092 | — |
| TimescaleDB | 5432 | `psql -h localhost -U transit -d transit` |
| Flink Web UI | 8081 | http://localhost:8081 |
| Grafana | 3000 | http://localhost:3000 (admin / admin) |

### 4. Install Python dependencies

```bash
pip install -e ".[dev]"
```

### 5. Run database migrations

```bash
python scripts/migrate.py
```

This applies all four SQL migrations in order (GTFS tables → analytics hypertables → Grafana views → continuous aggregates). Use `--status` to check what's applied, `--dry-run` to preview.

### 6. Load GTFS static data

```bash
python -m services.gtfs_ingestion.loader
```

Downloads the TPBI Bucharest Region GTFS feed (~8.7 MB) and bulk-loads 7 tables (agencies, routes, stops, trips, stop_times, calendar, shapes).

### 7. Start the live Kafka producer

```bash
python -m services.live_producer.producer
```

Polls `https://maps.mo-bi.ro/api/busData` every 10 seconds and publishes sanitized vehicle positions to the `transit-live-telemetry` Kafka topic.

### 8. Submit the Flink processing job

```bash
# Linux / macOS
./scripts/submit_flink_job.sh

# Windows (PowerShell)
.\scripts\submit_flink_job.ps1
```

This installs PyFlink in the JobManager container, copies connector JARs, and submits the processing pipeline. Monitor it at http://localhost:8081.

### 9. Open Grafana

Navigate to http://localhost:3000 (default credentials: `admin` / `admin`). Three dashboards are auto-provisioned under **Transit Dashboards**.

---

## Data Sources

### GTFS Static Feed
- **Provider:** TPBI (Transportul Public Bucuresti-Ilfov)
- **URL:** `https://gtfs.tpbi.ro/regional/BUCHAREST-REGION.zip`
- **Contents:** 199 routes, 4,552 stops, 61,438 trips, 404 shapes
- **Agencies:** STB SA, STV SA, STCM, METROREX SA, Regio Serv Transport
- **Format:** Standard GTFS (CSV files in ZIP)

### Live Vehicle Positions API
- **URL:** `https://maps.mo-bi.ro/api/busData`
- **Auth:** None required (public open-data endpoint)
- **Format:** JSON array of vehicle objects with GPS coordinates, route, direction, and timestamp
- **Update frequency:** Polled every 10 seconds

---

## Grafana Dashboards

All dashboards support template variables for filtering by vehicle type (Bus, Tram, Metro, Trolleybus) and specific route IDs.

### Fleet Overview
Live operational snapshot of the entire transit network.
- **6 KPI stat panels:** active vehicles, active routes, average network delay, average speed, bunching events (30 min), service gaps (60 min)
- **Geomap panel:** real-time vehicle positions on OpenStreetMap centered on Bucharest
- **Route status table:** per-route delay with severity coloring (normal / warning / critical)
- **Speed summary table:** per-route speed with traffic status (normal / slow / congested)
- **Daily fleet activity chart:** vehicle and route counts from continuous aggregates

### Delay Analytics
Historical delay trends and schedule adherence analysis.
- **Hourly delay time-series:** average and peak delay per route with smooth interpolation
- **Vehicle observation counts:** stacked bars showing data volume per route
- **Daily delay bar chart:** top 20 worst-performing routes
- **Daily delay heatmap:** route × day state-timeline with color gradient
- **Hourly speed trends:** per-route speed over time
- **Slowest routes ranking:** bottom 15 routes by average speed

### Anomaly Detection
Route bunching, service gaps, and reliability scoring.
- **Bunching events table:** live alerts with gap duration and color coding
- **Bunching events map:** triangle markers on Bucharest geomap
- **Bunching frequency chart:** hourly stacked bar chart per route
- **Service gap alerts table:** duration severity with rolling 60-min window
- **Reliability score time-series:** gap count per route over time
- **Total gap duration chart:** cumulative downtime per route
- **Most unreliable routes ranking:** top 10 by total service gaps

---

## Team Contributions

### Robert Grancsa

Responsible for **infrastructure architecture and database design**. Set up the Docker Compose orchestration with all five services (Kafka KRaft, Flink JobManager/TaskManager, TimescaleDB, Grafana), including health checks, volumes, and networking. Designed and implemented all database schemas: the GTFS relational tables (`001_gtfs_tables.sql`), the five analytics hypertables with TimescaleDB retention policies (`002_analytics_tables.sql`), and the seven Grafana-ready SQL views (`003_grafana_views.sql`). Built the Fleet Overview dashboard with the live Geomap panel, KPI stats, and route status tables.

**Key commits:**
- `docs: add project README and proposal`
- `infra: add Docker Compose with Kafka, Flink, TimescaleDB, and Grafana`
- `feat(db): add GTFS static schema migration for transit tables`
- `feat(db): add analytics output tables for Flink pipeline (TimescaleDB hypertables)`
- `feat(db): add Grafana-ready SQL views for dashboard panels`
- `feat(grafana): add Fleet Overview dashboard`

### Ana-Maria Toader (Anatoad)

Responsible for **data ingestion, stream processing, and historical analytics**. Implemented the GTFS static data loader that downloads, parses, and bulk-loads the TPBI feed into PostgreSQL with proper handling of GTFS >24h timestamps. Developed the core PyFlink real-time processing pipeline with event-time watermarks, tumbling-window aggregations, temporal self-joins for bunching detection, and JDBC sinks. Created the TimescaleDB continuous aggregates for hourly and daily historical trend rollups. Built the Delay Analytics dashboard with time-series, heatmaps, and speed analysis panels.

**Key commits:**
- `chore: add .gitignore for Python, Docker, and IDE artifacts`
- `infra: add Grafana datasource and dashboard provisioning`
- `feat(ingestion): add GTFS static data loader for TPBI Bucharest feed`
- `feat(flink): implement PyFlink real-time processing pipeline`
- `feat(db): add TimescaleDB continuous aggregates for historical trends`
- `feat(grafana): add Delay Analytics dashboard`

### Rares-Alexandru Constantin (RaresCon)

Responsible for **real-time data production, deployment tooling, and anomaly visualization**. Built the async Kafka producer that polls the TPBI live vehicle API via aiohttp, sanitizes payloads, and publishes to Kafka with idempotent delivery and LZ4 compression. Created the Flink connector JAR download scripts and the Docker Compose configuration for mounting PyFlink jobs with forwarded environment variables. Developed the SQL migration runner with ledger tracking, dry-run, and status modes. Built the Anomaly Detection dashboard with bunching events, service gap alerts, and reliability scoring panels.

**Key commits:**
- `build: add pyproject.toml and environment config template`
- `infra: add Flink connector JAR download scripts (bash + PowerShell)`
- `feat(producer): add async Kafka producer for live vehicle telemetry`
- `infra: mount Flink jobs volume and forward env vars for PyFlink pipeline`
- `feat(ops): add SQL migration runner with ledger tracking`
- `feat(grafana): add Anomaly Detection dashboard`

### Andrei-Daniel Anghelescu (GemDeKaise)

Responsible for **configuration management, project scaffolding, and operational tooling**. Created the centralized settings module with frozen dataclasses and environment-variable resolution for all service configurations (Postgres, Kafka, Live API, GTFS, Flink). Scaffolded the Python service packages and module structure. Managed build dependencies including adding the `requests` library for GTFS download support. Created the Flink job submission scripts for both bash and PowerShell. Fixed the Grafana datasource UID provisioning for dashboard cross-references.

**Key commits:**
- `feat: add centralized settings module with env-var config`
- `chore: scaffold service packages for ingestion, producer, and Flink jobs`
- `build: add requests dependency for GTFS download`
- `ops: add Flink job submission scripts (bash + PowerShell)`
- `fix(grafana): add UID to datasource provisioning for dashboard references`
