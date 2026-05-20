#!/usr/bin/env bash
# =============================================================================
# run.sh — End-to-end bootstrap for the Real-Time Transit Delay Detection stack.
#
# Performs (in order):
#   1. Ensure .env exists (copies from .env.example if missing)
#   2. Download Flink connector JARs into flink/lib/ (if missing)
#   3. Build & start the Docker Compose stack
#   4. Install Python dependencies into a local .venv
#   5. Apply database migrations (TimescaleDB)
#   6. Load GTFS static data into PostgreSQL
#   7. Start the live Kafka producer in the background (logs/producer.log)
#   8. Submit the PyFlink processing job to the Flink cluster
#
# Usage:
#   ./run.sh
#
# Logs:
#   logs/producer.log    — live Kafka producer stdout/stderr
#
# To stop everything:
#   ./stop.sh            (keeps data volumes)
#   ./stop.sh --clean    (wipes data volumes)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
log() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
warn() { printf '\033[1;33m!!  %s\033[0m\n' "$*"; }
die() { printf '\033[1;31mxx  %s\033[0m\n' "$*" >&2; exit 1; }

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

need_cmd docker
need_cmd curl
need_cmd python3

# -----------------------------------------------------------------------------
# Auto-detect this host's primary LAN IPv4 (best-effort; falls back to
# localhost). Used so Kafka's EXTERNAL listener advertises an address that
# clients on other machines can reach. Override with KAFKA_EXTERNAL_HOST in .env.
# -----------------------------------------------------------------------------
detect_host_ip() {
    local ip=""
    case "$(uname -s)" in
        Darwin)
            ip="$(ipconfig getifaddr en0 2>/dev/null || true)"
            [ -z "$ip" ] && ip="$(ipconfig getifaddr en1 2>/dev/null || true)"
            ;;
        Linux)
            ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
            [ -z "$ip" ] && ip="$(ip -4 -o addr show scope global 2>/dev/null \
                | awk '{print $4}' | cut -d/ -f1 | head -n1)"
            ;;
    esac
    [ -z "$ip" ] && ip="localhost"
    echo "$ip"
}

# -----------------------------------------------------------------------------
# 1. .env
# -----------------------------------------------------------------------------
log "Step 1/8 — Checking .env"
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "  Created .env from .env.example"
    else
        die ".env.example not found"
    fi
else
    echo "  .env already exists"
fi

# -----------------------------------------------------------------------------
# 2. Flink connector JARs
# -----------------------------------------------------------------------------
log "Step 2/8 — Downloading Flink connector JARs (if missing)"
mkdir -p flink/lib

declare -a JAR_NAMES=(
    "flink-sql-connector-kafka-3.3.0-1.20.jar"
    "flink-connector-jdbc-3.2.0-1.19.jar"
    "postgresql-42.7.3.jar"
)
declare -a JAR_URLS=(
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"
    "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
)

for i in "${!JAR_NAMES[@]}"; do
    name="${JAR_NAMES[$i]}"
    url="${JAR_URLS[$i]}"
    dest="flink/lib/${name}"
    if [ -f "$dest" ]; then
        echo "  [skip] $name"
    else
        echo "  [download] $name"
        curl -fSL -o "$dest" "$url"
    fi
done

# -----------------------------------------------------------------------------
# 3. Docker Compose stack
# -----------------------------------------------------------------------------
log "Step 3/8 — Building & starting Docker Compose stack on 0.0.0.0"

# Load .env so we can read/override KAFKA_EXTERNAL_HOST and GRAFANA_HOST_PORT
set -a
# shellcheck disable=SC1091
[ -f .env ] && source .env
set +a

if [ -z "${KAFKA_EXTERNAL_HOST:-}" ] || [ "${KAFKA_EXTERNAL_HOST}" = "localhost" ]; then
    KAFKA_EXTERNAL_HOST="$(detect_host_ip)"
    export KAFKA_EXTERNAL_HOST
    echo "  Auto-detected host IP for Kafka external listener: ${KAFKA_EXTERNAL_HOST}"
fi

# Build shared images once, sequentially. Building services with the same image
# tag in parallel can fail with "image already exists" on Docker Desktop.
docker compose build flink-jobmanager
docker compose build gtfs-matcher
docker compose up -d --no-build

log "Waiting for services to become healthy (max 5 min)..."
attempts=0
max_attempts=60
while true; do
    attempts=$((attempts + 1))
    unhealthy=$(docker compose ps --format '{{.Name}} {{.Status}}' \
        | grep -E 'starting|unhealthy' || true)
    if [ -z "$unhealthy" ]; then
        break
    fi
    if [ "$attempts" -ge "$max_attempts" ]; then
        docker compose ps
        die "Services did not become healthy in time"
    fi
    sleep 5
done
docker compose ps

# -----------------------------------------------------------------------------
# 4. Python deps in a local virtualenv
# -----------------------------------------------------------------------------
log "Step 4/8 — Installing Python dependencies into .venv"
if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -e ".[dev]"

# -----------------------------------------------------------------------------
# 5. Database migrations
# -----------------------------------------------------------------------------
log "Step 5/8 — Applying database migrations"
python scripts/migrate.py

# -----------------------------------------------------------------------------
# 6. GTFS static data load
# -----------------------------------------------------------------------------
log "Step 6/8 — Loading GTFS static data (TPBI Bucharest feed)"
python -m services.gtfs_ingestion.loader

# -----------------------------------------------------------------------------
# 7. Live Kafka producer (background)
# -----------------------------------------------------------------------------
log "Step 7/8 — Starting live Kafka producer in the background"
PRODUCER_PID_FILE="$LOG_DIR/producer.pid"
if [ -f "$PRODUCER_PID_FILE" ] && kill -0 "$(cat "$PRODUCER_PID_FILE")" 2>/dev/null; then
    warn "Producer already running (PID $(cat "$PRODUCER_PID_FILE")), skipping start"
else
    nohup python -m services.live_producer.producer \
        >"$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$PRODUCER_PID_FILE"
    echo "  Producer PID: $(cat "$PRODUCER_PID_FILE")  |  log: $LOG_DIR/producer.log"
fi

# -----------------------------------------------------------------------------
# 8. Submit Flink job
# -----------------------------------------------------------------------------
log "Step 8/8 — Submitting PyFlink processing job"
bash scripts/submit_flink_job.sh

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
cat <<EOF

\033[1;32m✓ Stack is up and running on 0.0.0.0 (all interfaces).\033[0m

Endpoints (reachable from any host on the network):
  - Flink Web UI : http://${KAFKA_EXTERNAL_HOST}:8081
  - Transit UI   : http://${KAFKA_EXTERNAL_HOST}:${TRANSIT_UI_HOST_PORT:-8000}
  - Grafana      : http://${KAFKA_EXTERNAL_HOST}:${GRAFANA_HOST_PORT:-3000}   (admin / admin)
  - TimescaleDB  : psql -h ${KAFKA_EXTERNAL_HOST} -p 5433 -U transit -d transit
  - Kafka        : ${KAFKA_EXTERNAL_HOST}:9094

Background processes:
  - Live producer  : PID \$(cat $PRODUCER_PID_FILE 2>/dev/null || echo '?')
                     log → $LOG_DIR/producer.log

To stop:
  ./stop.sh           # keep data
  ./stop.sh --clean   # wipe data volumes

(The producer must be stopped manually with: kill \$(cat $PRODUCER_PID_FILE))
EOF
