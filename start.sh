#!/usr/bin/env bash
# =============================================================================
# start.sh — Start the Docker Compose stack (Kafka, TimescaleDB, Flink, Grafana).
#
# All host ports are bound on 0.0.0.0 so the services are reachable from any
# network interface. Kafka also advertises this machine's LAN IP to external
# clients (auto-detected, override with KAFKA_EXTERNAL_HOST in .env).
#
# Usage:
#   ./start.sh
# =============================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

# -----------------------------------------------------------------------------
# Auto-detect this host's primary LAN IPv4 (best-effort; falls back to
# localhost). Used so Kafka's EXTERNAL listener advertises an address that
# clients on other machines can reach.
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

set -a
# shellcheck disable=SC1091
[ -f .env ] && source .env
set +a

if [ -z "${KAFKA_EXTERNAL_HOST:-}" ] || [ "${KAFKA_EXTERNAL_HOST}" = "localhost" ]; then
    KAFKA_EXTERNAL_HOST="$(detect_host_ip)"
    export KAFKA_EXTERNAL_HOST
    echo "==> Auto-detected host IP for Kafka external listener: ${KAFKA_EXTERNAL_HOST}"
fi

echo "==> Building Docker images..."
# Build shared images once, sequentially. Building services with the same image
# tag in parallel can fail with "image already exists" on Docker Desktop.
docker compose build flink-jobmanager
docker compose build gtfs-matcher

echo "==> Starting infrastructure on 0.0.0.0 (docker compose up -d --no-build)..."
docker compose up -d --no-build

echo
echo "==> Waiting for services to become healthy..."
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
        echo "!! Timed out waiting for services after $((attempts * 5))s. Current status:"
        docker compose ps
        exit 1
    fi
    sleep 5
done

echo
echo "==> All services are up. Status:"
docker compose ps

cat <<EOF

Endpoints (bound on 0.0.0.0 — reachable from any host on the network):
  - Kafka            : ${KAFKA_EXTERNAL_HOST}:9094
  - TimescaleDB      : psql -h ${KAFKA_EXTERNAL_HOST} -p 5433 -U transit -d transit
  - Flink Web UI     : http://${KAFKA_EXTERNAL_HOST}:8081
  - Transit UI       : http://${KAFKA_EXTERNAL_HOST}:${TRANSIT_UI_HOST_PORT:-8000}
  - Grafana          : http://${KAFKA_EXTERNAL_HOST}:${GRAFANA_HOST_PORT:-3000}  (admin / admin)

(Also reachable as localhost:<port> from this machine.)
EOF
