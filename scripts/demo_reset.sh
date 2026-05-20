#!/usr/bin/env bash
# Prepare a full local demo of the transit delay detection stack.
#
# Usage:
#   ./scripts/demo_reset.sh
#   ./scripts/demo_reset.sh --reset --force
#   ./scripts/demo_reset.sh --check-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

RESET=false
FORCE=false
RELOAD_GTFS=false
SKIP_FLINK_SUBMIT=false
SKIP_BUILD=false
CHECK_ONLY=false
TIMEOUT_SECONDS=900
MIN_ROWS=1

GRAFANA_URL="http://localhost:3000"
GTFS_DASHBOARD_URL="${GRAFANA_URL}/d/gtfs-delay-predictions/gtfs-delay-and-predictions"
FLINK_URL="http://localhost:8081"

print_usage() {
    cat <<'EOF'
Usage: ./scripts/demo_reset.sh [options]

Options:
  --reset, -Reset                  Remove Docker Compose containers and volumes first.
  --force, -Force                  Skip reset confirmation.
  --reload-gtfs, -ReloadGtfs       Load GTFS even when routes already exist.
  --skip-flink-submit, -SkipFlinkSubmit
                                   Do not submit a new Flink job.
  --skip-build, -SkipBuild         Do not build Docker images.
  --check-only, -CheckOnly         Print planned phases without changing anything.
  --timeout-seconds, -TimeoutSeconds SECONDS
                                   Seconds to wait for containers and rows. Default: 900.
  --min-rows, -MinRows COUNT       Minimum row count for live tables. Default: 1.
  --help, -h                       Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reset|-Reset)
            RESET=true
            shift
            ;;
        --force|-Force)
            FORCE=true
            shift
            ;;
        --reload-gtfs|-ReloadGtfs)
            RELOAD_GTFS=true
            shift
            ;;
        --skip-flink-submit|-SkipFlinkSubmit)
            SKIP_FLINK_SUBMIT=true
            shift
            ;;
        --skip-build|-SkipBuild)
            SKIP_BUILD=true
            shift
            ;;
        --check-only|-CheckOnly)
            CHECK_ONLY=true
            shift
            ;;
        --timeout-seconds|-TimeoutSeconds)
            TIMEOUT_SECONDS="${2:?Missing value for $1}"
            shift 2
            ;;
        --timeout-seconds=*|-TimeoutSeconds=*)
            TIMEOUT_SECONDS="${1#*=}"
            shift
            ;;
        --min-rows|-MinRows)
            MIN_ROWS="${2:?Missing value for $1}"
            shift 2
            ;;
        --min-rows=*|-MinRows=*)
            MIN_ROWS="${1#*=}"
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            print_usage >&2
            exit 2
            ;;
    esac
done

if ! [[ "${TIMEOUT_SECONDS}" =~ ^[0-9]+$ ]]; then
    echo "--timeout-seconds must be a positive integer." >&2
    exit 2
fi

if ! [[ "${MIN_ROWS}" =~ ^[0-9]+$ ]]; then
    echo "--min-rows must be a positive integer." >&2
    exit 2
fi

write_step() {
    printf '\n==> %s\n' "$1"
}

write_ok() {
    printf '[ok] %s\n' "$1"
}

run_step() {
    local message="$1"
    shift
    write_step "${message}"
    "$@"
}

require_command() {
    local name="$1"
    if ! command -v "${name}" >/dev/null 2>&1; then
        echo "Required command not found on PATH: ${name}" >&2
        exit 1
    fi
}

sql_scalar() {
    local sql="$1"
    docker exec transit-timescaledb psql -U transit -d transit -t -A -c "${sql}" | head -n 1 | tr -d '[:space:]'
}

wait_container_healthy() {
    local container_name="$1"
    local timeout="${2:-${TIMEOUT_SECONDS}}"
    local deadline=$(( $(date +%s) + timeout ))
    local state=""

    while [[ $(date +%s) -lt ${deadline} ]]; do
        state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_name}" 2>/dev/null || true)"
        if [[ "${state}" == "healthy" || "${state}" == "running" ]]; then
            write_ok "${container_name} is ${state}"
            return 0
        fi
        sleep 5
    done

    echo "Timed out waiting for ${container_name} to become healthy or running." >&2
    exit 1
}

wait_table_rows() {
    local table_name="$1"
    local time_column="${2:-}"
    local minimum_rows="${3:-${MIN_ROWS}}"
    local timeout="${4:-${TIMEOUT_SECONDS}}"
    local deadline=$(( $(date +%s) + timeout ))
    local where_clause=""
    local count="0"

    if [[ -n "${time_column}" ]]; then
        where_clause=" WHERE ${time_column} >= NOW() - INTERVAL '2 hours'"
    fi

    while [[ $(date +%s) -lt ${deadline} ]]; do
        count="$(sql_scalar "SELECT COUNT(*) FROM ${table_name}${where_clause};")"
        count="${count//[^0-9]/}"
        count="${count:-0}"
        if (( count >= minimum_rows )); then
            write_ok "${table_name} has ${count} row(s)"
            return 0
        fi
        printf 'Waiting for %s rows: %s/%s\n' "${table_name}" "${count}" "${minimum_rows}"
        sleep 10
    done

    echo "Timed out waiting for ${table_name} to reach ${minimum_rows} row(s)." >&2
    exit 1
}

get_gtfs_route_count() {
    sql_scalar "SELECT COUNT(*) FROM transit.routes;" 2>/dev/null || printf '0'
}

write_step "Checking prerequisites"
require_command docker
require_command python
write_ok "Docker and Python are available"

if [[ "${CHECK_ONLY}" == "true" ]]; then
    cat <<'EOF'

Demo script is ready. It will run these phases:
  1. Download Flink connector JARs
  2. Optionally reset Docker volumes with --reset --force
  3. Build worker and Flink images
  4. Start Kafka, TimescaleDB, Flink, and Grafana
  5. Apply migrations and load GTFS if needed
  6. Start live producer, GTFS matcher, and predictor
  7. Wait for rows and print Grafana URLs
EOF
    exit 0
fi

if [[ "${RESET}" == "true" ]]; then
    if [[ "${FORCE}" != "true" ]]; then
        read -r -p "This will remove Docker Compose containers and volumes. Type RESET to continue: " answer
        if [[ "${answer}" != "RESET" ]]; then
            echo "Reset cancelled." >&2
            exit 1
        fi
    fi
    run_step "Resetting Docker Compose stack and volumes" docker compose down -v --remove-orphans
fi

run_step "Downloading Flink connector JARs" bash "${SCRIPT_DIR}/download_flink_jars.sh"

if [[ "${SKIP_BUILD}" != "true" ]]; then
    run_step "Building Python worker and Flink images" docker compose build gtfs-matcher flink-jobmanager
fi

run_step "Starting core infrastructure" docker compose up -d timescaledb kafka grafana flink-jobmanager flink-taskmanager

wait_container_healthy "transit-timescaledb"
wait_container_healthy "transit-kafka"
wait_container_healthy "transit-flink-jobmanager"
wait_container_healthy "transit-grafana"

run_step "Applying database migrations" docker compose run --rm --no-deps gtfs-matcher python scripts/migrate.py

route_count="$(get_gtfs_route_count)"
route_count="${route_count//[^0-9]/}"
route_count="${route_count:-0}"
if [[ "${RELOAD_GTFS}" == "true" || "${route_count}" == "0" ]]; then
    run_step "Loading GTFS static feed" docker compose run --rm --no-deps gtfs-matcher python -m services.gtfs_ingestion.loader
else
    write_ok "GTFS already loaded with ${route_count} route(s). Use --reload-gtfs to load again."
fi

wait_table_rows "transit.routes" "" 1
wait_table_rows "transit.stop_times" "" 1

run_step "Starting live producer" docker compose up -d live-producer
wait_container_healthy "transit-live-producer"

if [[ "${SKIP_FLINK_SUBMIT}" != "true" ]]; then
    run_step "Submitting Flink processing job" bash "${SCRIPT_DIR}/submit_flink_job.sh"
else
    write_ok "Skipped Flink job submission."
fi

run_step "Starting GTFS matcher and delay predictor" docker compose up -d gtfs-matcher delay-predictor
wait_container_healthy "transit-gtfs-matcher"
wait_container_healthy "transit-delay-predictor"

write_step "Waiting for live demo rows"
wait_table_rows "transit.vehicle_positions" "event_time" "${MIN_ROWS}"
wait_table_rows "transit.stop_delay_events" "observed_time" "${MIN_ROWS}"
wait_table_rows "transit.realtime_feature_windows" "feature_time" "${MIN_ROWS}"
wait_table_rows "transit.route_delay_predictions" "prediction_time" "${MIN_ROWS}"

write_step "Current demo table counts"
docker exec transit-timescaledb psql -U transit -d transit -c "SELECT 'vehicle_positions' AS table_name, COUNT(*) FROM transit.vehicle_positions UNION ALL SELECT 'stop_delay_events', COUNT(*) FROM transit.stop_delay_events UNION ALL SELECT 'realtime_feature_windows', COUNT(*) FROM transit.realtime_feature_windows UNION ALL SELECT 'route_delay_predictions', COUNT(*) FROM transit.route_delay_predictions;"

printf '\nDemo is ready.\n'
printf 'Grafana:               %s\n' "${GRAFANA_URL}"
printf 'GTFS Predictions:      %s\n' "${GTFS_DASHBOARD_URL}"
printf 'Flink Web UI:          %s\n' "${FLINK_URL}"
printf 'TimescaleDB local DSN: postgresql://transit:transit_secret@localhost:5433/transit\n'