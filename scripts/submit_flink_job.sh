#!/usr/bin/env bash
# submit_flink_job.sh — Submit the PyFlink processing pipeline to the Flink cluster.
#
# Prerequisites:
#   - Docker Compose stack running (docker compose up -d)
#   - Kafka topic populated by the live producer
#   - TimescaleDB migrations applied
#
# Usage:
#   ./scripts/submit_flink_job.sh

set -euo pipefail

FLINK_CONTAINER="transit-flink-jobmanager"
JOB_PATH="/opt/flink/jobs/processing_job.py"

echo "==> Submitting PyFlink processing job..."
docker exec "${FLINK_CONTAINER}" flink run \
    --python "${JOB_PATH}" \
    -d

echo "==> Job submitted. Check Flink dashboard at http://localhost:8081"
