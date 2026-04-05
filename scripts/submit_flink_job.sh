#!/usr/bin/env bash
# submit_flink_job.sh — Submit the PyFlink processing pipeline to the Flink cluster.
#
# Prerequisites:
#   - Docker Compose stack running (docker compose up -d)
#   - Flink JAR connectors downloaded (./scripts/download_flink_jars.sh)
#   - Kafka topic populated by the live producer
#   - TimescaleDB migrations applied
#
# Usage:
#   ./scripts/submit_flink_job.sh

set -euo pipefail

FLINK_CONTAINER="transit-flink-jobmanager"
JOB_PATH="/opt/flink/jobs/processing_job.py"

echo "==> Installing PyFlink in Flink JobManager container..."
docker exec "${FLINK_CONTAINER}" pip install apache-flink==1.20.0 --quiet 2>/dev/null || \
    docker exec "${FLINK_CONTAINER}" pip install apache-flink==1.20.0

echo "==> Copying connector JARs to Flink lib directory..."
docker exec "${FLINK_CONTAINER}" bash -c '
    if [ -d /opt/flink/lib/custom ]; then
        cp -n /opt/flink/lib/custom/*.jar /opt/flink/lib/ 2>/dev/null || true
        echo "   Connector JARs copied."
    fi
'

echo "==> Submitting PyFlink processing job..."
docker exec "${FLINK_CONTAINER}" flink run \
    --python "${JOB_PATH}" \
    --jarfile /opt/flink/lib/custom/flink-connector-kafka-3.3.0-1.20.jar \
    -d

echo "==> Job submitted. Check Flink dashboard at http://localhost:8081"
