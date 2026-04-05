# submit_flink_job.ps1 — Submit the PyFlink processing pipeline to the Flink cluster.
#
# Prerequisites:
#   - Docker Compose stack running (docker compose up -d)
#   - Kafka topic populated by the live producer
#   - TimescaleDB migrations applied
#
# Usage:
#   .\scripts\submit_flink_job.ps1

$ErrorActionPreference = "Stop"

$FlinkContainer = "transit-flink-jobmanager"
$JobPath = "/opt/flink/jobs/processing_job.py"

Write-Host "==> Submitting PyFlink processing job..."
docker exec $FlinkContainer flink run `
    --python $JobPath `
    -d

Write-Host "==> Job submitted. Check Flink dashboard at http://localhost:8081"
