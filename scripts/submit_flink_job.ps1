# submit_flink_job.ps1 — Submit the PyFlink processing pipeline to the Flink cluster.
#
# Prerequisites:
#   - Docker Compose stack running (docker compose up -d)
#   - Flink JAR connectors downloaded (.\scripts\download_flink_jars.ps1)
#   - Kafka topic populated by the live producer
#   - TimescaleDB migrations applied
#
# Usage:
#   .\scripts\submit_flink_job.ps1

$ErrorActionPreference = "Stop"

$FlinkContainer = "transit-flink-jobmanager"
$JobPath = "/opt/flink/jobs/processing_job.py"

Write-Host "==> Installing PyFlink in Flink JobManager container..."
docker exec $FlinkContainer pip install apache-flink==1.20.0 --quiet

Write-Host "==> Copying connector JARs to Flink lib directory..."
docker exec $FlinkContainer bash -c @"
if [ -d /opt/flink/lib/custom ]; then
    cp -n /opt/flink/lib/custom/*.jar /opt/flink/lib/ 2>/dev/null || true
    echo '   Connector JARs copied.'
fi
"@

Write-Host "==> Submitting PyFlink processing job..."
docker exec $FlinkContainer flink run `
    --python $JobPath `
    --jarfile /opt/flink/lib/custom/flink-connector-kafka-3.3.0-1.20.jar `
    -d

Write-Host "==> Job submitted. Check Flink dashboard at http://localhost:8081"
