<#
.SYNOPSIS
    Prepare a full local demo of the transit delay detection stack.
.DESCRIPTION
    Builds required images, starts Docker services, applies migrations, loads GTFS data,
    starts the live producer, submits the Flink job, waits for seeded rows, and prints
    the presentation URLs. Use -Reset -Force for a clean demo database and Docker volumes.
.EXAMPLE
    .\scripts\demo_reset.ps1
.EXAMPLE
    .\scripts\demo_reset.ps1 -Reset -Force
.EXAMPLE
    .\scripts\demo_reset.ps1 -CheckOnly
#>
[CmdletBinding()]
param(
    [switch]$Reset,
    [switch]$Force,
    [switch]$ReloadGtfs,
    [switch]$SkipFlinkSubmit,
    [switch]$SkipBuild,
    [switch]$CheckOnly,
    [int]$TimeoutSeconds = 900,
    [int]$MinRows = 1
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$GrafanaUrl = "http://localhost:3000"
$GtfsDashboardUrl = "$GrafanaUrl/d/gtfs-delay-predictions/gtfs-delay-and-predictions"
$FlinkUrl = "http://localhost:8081"

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[ok] $Message" -ForegroundColor Green
}

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Command
    )
    Write-Step $Message
    & $Command
}

function Test-CommandAvailable {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found on PATH: $Name"
    }
}

function Invoke-SqlScalar {
    param([string]$Sql)
    $value = docker exec transit-timescaledb psql -U transit -d transit -t -A -c $Sql
    if ($LASTEXITCODE -ne 0) {
        throw "SQL command failed: $Sql"
    }
    return ($value | Select-Object -First 1).Trim()
}

function Wait-ContainerHealthy {
    param(
        [string]$ContainerName,
        [int]$Timeout = $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($Timeout)
    while ((Get-Date) -lt $deadline) {
        $state = docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $ContainerName 2>$null
        if ($LASTEXITCODE -eq 0 -and ($state -eq "healthy" -or $state -eq "running")) {
            Write-Ok "$ContainerName is $state"
            return
        }
        Start-Sleep -Seconds 5
    }
    throw "Timed out waiting for $ContainerName to become healthy or running."
}

function Wait-TableRows {
    param(
        [string]$TableName,
        [string]$TimeColumn = "",
        [int]$MinimumRows = $MinRows,
        [int]$Timeout = $TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($Timeout)
    while ((Get-Date) -lt $deadline) {
        $whereClause = ""
        if ($TimeColumn) {
            $whereClause = " WHERE $TimeColumn >= NOW() - INTERVAL '2 hours'"
        }
        $count = [int](Invoke-SqlScalar "SELECT COUNT(*) FROM $TableName$whereClause;")
        if ($count -ge $MinimumRows) {
            Write-Ok "$TableName has $count row(s)"
            return $count
        }
        Write-Host "Waiting for $TableName rows: $count/$MinimumRows"
        Start-Sleep -Seconds 10
    }
    throw "Timed out waiting for $TableName to reach $MinimumRows row(s)."
}

function Get-GtfsRouteCount {
    try {
        return [int](Invoke-SqlScalar "SELECT COUNT(*) FROM transit.routes;")
    } catch {
        return 0
    }
}

Write-Step "Checking prerequisites"
Test-CommandAvailable "docker"
Test-CommandAvailable "python"
Write-Ok "Docker and Python are available"

if ($CheckOnly) {
    Write-Host "`nDemo script is ready. It will run these phases:"
    Write-Host "  1. Download Flink connector JARs"
    Write-Host "  2. Optionally reset Docker volumes with -Reset -Force"
    Write-Host "  3. Build worker and Flink images"
    Write-Host "  4. Start Kafka, TimescaleDB, Flink, and Grafana"
    Write-Host "  5. Apply migrations and load GTFS if needed"
    Write-Host "  6. Start live producer, GTFS matcher, and predictor"
    Write-Host "  7. Wait for rows and print Grafana URLs"
    exit 0
}

if ($Reset) {
    if (-not $Force) {
        $answer = Read-Host "This will remove Docker Compose containers and volumes. Type RESET to continue"
        if ($answer -ne "RESET") {
            throw "Reset cancelled."
        }
    }
    Invoke-Step "Resetting Docker Compose stack and volumes" {
        docker compose down -v --remove-orphans
    }
}

Invoke-Step "Downloading Flink connector JARs" {
    & "$ScriptDir\download_flink_jars.ps1"
}

if (-not $SkipBuild) {
    Invoke-Step "Building Python worker and Flink images" {
        docker compose build gtfs-matcher flink-jobmanager
    }
}

Invoke-Step "Starting core infrastructure" {
    docker compose up -d timescaledb kafka grafana flink-jobmanager flink-taskmanager
}

Wait-ContainerHealthy "transit-timescaledb"
Wait-ContainerHealthy "transit-kafka"
Wait-ContainerHealthy "transit-flink-jobmanager"
Wait-ContainerHealthy "transit-grafana"

Invoke-Step "Applying database migrations" {
    docker compose run --rm --no-deps gtfs-matcher python scripts/migrate.py
}

$routeCount = Get-GtfsRouteCount
if ($ReloadGtfs -or $routeCount -eq 0) {
    Invoke-Step "Loading GTFS static feed" {
        docker compose run --rm --no-deps gtfs-matcher python -m services.gtfs_ingestion.loader
    }
} else {
    Write-Ok "GTFS already loaded with $routeCount route(s). Use -ReloadGtfs to load again."
}

Wait-TableRows -TableName "transit.routes" -MinimumRows 1 | Out-Null
Wait-TableRows -TableName "transit.stop_times" -MinimumRows 1 | Out-Null

Invoke-Step "Starting live producer" {
    docker compose up -d live-producer
}
Wait-ContainerHealthy "transit-live-producer"

if (-not $SkipFlinkSubmit) {
    Invoke-Step "Submitting Flink processing job" {
        & "$ScriptDir\submit_flink_job.ps1"
    }
} else {
    Write-Ok "Skipped Flink job submission."
}

Invoke-Step "Starting GTFS matcher and delay predictor" {
    docker compose up -d gtfs-matcher delay-predictor
}
Wait-ContainerHealthy "transit-gtfs-matcher"
Wait-ContainerHealthy "transit-delay-predictor"

Write-Step "Waiting for live demo rows"
Wait-TableRows -TableName "transit.vehicle_positions" -TimeColumn "event_time" -MinimumRows $MinRows | Out-Null
Wait-TableRows -TableName "transit.stop_delay_events" -TimeColumn "observed_time" -MinimumRows $MinRows | Out-Null
Wait-TableRows -TableName "transit.realtime_feature_windows" -TimeColumn "feature_time" -MinimumRows $MinRows | Out-Null
Wait-TableRows -TableName "transit.route_delay_predictions" -TimeColumn "prediction_time" -MinimumRows $MinRows | Out-Null

Write-Step "Current demo table counts"
docker exec transit-timescaledb psql -U transit -d transit -c "SELECT 'vehicle_positions' AS table_name, COUNT(*) FROM transit.vehicle_positions UNION ALL SELECT 'stop_delay_events', COUNT(*) FROM transit.stop_delay_events UNION ALL SELECT 'realtime_feature_windows', COUNT(*) FROM transit.realtime_feature_windows UNION ALL SELECT 'route_delay_predictions', COUNT(*) FROM transit.route_delay_predictions;"

Write-Host "`nDemo is ready." -ForegroundColor Green
Write-Host "Grafana:               $GrafanaUrl"
Write-Host "GTFS Predictions:      $GtfsDashboardUrl"
Write-Host "Flink Web UI:          $FlinkUrl"
Write-Host "TimescaleDB local DSN: postgresql://transit:transit_secret@localhost:5433/transit"
