<# 
.SYNOPSIS
    Download Flink connector JARs required for the transit processing pipeline.
.DESCRIPTION
    Downloads Kafka connector, JDBC connector, and PostgreSQL driver JARs
    into flink/lib/ so Docker mounts them into the Flink containers.
.EXAMPLE
    .\scripts\download_flink_jars.ps1
#>
$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TargetDir = Join-Path (Split-Path -Parent $ScriptDir) "flink\lib"
New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null

$MavenCentral = "https://repo1.maven.org/maven2"

$Jars = @{
    "flink-sql-connector-kafka-3.3.0-1.20.jar" = "$MavenCentral/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"
    "flink-connector-jdbc-3.2.0-1.19.jar"      = "$MavenCentral/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
    "postgresql-42.7.3.jar"                     = "$MavenCentral/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
}

Write-Host "Downloading Flink connector JARs to $TargetDir ..."

foreach ($jar in $Jars.GetEnumerator()) {
    $dest = Join-Path $TargetDir $jar.Key
    if (Test-Path $dest) {
        Write-Host "  [skip] $($jar.Key) already exists"
    } else {
        Write-Host "  [download] $($jar.Key)"
        Invoke-WebRequest -Uri $jar.Value -OutFile $dest -UseBasicParsing
    }
}

Write-Host "`nDone. JARs in ${TargetDir}:"
Get-ChildItem -Path $TargetDir -Filter "*.jar" | Format-Table Name, Length -AutoSize
