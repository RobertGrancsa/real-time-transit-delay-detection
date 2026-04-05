#!/usr/bin/env bash
# =============================================================================
# Download Flink connector JARs required for the transit processing pipeline.
# Place them in flink/lib/ so Docker mounts them into the Flink containers.
#
# Usage:  bash scripts/download_flink_jars.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SCRIPT_DIR}/../flink/lib"
mkdir -p "${TARGET_DIR}"

MAVEN_CENTRAL="https://repo1.maven.org/maven2"

declare -A JARS=(
    # Kafka SQL connector for Flink 1.20 (fat JAR — bundles kafka-clients)
    ["flink-sql-connector-kafka-3.3.0-1.20.jar"]="${MAVEN_CENTRAL}/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"
    # JDBC connector for Flink
    ["flink-connector-jdbc-3.2.0-1.19.jar"]="${MAVEN_CENTRAL}/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar"
    # PostgreSQL JDBC driver
    ["postgresql-42.7.3.jar"]="${MAVEN_CENTRAL}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
)

echo "Downloading Flink connector JARs to ${TARGET_DIR} ..."
for jar_name in "${!JARS[@]}"; do
    dest="${TARGET_DIR}/${jar_name}"
    if [ -f "${dest}" ]; then
        echo "  [skip] ${jar_name} already exists"
    else
        echo "  [download] ${jar_name}"
        curl -fSL -o "${dest}" "${JARS[${jar_name}]}"
    fi
done

echo "Done. JARs in ${TARGET_DIR}:"
ls -lh "${TARGET_DIR}"/*.jar 2>/dev/null || echo "  (none found)"
