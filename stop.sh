#!/usr/bin/env bash
# =============================================================================
# stop.sh — Stop the Docker Compose stack.
#
# Usage:
#   ./stop.sh           # stop containers, keep volumes (data persists)
#   ./stop.sh --clean   # stop containers AND remove volumes (wipes data)
# =============================================================================
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [ "${1:-}" = "--clean" ]; then
    echo "==> Stopping stack and removing volumes (data will be wiped)..."
    docker compose down -v
else
    echo "==> Stopping stack (volumes preserved)..."
    docker compose down
fi

echo "==> Done."
