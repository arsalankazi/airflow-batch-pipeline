#!/usr/bin/env bash
set -euo pipefail

log() {
    local level="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $message"
}

log "INFO" "Initiating graceful teardown of batch platform services..."
docker compose down
log "SUCCESS" "Teardown completed. All containers stopped, networks detached, and host resources released."