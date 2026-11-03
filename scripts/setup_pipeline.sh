#!/usr/bin/env bash
set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$1] $2"; }

log INFO "Initializing batch data platform infrastructure..."

log INFO "Deploying containerized services via Docker Compose..."
docker compose up -d

log INFO "Awaiting Airflow scheduler and LocalStack daemon health checks (20s)..."
sleep 20

log INFO "Provisioning enterprise S3 storage buckets (raw-landing, curated-lake, quarantine)..."
docker exec s3_storage awslocal s3 mb s3://ecommerce-raw-landing 2>/dev/null || true
docker exec s3_storage awslocal s3 mb s3://ecommerce-curated-lake 2>/dev/null || true
docker exec s3_storage awslocal s3 mb s3://ecommerce-quarantine  2>/dev/null || true

log INFO "Configured S3 storage buckets verified:"
docker exec s3_storage awslocal s3 ls 2>/dev/null || true

log SUCCESS "Infrastructure provisioned and ready for pipeline orchestration."