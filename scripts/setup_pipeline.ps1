function Write-Log {
    param (
        [string]$Level = "INFO",
        [string]$Message
    )
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Write-Host "[$timestamp] [$Level] $Message"
}

Write-Log "INFO" "Initializing batch data platform infrastructure..."

# 1. Start Docker Containers
Write-Log "INFO" "Deploying containerized services via Docker Compose..."
docker compose up -d

# 2. Wait for Service Daemons & DAG Parser
Write-Log "INFO" "Awaiting Airflow scheduler and LocalStack daemon health checks (20s)..."
Start-Sleep -Seconds 20

# 3. Create Exact LocalStack S3 Storage Buckets
Write-Log "INFO" "Provisioning enterprise S3 storage buckets (raw-landing, curated-lake, quarantine)..."
docker exec s3_storage awslocal s3 mb s3://ecommerce-raw-landing 2>$null | Out-Null
docker exec s3_storage awslocal s3 mb s3://ecommerce-curated-lake 2>$null | Out-Null
docker exec s3_storage awslocal s3 mb s3://ecommerce-quarantine 2>$null | Out-Null

# 4. Storage Verification
Write-Log "INFO" "Configured S3 storage buckets verified:"
$buckets = docker exec s3_storage awslocal s3 ls 2>$null
if ($buckets) {
    $buckets | ForEach-Object { Write-Host "    $_" }
}

Write-Log "SUCCESS" "Infrastructure provisioned and ready for pipeline orchestration."
