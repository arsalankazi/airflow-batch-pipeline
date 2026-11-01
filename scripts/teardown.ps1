function Write-Log {
    param (
        [string]$Level = "INFO",
        [string]$Message
    )
    $timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Write-Host "[$timestamp] [$Level] $Message"
}

Write-Log "INFO" "Initiating graceful teardown of batch platform services..."
docker compose down
Write-Log "Teardown completed." "All containers stopped, networks detached, and host resources released."
