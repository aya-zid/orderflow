param(
    [switch]$Down,
    [int]$TimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $root "deploy/docker/docker-compose.yml"
$envFile = Join-Path $root ".env"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker is not installed or not on PATH"
}

if ($Down) {
    docker compose --env-file $envFile -f $composeFile down -v
    exit 0
}

if (-not (Test-Path -LiteralPath $envFile)) {
    Copy-Item -LiteralPath (Join-Path $root ".env.example") -Destination $envFile
    Write-Host "created .env from .env.example"
}

docker compose --env-file $envFile -f $composeFile up -d --build

$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$ready = $false

while ((Get-Date) -lt $deadline) {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8080/readyz" -TimeoutSec 3
        if ($response.status -eq "ready") {
            $ready = $true
            break
        }
    } catch {
        Start-Sleep -Seconds 2
    }
}

if (-not $ready) {
    docker compose --env-file $envFile -f $composeFile ps
    throw "gateway did not become ready within $TimeoutSeconds seconds"
}

Write-Host ""
Write-Host "OrderFlow is ready"
Write-Host "  Gateway      http://localhost:8080"
Write-Host "  Swagger      http://localhost:8080/docs"
Write-Host "  RabbitMQ     http://localhost:15672"
Write-Host "  Prometheus   http://localhost:9090"
Write-Host "  Grafana      http://localhost:3000"
Write-Host ""
Write-Host "Run 'pwsh -File scripts/demo.ps1' for the end-to-end demo."
