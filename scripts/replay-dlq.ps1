param(
    [Parameter(Mandatory = $true)]
    [string]$Queue,

    [Parameter(Mandatory = $true)]
    [string]$RoutingKey,

    [int]$Limit = 100,
    [string]$RabbitUrl = "amqp://orderflow:orderflow@localhost:5672/",
    [string]$Exchange = "orderflow.events"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "go is not installed or not on PATH"
}

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    go run ./cmd/replay-dlq `
        -url $RabbitUrl `
        -exchange $Exchange `
        -queue $Queue `
        -routing-key $RoutingKey `
        -limit $Limit
} finally {
    Pop-Location
}
