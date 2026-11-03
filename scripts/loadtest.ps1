param(
    [string]$GatewayUrl = "http://localhost:8080",
    [int]$Orders = 500,
    [int]$Concurrency = 50,
    [string]$Sku = "SKU-1",
    [int]$Quantity = 1
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "go is not installed or not on PATH"
}

$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    go run ./cmd/loadgen `
        -url $GatewayUrl `
        -orders $Orders `
        -concurrency $Concurrency `
        -sku $Sku `
        -quantity $Quantity
} finally {
    Pop-Location
}
