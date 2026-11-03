param(
    [string]$GatewayUrl = "http://localhost:8080",
    [int]$TimeoutSeconds = 45
)

$ErrorActionPreference = "Stop"

function New-Order {
    param(
        [string]$IdempotencyKey,
        [string]$Sku,
        [int]$Quantity,
        [int]$UnitPriceCents
    )

    $body = @{
        customer_id = "demo-" + [guid]::NewGuid().ToString().Substring(0, 8)
        currency    = "EUR"
        items       = @(@{
            sku              = $Sku
            quantity         = $Quantity
            unit_price_cents = $UnitPriceCents
        })
    } | ConvertTo-Json -Depth 5

    Invoke-RestMethod -Method Post `
        -Uri "$GatewayUrl/v1/orders" `
        -Headers @{ "Idempotency-Key" = $IdempotencyKey } `
        -ContentType "application/json" `
        -Body $body
}

function Wait-Order {
    param([string]$OrderId)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $order = Invoke-RestMethod -Method Get -Uri "$GatewayUrl/v1/orders/$OrderId"
        if ($order.status -eq "CONFIRMED" -or $order.status -eq "CANCELLED") {
            return $order
        }
        Start-Sleep -Milliseconds 750
    }
    throw "order $OrderId did not reach a terminal state within $TimeoutSeconds seconds"
}

Write-Host "Creating order with SKU-1..."
$key = [guid]::NewGuid().ToString()
$first = New-Order -IdempotencyKey $key -Sku "SKU-1" -Quantity 2 -UnitPriceCents 1999
Write-Host ("  order_id={0} status={1}" -f $first.order_id, $first.status)

Write-Host "Replaying the same Idempotency-Key..."
$replay = New-Order -IdempotencyKey $key -Sku "SKU-1" -Quantity 2 -UnitPriceCents 1999
if ($replay.order_id -ne $first.order_id) {
    throw "idempotency failed: got a different order id"
}
Write-Host "  same order_id returned (idempotency OK)"

Write-Host "Creating two more orders..."
$second = New-Order -IdempotencyKey ([guid]::NewGuid().ToString()) -Sku "SKU-2" -Quantity 1 -UnitPriceCents 4999
$third = New-Order -IdempotencyKey ([guid]::NewGuid().ToString()) -Sku "SKU-3" -Quantity 1 -UnitPriceCents 7999
Write-Host ("  order_id={0}" -f $second.order_id)
Write-Host ("  order_id={0}" -f $third.order_id)

Write-Host "Waiting for asynchronous processing..."
$finalFirst = Wait-Order -OrderId $first.order_id
$finalSecond = Wait-Order -OrderId $second.order_id
$finalThird = Wait-Order -OrderId $third.order_id

Write-Host ""
Write-Host "Final states:"
Write-Host ("  {0}  {1}" -f $finalFirst.order_id, $finalFirst.status)
Write-Host ("  {0}  {1}" -f $finalSecond.order_id, $finalSecond.status)
Write-Host ("  {0}  {1}" -f $finalThird.order_id, $finalThird.status)

Write-Host ""
Write-Host "Cancelling the first order..."
try {
    $cancelled = Invoke-RestMethod -Method Post `
        -Uri "$GatewayUrl/v1/orders/$($first.order_id)/cancel" `
        -ContentType "application/json" `
        -Body (@{ reason = "demo cancellation" } | ConvertTo-Json)
    Write-Host ("  status={0} reason={1}" -f $cancelled.status, "demo cancellation")
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    if ($status -eq 409) {
        Write-Host "  order already confirmed; cancellation correctly rejected with 409"
    } else {
        throw
    }
}

Write-Host ""
Write-Host "Demo complete. Follow events in the RabbitMQ UI or Grafana dashboard."
