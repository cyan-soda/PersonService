# ================== CONFIGURATION ==================
$BootstrapServers = "localhost:9092"
$Topic = "tax.kafka.batch"
$KafkaBinPath = "C:\kafka\bin\windows"

# ================== CHECK PRODUCER ==================
$ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
if (-not (Test-Path $ProducerBat)) {
    Write-Error "Cannot find kafka-console-producer.bat at '$ProducerBat'. Please update the `$KafkaBinPath variable in the script."
    exit
}

# ================== PRODUCE FUNCTION ==================
function Send-To-Kafka {
    param (
        [array]$Messages
    )

    Write-Host "Sending $($Messages.Count) events to '$Topic'..." -ForegroundColor Cyan

    $InputData = $Messages | ForEach-Object {
        $json = $_.value | ConvertTo-Json -Depth 10 -Compress
        "$($_.key):$json"
    }

    $InputData | & $ProducerBat `
        --bootstrap-server $BootstrapServers `
        --topic $Topic `
        --property "parse.key=true" `
        --property "key.separator=:"
}

# ================== MENU ==================
Clear-Host
Write-Host "=== TAX BATCH NON-BLOCKING TESTER ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "Test Scenarios:"
Write-Host " 1. Single Mid-Batch Failure (Retryable Error)"
Write-Host " 2. Multiple Scattered Failures (Retryable & Fatal Errors)"
Write-Host " 3. Back-to-Back Batches (Idempotency & Flow Test)"
Write-Host ""
$Choice = Read-Host "Select Option"

# ================== SCENARIO 1: Single Mid-Batch Failure ==================
if ($Choice -eq "1") {
    Write-Host "`n=== SCENARIO 1: Single Mid-Batch Failure (Retryable) ===" -ForegroundColor Yellow
    Write-Host "Purpose: Ensure successful events are processed while the single failed event is sent to retry."
    Write-Host ""

    $Batch = @(
        @{ key="TAX201"; value=@{eventType="ADD";taxId="TAX201";amount=100.0} },
        @{ key="TAX202"; value=@{eventType="ADD";taxId="TAX202";amount=200.0} },
        @{ key="TAX888"; value=@{eventType="ADD";taxId="TAX888";amount=888.0} },  # RETRYABLE FAIL
        @{ key="TAX204"; value=@{eventType="ADD";taxId="TAX204";amount=400.0} },
        @{ key="TAX205"; value=@{eventType="ADD";taxId="TAX205";amount=500.0} }
    )
    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Results:" -ForegroundColor Green
    Write-Host " TAX201, TAX202, TAX204, TAX205 are processed successfully."
    Write-Host " TAX888 fails and is sent to 'tax.kafka.batch.retry-1'."
    Write-Host " The entire batch is acknowledged on the main topic."
}

# ================== SCENARIO 2: Multiple Failures ==================
elseif ($Choice -eq "2") {
    Write-Host "`n=== SCENARIO 2: Multiple Scattered Failures (Retryable & Fatal) ===" -ForegroundColor Yellow
    Write-Host "Purpose: Test resilience with a mix of failure types."
    Write-Host ""

    $Batch = @(
        @{ key="TAX301"; value=@{eventType="ADD";taxId="TAX301";amount=100.0} },
        @{ key="TAX302"; value=@{eventType="ADD";taxId="TAX302";amount=0.0} },      # FATAL FAIL (Invalid Amount) -> DLT
        @{ key="TAX303"; value=@{eventType="ADD";taxId="TAX303";amount=300.0} },
        @{ key="TAX888"; value=@{eventType="ADD";taxId="TAX888";amount=888.0} },
        @{ key="TAX305"; value=@{eventType="ADD";taxId="TAX305";amount=500.0} }
    )
    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Results:" -ForegroundColor Green
    Write-Host " TAX301, TAX303, TAX305 are processed successfully."
    Write-Host " TAX302 (fatal) is sent directly to 'tax.kafka.batch.dlt'."
    Write-Host " TAX888 (retryable) is sent to 'tax.kafka.batch.retry-1'."
}

# ================== SCENARIO 3: Back-to-Back Batches ==================
elseif ($Choice -eq "3") {
    Write-Host "`n=== SCENARIO 3: Back-to-Back Batches ===" -ForegroundColor Yellow
    Write-Host "Purpose: Test system handling of continuous data flow with failures."
    Write-Host ""

    # --- BATCH 1 ---
    Write-Host "Sending BATCH 1..." -ForegroundColor Cyan
    $Batch1 = @(
        @{ key="TAX401"; value=@{eventType="ADD";taxId="TAX401";amount=100.0} },
        @{ key="TAX888"; value=@{eventType="ADD";taxId="TAX888";amount=888.0} },
        @{ key="TAX403"; value=@{eventType="ADD";taxId="TAX403";amount=300.0} },
        @{ key="TAX404"; value=@{eventType="ADD";taxId="TAX404";amount=400.0} },
        @{ key="TAX405"; value=@{eventType="ADD";taxId="TAX405";amount=500.0} }
    )
    Send-To-Kafka -Messages $Batch1

    # --- BATCH 2 ---
    Write-Host "Sending BATCH 2..." -ForegroundColor Cyan
    $Batch2 = @(
        @{ key="TAX501"; value=@{eventType="ADD";taxId="TAX501";amount=100.0} },
        @{ key="TAX502"; value=@{eventType="ADD";taxId="TAX502";amount=200.0} },
        @{ key="TAX888"; value=@{eventType="ADD";taxId="TAX888";amount=10.0} },
        @{ key="TAX504"; value=@{eventType="ADD";taxId="TAX504";amount=400.0} },
        @{ key="TAX505"; value=@{eventType="ADD";taxId="TAX505";amount=500.0} }
    )
    Send-To-Kafka -Messages $Batch2

    Write-Host ""
    Write-Host "Expected Results:" -ForegroundColor Green
    Write-Host "BATCH 1: TAX401, 403, 404, 405 processed. TAX888 sent to retry-1."
    Write-Host "BATCH 2: TAX501, 502, 504, 505 processed. TAX901 sent to DLT."
    Write-Host "The retry consumer will eventually process the event for TAX888 from Batch 1."
}

else {
    Write-Host "Invalid selection." -ForegroundColor Red
}
