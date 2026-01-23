# ================== CONFIGURATION ==================
$BootstrapServers = "localhost:9092"
$Topic = "tax.kafka.batch"
$KafkaBinPath = "C:\Users\nhnha\Downloads\kafka\bin\windows"

# ================== CHECK PRODUCER =================
$ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
if (-not (Test-Path $ProducerBat)) {
    Write-Error "Cannot find kafka-console-producer.bat at: $ProducerBat"
    Write-Host "Please update the KafkaBinPath variable to point to your Kafka installation"
    exit
}

# ================== PRODUCE FUNCTION =================
function Send-To-Kafka {
    param (
        [array]$Messages
    )

    Write-Host "Sending $($Messages.Count) events to '$Topic'..." -ForegroundColor Cyan

    $InputData = $Messages | ForEach-Object {
        "$($_.key):$($_.value)"
    }

    $InputData | & $ProducerBat `
        --bootstrap-server $BootstrapServers `
        --topic $Topic `
        --property "parse.key=true" `
        --property "key.separator=:"

    Write-Host "Messages sent successfully!" -ForegroundColor Green
}

# ================== MENU ==================
Clear-Host
Write-Host "=== TAX BATCH BLOCKING RETRY TESTER ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "Available Test Scenarios:" -ForegroundColor White
Write-Host ""
Write-Host "1. Happy Path (All Success)" -ForegroundColor Green
Write-Host "   - 3 valid tax calculations with positive amounts"
Write-Host "   - Expected: All processed successfully in main consumer"
Write-Host ""
Write-Host "2. Blocking Retry - Invalid Amount" -ForegroundColor Red
Write-Host "   - Valid -> Invalid (amount=0) -> Valid"
Write-Host "   - Expected: Whole batch fails and moves to Retry-1 -> Retry-2 -> DLT"
Write-Host ""
Write-Host "3. Blocking Retry - First Record Invalid" -ForegroundColor Red
Write-Host "   - Invalid (amount=0) -> Valid -> Valid"
Write-Host "   - Expected: Batch fails immediately, all records move to Retry-1"
Write-Host ""
Write-Host "4. Large Batch with Mixed Results" -ForegroundColor Yellow
Write-Host "   - 5 events, 1 invalid amount at position 3"
Write-Host "   - Expected: First 2 processed, then failure, all 5 move to retry"
Write-Host ""
Write-Host "5. Simulated Service Error" -ForegroundColor Magenta
Write-Host "   - Uses special tax numbers (TAX888, TAX889) to trigger errors"
Write-Host "   - Expected: Service-level failures trigger retry mechanism"
Write-Host ""

$Choice = Read-Host "Select Scenario (1-5)"

# ================== SCENARIO 1 - HAPPY PATH ==================
if ($Choice -eq "1") {
    Write-Host "`nExecuting Happy Path Scenario..." -ForegroundColor Green

    $Batch = @(
        @{ key="TAX101"; value='{"eventType":"ADD","taxId":"TAX101","amount":100.50}' },
        @{ key="TAX999"; value='{"eventType":"ADD","taxId":"TAX999","amount":200.75}' },
        @{ key="TAX105"; value='{"eventType":"ADD","taxId":"TAX105","amount":150.25}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "✓ Batch Consumer receives 3 records"
    Write-Host "✓ All 3 records processed successfully"
    Write-Host "✓ Batch acknowledged and offset committed"
    Write-Host "✓ No records sent to retry topics"
}

# ================== SCENARIO 2 - MIDDLE FAILURE ==================
elseif ($Choice -eq "2") {
    Write-Host "`nExecuting Middle Failure Scenario..." -ForegroundColor Red

    $Batch = @(
        @{ key="TAX200"; value='{"eventType":"ADD","taxId":"TAX200","amount":100.0}' },
        @{ key="TAX888"; value='{"eventType":"ADD","taxId":"TAX888","amount":50.0}' },  # Invalid amount
        @{ key="TAX202"; value='{"eventType":"ADD","taxId":"TAX202","amount":150.0}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Red
    Write-Host "1. Main Consumer processes TAX200 successfully"
    Write-Host "2. Main Consumer fails at TAX201 (amount=0)"
    Write-Host "3. ENTIRE batch (all 3 records) sent to 'tax.kafka.batch.retry-1'"
    Write-Host "4. Retry-1 Consumer receives batch, fails again"
    Write-Host "5. All 3 records sent to 'tax.kafka.batch.retry-2'"
    Write-Host "6. Retry-2 Consumer receives batch, fails again"
    Write-Host "7. All 3 records sent to 'tax.kafka.batch.dlt'"
}

# ================== SCENARIO 3 - FIRST FAILURE ==================
elseif ($Choice -eq "3") {
    Write-Host "`nExecuting First Record Failure Scenario..." -ForegroundColor Red

    $Batch = @(
        @{ key="TAX888"; value='{"eventType":"ADD","taxId":"TAX888","amount":50.0}' },  # Invalid amount
        @{ key="TAX103"; value='{"eventType":"ADD","taxId":"TAX103","amount":200.0}' },
        @{ key="TAX105"; value='{"eventType":"ADD","taxId":"TAX105","amount":150.0}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Red
    Write-Host "1. Main Consumer fails immediately at TAX300 (amount=0)"
    Write-Host "2. TAX301 and TAX302 are NOT processed in main consumer"
    Write-Host "3. Entire batch (all 3 records) moved to Retry-1"
    Write-Host "4. This demonstrates blocking behavior - one failure blocks all"
}

# ================== SCENARIO 4 - LARGE BATCH ==================
elseif ($Choice -eq "4") {
    Write-Host "`nExecuting Large Batch Scenario..." -ForegroundColor Yellow

    $Batch = @(
        @{ key="TAX400"; value='{"eventType":"ADD","taxId":"TAX400","amount":10.0}' },
        @{ key="TAX401"; value='{"eventType":"ADD","taxId":"TAX401","amount":20.0}' },
        @{ key="TAX402"; value='{"eventType":"ADD","taxId":"TAX402","amount":30.0}' },
        @{ key="TAX403"; value='{"eventType":"ADD","taxId":"TAX403","amount":0.0}' },    # Invalid amount
        @{ key="TAX404"; value='{"eventType":"ADD","taxId":"TAX404","amount":50.0}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Yellow
    Write-Host "1. Main Consumer processes TAX400, TAX401, TAX402 successfully"
    Write-Host "2. Main Consumer fails at TAX403 (amount=0)"
    Write-Host "3. ALL 5 records sent to Retry-1 (including already processed ones)"
    Write-Host "4. Note: This demonstrates the trade-off of blocking batch processing"
    Write-Host "5. Successfully processed records may be reprocessed (ensure idempotency)"
}

# ================== SCENARIO 5 - SERVICE ERRORS ==================
elseif ($Choice -eq "5") {
    Write-Host "`nExecuting Service Error Scenario..." -ForegroundColor Magenta

    $Batch = @(
        @{ key="TAX500"; value='{"eventType":"ADD","taxId":"TAX500","amount":100.0}' },
        @{ key="TAX888"; value='{"eventType":"ADD","taxId":"TAX888","amount":200.0}' },  # Triggers simulated error
        @{ key="TAX501"; value='{"eventType":"ADD","taxId":"TAX501","amount":150.0}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Magenta
    Write-Host "1. Main Consumer processes TAX500 successfully"
    Write-Host "2. Main Consumer hits simulated error at TAX888"
    Write-Host "3. Entire batch sent to Retry-1"
    Write-Host "4. Retry consumers will continue to fail on TAX888"
    Write-Host "5. Eventually all records end up in DLT"
    Write-Host ""
    Write-Host "Special Tax Numbers for Testing:" -ForegroundColor Cyan
    Write-Host "- TAX888: Triggers simulated transient error"
    Write-Host "- TAX889: Triggers simulated fatal error"
}

else {
    Write-Host "Invalid selection. Please choose 1-5." -ForegroundColor Red
    exit
}

Write-Host ""
Write-Host "Monitor your application logs to see the batch processing behavior!" -ForegroundColor Cyan
Write-Host ""
Write-Host "Useful Kafka Commands for Monitoring:" -ForegroundColor Yellow
Write-Host "- List topics: kafka-topics.bat --bootstrap-server localhost:9092 --list"
Write-Host "- Check main topic: kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic tax.kafka.batch --from-beginning"
Write-Host "- Check retry-1: kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic tax.kafka.batch.retry-1 --from-beginning"
Write-Host "- Check retry-2: kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic tax.kafka.batch.retry-2 --from-beginning"
Write-Host "- Check DLT: kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic tax.kafka.batch.dlt --from-beginning"
