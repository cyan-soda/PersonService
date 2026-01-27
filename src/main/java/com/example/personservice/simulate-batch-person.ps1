# ================== CONFIGURATION ==================
$BootstrapServers = "localhost:9092"
$Topic = "person.kafka.batch"
$KafkaBinPath = "C:\kafka\bin\windows"

# ================== CHECK PRODUCER =================
$ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
if (-not (Test-Path $ProducerBat)) {
    Write-Error "Cannot find kafka-console-producer.bat at '$ProducerBat'. Please update the `$KafkaBinPath variable."
    exit
}

# ================== PRODUCE FUNCTION =================
function Send-To-Kafka {
    param (
        [array]$Messages
    )

    Write-Host "Sending $($Messages.Count) events to topic '$Topic'..." -ForegroundColor Cyan

    $InputData = $Messages | ForEach-Object {
        # Format as "key:json_value"
        "$($_.key):$($_.value)"
    }

    $InputData | & $ProducerBat `
        --bootstrap-server $BootstrapServers `
        --topic $Topic `
        --property "parse.key=true" `
        --property "key.separator=:"
}

# ================== MENU ==================
Clear-Host
Write-Host "=== KAFKA NON-BLOCKING BATCH TESTER ===" -ForegroundColor Yellow
Write-Host "Tests the Redis-based buffering and retry mechanism. All batches have at least 5 events."
Write-Host ""
Write-Host "Test Scenarios:"
Write-Host " 1. Single Batch: Mixed Success and Failure"
Write-Host " 2. Single Batch: Dependency Chain Failure"
Write-Host " 3. Multi-Batch: Failure followed by more events for the same key"
Write-Host ""
$Choice = Read-Host "Select a scenario to run"

# ================== SCENARIO 1: Mixed Success and Failure ==================
if ($Choice -eq "1") {
    $Batch = @(
        @{ key="TAX101"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"A","dateOfBirth":"1990-01-01","taxNumber":"TAX101"}}' },
        @{ key="TAX102"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Fail","dateOfBirth":"1990-01-01","taxNumber":"TAX102"}}' },
        @{ key="TAX103"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX103"}}' },
        @{ key="TAX102"; value='{"eventType":"UPDATE","person":{"firstName":"retry","lastName":"FailUpdate","dateOfBirth":"1990-01-01","taxNumber":"TAX102"}}' },
        @{ key="TAX104"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"C","dateOfBirth":"1990-01-01","taxNumber":"TAX104"}}' }
    )
    Send-To-Kafka -Messages $Batch
    Write-Host "Expected Behavior:" -ForegroundColor Green
    Write-Host " - TAX101, TAX103, TAX104: Processed successfully."
    Write-Host " - TAX102 (CREATE): Fails, sent to retry-1. Redis state for TAX102 is set to RETRYING."
    Write-Host " - TAX102 (UPDATE): Is buffered in Redis because the key is marked as RETRYING."
}

# ================== SCENARIO 2: Dependency Chain Failure ==================
elseif ($Choice -eq "2") {
    $Batch = @(
        @{ key="TAX401"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Blocker","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX401"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"Waiter","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX402"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Independent1","dateOfBirth":"1990-01-01","taxNumber":"TAX402"}}' },
        @{ key="TAX401"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX403"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Independent2","dateOfBirth":"1990-01-01","taxNumber":"TAX403"}}' }
    )
    Send-To-Kafka -Messages $Batch
    Write-Host "Expected Behavior:" -ForegroundColor Green
    Write-Host " - TAX401 (CREATE): Fails, sent to retry-1. Redis state for TAX401 is set to RETRYING."
    Write-Host " - TAX401 (UPDATE & DELETE): Are buffered in Redis in order."
    Write-Host " - TAX402 & TAX403 (CREATEs): Processed successfully."
}

# ================== SCENARIO 3: Multi-Batch Buffering ==================
elseif ($Choice -eq "3") {
    # Batch 1:
    $Batch1 = @(
        @{ key="TAX301"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"InitialFail","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX302"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B1_OK_A","dateOfBirth":"1990-01-01","taxNumber":"TAX302"}}' },
        @{ key="TAX303"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B1_OK_B","dateOfBirth":"1990-01-01","taxNumber":"TAX303"}}' },
        @{ key="TAX304"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B1_OK_C","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },
        @{ key="TAX305"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B1_OK_D","dateOfBirth":"1990-01-01","taxNumber":"TAX305"}}' }
    )

    # Batch 2:
    $Batch2 = @(
        @{ key="TAX306"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B2_OK_A","dateOfBirth":"1990-01-01","taxNumber":"TAX306"}}' },
        @{ key="TAX301"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"BufferedFromB2","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX307"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B2_OK_B","dateOfBirth":"1990-01-01","taxNumber":"TAX307"}}' },
        @{ key="TAX308"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B2_OK_C","dateOfBirth":"1990-01-01","taxNumber":"TAX308"}}' },
        @{ key="TAX301"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"AlsoBufferedFromB2","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' }
    )

    Write-Host "Sending Batch 1 (5 events, one fails)..." -ForegroundColor Cyan
    Send-To-Kafka -Messages $Batch1
    Write-Host "Waiting for consumer to process Batch 1..."
    Start-Sleep -Seconds 3

    Write-Host "Sending Batch 2 (5 events, some for the failed key)..." -ForegroundColor Cyan
    Send-To-Kafka -Messages $Batch2

    Write-Host "Expected Behavior:" -ForegroundColor Green
    Write-Host " - Batch 1:"
    Write-Host "   - TAX301 (CREATE): Fails, sent to retry-1. Redis state for TAX301 is set."
    Write-Host "   - TAX302, 303, 304, 305: Processed successfully."
    Write-Host " - Batch 2:"
    Write-Host "   - TAX306, 307, 308: Processed successfully."
    Write-Host "   - TAX301 (UPDATE & DELETE): Are both buffered in Redis because the consumer finds the RETRYING state."
}

else {
    Write-Host "Invalid selection. Please run the script again and choose 1, 2, or 3." -ForegroundColor Red
}
