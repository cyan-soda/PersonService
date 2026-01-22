# ================== CONFIGURATION ==================
$BootstrapServers = "localhost:9092"
$Topic = "person.kafka.batch"
$KafkaBinPath = "C:\Users\nhnha\Downloads\kafka\bin\windows"

# ================== CHECK PRODUCER =================
$ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
if (-not (Test-Path $ProducerBat)) {
    Write-Error "Cannot find kafka-console-producer.bat"
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
}

# ================== MENU ==================
Clear-Host
Write-Host "=== KAFKA BATCH & RETRY TESTER ===" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Test Non-Blocking (Mixed Success/Fail)"
Write-Host "   - 5 independent events"
Write-Host "   - 2 fail (retry), 3 succeed"
Write-Host ""
Write-Host "2. Test Blocking (Dependent Chain)"
Write-Host "   - Create (Fails) -> Update (Waits) -> Independent (Waits)"
Write-Host ""
Write-Host "3. Test Parallel Processing (10 Events)"
Write-Host ""
Write-Host "4. Test Non-Blocking Failure (UPDATE fails -> DELETE runs)"
Write-Host "5. Test Mixed Criticality (CREATE Block -> UPDATE -> DELETE Non-Block)"
Write-Host "6. Test Fatal Error (Breaks Chain)"
Write-Host "7. Test Parallel Blocking (Key A waits, Key B runs)"
$Choice = Read-Host "Select Scenario"

# ================== SCENARIO 1 ==================
if ($Choice -eq "1") {

    $Batch = @(
        @{ key="TAX101"; value='{"eventType":"CREATE","person":{"firstName":"SuccessA","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX101"}}' },
        @{ key="TAX102"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"FailSim","dateOfBirth":"1990-01-01","taxNumber":"TAX102"}}' },
        @{ key="TAX103"; value='{"eventType":"CREATE","person":{"firstName":"SuccessB","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX103"}}' },
        @{ key="TAX104"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"FailSim","dateOfBirth":"1990-01-01","taxNumber":"TAX104"}}' },
        @{ key="TAX105"; value='{"eventType":"CREATE","person":{"firstName":"SuccessC","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX105"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX101, TAX103, TAX105 -> Processed immediately"
    Write-Host "2. TAX102, TAX104 -> Sent to Retry Topic"
    Write-Host "3. Retry Consumer handles 102 & 104 asynchronously"
}

# ================== SCENARIO 2 ==================
elseif ($Choice -eq "2") {

    $Batch = @(
        @{ key="TAX201"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainStart","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
        @{ key="TAX201"; value='{"eventType":"UPDATE","person":{"firstName":"UpdatedName","lastName":"ChainEnd","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
        @{ key="TAX202"; value='{"eventType":"CREATE","person":{"firstName":"BlockedGuy","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX202"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX201 CREATE -> Fail -> Retry-1"
    Write-Host "2. TAX201 UPDATE -> Detects dependency -> Batch pauses"
    Write-Host "3. Retry Consumer fixes TAX201 CREATE"
    Write-Host "4. Batch resumes -> UPDATE -> TAX202 processed"
}

# ================== SCENARIO 3 ==================
elseif ($Choice -eq "3") {

    $Batch = @(
        @{ key="TAX301"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX301"; value='{"eventType":"UPDATE","person":{"firstName":"UpdA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX301"; value='{"eventType":"DELETE","person":{"firstName":"DelA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },

        @{ key="TAX302"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX302"}}' },
        @{ key="TAX303"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX303"}}' },

        @{ key="TAX304"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },
        @{ key="TAX304"; value='{"eventType":"UPDATE","person":{"firstName":"UpdB","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },

        @{ key="TAX305"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX305"}}' },
        @{ key="TAX306"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX306"}}' },
        @{ key="TAX307"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX307"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX302, 303, 305, 306, 307 -> SUCCESS"
    Write-Host "2. TAX301 & TAX304 CREATE -> Retry-1"
    Write-Host "3. UPDATE events WAIT for dependency"
    Write-Host "4. Retry Consumer resolves -> Batch completes"
}

# ================== SCENARIO 4 ==================
elseif ($Choice -eq "4") {
    # Goal: Prove that UPDATE failure does NOT block the chain
    $Batch = @(
        # 1. CREATE succeeds
        @{ key="TAX401"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },

        # 2. UPDATE fails (retryable) -> Should go to Retry Topic, but NOT set blocking flag
        @{ key="TAX401"; value='{"eventType":"UPDATE","person":{"firstName":"retry","lastName":"FailUpdate","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },

        # 3. DELETE -> Should process IMMEDIATELY (because UPDATE is not critical)
        @{ key="TAX401"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },

        # 4. Independent event
        @{ key="TAX402"; value='{"eventType":"CREATE","person":{"firstName":"Indep","lastName":"Guy","dateOfBirth":"1990-01-01","taxNumber":"TAX402"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX401 CREATE -> Success"
    Write-Host "2. TAX401 UPDATE -> Fail -> Retry-1 (But NO BLOCKING)"
    Write-Host "3. TAX401 DELETE -> Processed IMMEDIATELY (Does not wait for UPDATE)"
    Write-Host "4. TAX402 -> Success"
}

# ================== SCENARIO 5 ==================
elseif ($Choice -eq "5") {
    # Goal: Test complex chain with blocking and non-blocking failures
    $Batch = @(
        # --- BLOCK 1 ---
        # 1. CREATE Fails (Critical) -> Sets Block
        @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Blocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },

        # 2. UPDATE -> Waits for CREATE
        @{ key="TAX501"; value='{"eventType":"UPDATE","person":{"firstName":"Waiter","lastName":"One","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },

        # --- BLOCK 2 (After Block 1 resolves) ---
        # 3. DELETE Fails (Non-Critical) -> No Block
        @{ key="TAX501"; value='{"eventType":"DELETE","person":{"firstName":"retry","lastName":"NonBlocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },

        # 4. CREATE (New) -> Should process immediately after DELETE is offloaded
        @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"Final","lastName":"Success","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE (Fail) -> Blocks"
    Write-Host "2. UPDATE -> Waits for Redis Latch"
    Write-Host "3. DELETE (Fail) -> Offloaded to Retry, but allows next event"
    Write-Host "4. Final CREATE -> Processed immediately"
}

# ================== SCENARIO 6 ==================
elseif ($Choice -eq "6") {
    # Goal: Test FATAL error breaking the chain
    $Batch = @(
        # 1. CREATE Succeeds
        @{ key="TAX601"; value='{"eventType":"CREATE","person":{"firstName":"Good","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },

        # 2. UPDATE Fails FATALLY (Simulated) -> Breaks Chain
        @{ key="TAX601"; value='{"eventType":"UPDATE","person":{"firstName":"fatal","lastName":"Crash","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },

        # 3. DELETE -> Should go directly to DLT (Skipped)
        @{ key="TAX601"; value='{"eventType":"DELETE","person":{"firstName":"Skipped","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Success"
    Write-Host "2. UPDATE -> Fatal Error -> DLT -> Chain Broken flag set"
    Write-Host "3. DELETE -> Sent to DLT immediately (No processing attempt)"
}


# ================== SCENARIO 7 ==================
elseif ($Choice -eq "7") {
    # Goal: Verify Key A waiting does not block Key B
    $Batch = @(
        # KEY A: Blocked
        @{ key="TAX700"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockA","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },
        @{ key="TAX700"; value='{"eventType":"UPDATE","person":{"firstName":"Wait","lastName":"A","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },

        # KEY B: Success (Should finish before A resolves)
        @{ key="TAX701"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },
        @{ key="TAX701"; value='{"eventType":"UPDATE","person":{"firstName":"Fast","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },

        # KEY C: Blocked
        @{ key="TAX702"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockC","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' },
        @{ key="TAX702"; value='{"eventType":"DELETE","person":{"firstName":"Wait","lastName":"C","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX701 (Key B) -> Finishes immediately"
    Write-Host "2. TAX700 (Key A) & TAX702 (Key C) -> Both enter Wait state independently"
    Write-Host "3. Batch Consumer holds ACK until A and C resolve or timeout"
}


