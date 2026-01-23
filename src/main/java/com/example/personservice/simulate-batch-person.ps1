# # ================== CONFIGURATION ==================
# $BootstrapServers = "localhost:9092"
# $Topic = "person.kafka.batch"
# $KafkaBinPath = "C:\Users\nhnha\Downloads\kafka\bin\windows"
#
# # ================== CHECK PRODUCER =================
# $ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
# if (-not (Test-Path $ProducerBat)) {
#     Write-Error "Cannot find kafka-console-producer.bat"
#     exit
# }
#
# # ================== PRODUCE FUNCTION =================
# function Send-To-Kafka {
#     param (
#         [array]$Messages
#     )
#
#     Write-Host "Sending $($Messages.Count) events to '$Topic'..." -ForegroundColor Cyan
#
#     $InputData = $Messages | ForEach-Object {
#         "$($_.key):$($_.value)"
#     }
#
#     $InputData | & $ProducerBat `
#         --bootstrap-server $BootstrapServers `
#         --topic $Topic `
#         --property "parse.key=true" `
#         --property "key.separator=:"
# }
#
# # ================== MENU ==================
# Clear-Host
# Write-Host "=== KAFKA BATCH & RETRY TESTER ===" -ForegroundColor Yellow
# Write-Host ""
# Write-Host "1. Test Non-Blocking (Mixed Success/Fail)"
# Write-Host "   - 5 independent events"
# Write-Host "   - 2 fail (retry), 3 succeed"
# Write-Host ""
# Write-Host "2. Test Blocking (Dependent Chain)"
# Write-Host "   - Create (Fails) -> Update (Waits) -> Independent (Waits)"
# Write-Host ""
# Write-Host "3. Test Parallel Processing (10 Events)"
# Write-Host ""
# Write-Host "4. Test Non-Blocking Failure (UPDATE fails -> DELETE runs)"
# Write-Host "5. Test Mixed Criticality (CREATE Block -> UPDATE -> DELETE Non-Block)"
# Write-Host "6. Test Fatal Error (Breaks Chain)"
# Write-Host "7. Test Parallel Blocking (Key A waits, Key B runs)"
# $Choice = Read-Host "Select Scenario"
#
# # ================== SCENARIO 1 ==================
# if ($Choice -eq "1") {
#
#     $Batch = @(
#         @{ key="TAX101"; value='{"eventType":"CREATE","person":{"firstName":"SuccessA","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX101"}}' },
#         @{ key="TAX102"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"FailSim","dateOfBirth":"1990-01-01","taxNumber":"TAX102"}}' },
#         @{ key="TAX103"; value='{"eventType":"CREATE","person":{"firstName":"SuccessB","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX103"}}' },
#         @{ key="TAX104"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"FailSim","dateOfBirth":"1990-01-01","taxNumber":"TAX104"}}' },
#         @{ key="TAX105"; value='{"eventType":"CREATE","person":{"firstName":"SuccessC","lastName":"Test","dateOfBirth":"1990-01-01","taxNumber":"TAX105"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. TAX101, TAX103, TAX105 -> Processed immediately"
#     Write-Host "2. TAX102, TAX104 -> Sent to Retry Topic"
#     Write-Host "3. Retry Consumer handles 102 & 104 asynchronously"
# }
#
# # ================== SCENARIO 2 ==================
# elseif ($Choice -eq "2") {
#
#     $Batch = @(
#         @{ key="TAX201"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainStart","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
#         @{ key="TAX201"; value='{"eventType":"UPDATE","person":{"firstName":"UpdatedName","lastName":"ChainEnd","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
#         @{ key="TAX202"; value='{"eventType":"CREATE","person":{"firstName":"BlockedGuy","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX202"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. TAX201 CREATE -> Fail -> Retry-1"
#     Write-Host "2. TAX201 UPDATE -> Detects dependency -> Batch pauses"
#     Write-Host "3. Retry Consumer fixes TAX201 CREATE"
#     Write-Host "4. Batch resumes -> UPDATE -> TAX202 processed"
# }
#
# # ================== SCENARIO 3 ==================
# elseif ($Choice -eq "3") {
#
#     $Batch = @(
#         @{ key="TAX301"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
#         @{ key="TAX301"; value='{"eventType":"UPDATE","person":{"firstName":"UpdA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
#         @{ key="TAX301"; value='{"eventType":"DELETE","person":{"firstName":"DelA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
#
#         @{ key="TAX302"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX302"}}' },
#         @{ key="TAX303"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX303"}}' },
#
#         @{ key="TAX304"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },
#         @{ key="TAX304"; value='{"eventType":"UPDATE","person":{"firstName":"UpdB","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },
#
#         @{ key="TAX305"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX305"}}' },
#         @{ key="TAX306"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX306"}}' },
#         @{ key="TAX307"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX307"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. TAX302, 303, 305, 306, 307 -> SUCCESS"
#     Write-Host "2. TAX301 & TAX304 CREATE -> Retry-1"
#     Write-Host "3. UPDATE events WAIT for dependency"
#     Write-Host "4. Retry Consumer resolves -> Batch completes"
# }
#
# # ================== SCENARIO 4 ==================
# elseif ($Choice -eq "4") {
#     # Goal: Prove that UPDATE failure does NOT block the chain
#     $Batch = @(
#         # 1. CREATE succeeds
#         @{ key="TAX401"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
#
#         # 2. UPDATE fails (retryable) -> Should go to Retry Topic, but NOT set blocking flag
#         @{ key="TAX401"; value='{"eventType":"UPDATE","person":{"firstName":"retry","lastName":"FailUpdate","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
#
#         # 3. DELETE -> Should process IMMEDIATELY (because UPDATE is not critical)
#         @{ key="TAX401"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
#
#         # 4. Independent event
#         @{ key="TAX402"; value='{"eventType":"CREATE","person":{"firstName":"Indep","lastName":"Guy","dateOfBirth":"1990-01-01","taxNumber":"TAX402"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. TAX401 CREATE -> Success"
#     Write-Host "2. TAX401 UPDATE -> Fail -> Retry-1 (But NO BLOCKING)"
#     Write-Host "3. TAX401 DELETE -> Processed IMMEDIATELY (Does not wait for UPDATE)"
#     Write-Host "4. TAX402 -> Success"
# }
#
# # ================== SCENARIO 5 ==================
# elseif ($Choice -eq "5") {
#     # Goal: Test complex chain with blocking and non-blocking failures
#     $Batch = @(
#         # --- BLOCK 1 ---
#         # 1. CREATE Fails (Critical) -> Sets Block
#         @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Blocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
#
#         # 2. UPDATE -> Waits for CREATE
#         @{ key="TAX501"; value='{"eventType":"UPDATE","person":{"firstName":"Waiter","lastName":"One","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
#
#         # --- BLOCK 2 (After Block 1 resolves) ---
#         # 3. DELETE Fails (Non-Critical) -> No Block
#         @{ key="TAX501"; value='{"eventType":"DELETE","person":{"firstName":"retry","lastName":"NonBlocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
#
#         # 4. CREATE (New) -> Should process immediately after DELETE is offloaded
#         @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"Final","lastName":"Success","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. CREATE (Fail) -> Blocks"
#     Write-Host "2. UPDATE -> Waits for Redis Latch"
#     Write-Host "3. DELETE (Fail) -> Offloaded to Retry, but allows next event"
#     Write-Host "4. Final CREATE -> Processed immediately"
# }
#
# # ================== SCENARIO 6 ==================
# elseif ($Choice -eq "6") {
#     # Goal: Test FATAL error breaking the chain
#     $Batch = @(
#         # 1. CREATE Succeeds
#         @{ key="TAX601"; value='{"eventType":"CREATE","person":{"firstName":"Good","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },
#
#         # 2. UPDATE Fails FATALLY (Simulated) -> Breaks Chain
#         @{ key="TAX601"; value='{"eventType":"UPDATE","person":{"firstName":"fatal","lastName":"Crash","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },
#
#         # 3. DELETE -> Should go directly to DLT (Skipped)
#         @{ key="TAX601"; value='{"eventType":"DELETE","person":{"firstName":"Skipped","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. CREATE -> Success"
#     Write-Host "2. UPDATE -> Fatal Error -> DLT -> Chain Broken flag set"
#     Write-Host "3. DELETE -> Sent to DLT immediately (No processing attempt)"
# }
#
#
# # ================== SCENARIO 7 ==================
# elseif ($Choice -eq "7") {
#     # Goal: Verify Key A waiting does not block Key B
#     $Batch = @(
#         # KEY A: Blocked
#         @{ key="TAX700"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockA","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },
#         @{ key="TAX700"; value='{"eventType":"UPDATE","person":{"firstName":"Wait","lastName":"A","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },
#
#         # KEY B: Success (Should finish before A resolves)
#         @{ key="TAX701"; value='{"eventType":"CREATE","person":{"firstName":"Fast","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },
#         @{ key="TAX701"; value='{"eventType":"UPDATE","person":{"firstName":"Fast","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },
#
#         # KEY C: Blocked
#         @{ key="TAX702"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockC","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' },
#         @{ key="TAX702"; value='{"eventType":"DELETE","person":{"firstName":"Wait","lastName":"C","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' }
#     )
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. TAX701 (Key B) -> Finishes immediately"
#     Write-Host "2. TAX700 (Key A) & TAX702 (Key C) -> Both enter Wait state independently"
#     Write-Host "3. Batch Consumer holds ACK until A and C resolve or timeout"
# }
#
#


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
Write-Host "Note: All scenarios assume 1 partition for the topic, ensuring global order preservation."
Write-Host "Failures simulated via 'firstName': 'retry' for retryable, 'fatal' for fatal errors."
Write-Host ""
Write-Host "1. Test Non-Blocking (Mixed Success/Fail)"
Write-Host "   - 5 independent events"
Write-Host "   - 2 fail (retry), 3 succeed"
Write-Host ""
Write-Host "2. Test Blocking (Dependent Chain)"
Write-Host "   - Create (Fails) -> Update (Waits) -> Independent (Proceeds)"
Write-Host ""
Write-Host "3. Test Parallel Processing (10 Events)"
Write-Host "   - Mixed chains and independents"
Write-Host ""
Write-Host "4. Test Non-Blocking Failure (UPDATE fails -> DELETE runs)"
Write-Host ""
Write-Host "5. Test Mixed Criticality (CREATE Block -> UPDATE -> DELETE Non-Block)"
Write-Host ""
Write-Host "6. Test Fatal Error (Breaks Chain)"
Write-Host ""
Write-Host "7. Test Parallel Blocking (Key A waits, Key B runs)"
Write-Host ""
Write-Host "8. Test All Success (Basic Sanity)"
Write-Host "   - 5 successful independent events"
Write-Host ""
Write-Host "9. Test Retry Success Chain"
Write-Host "   - Fail -> Retry succeeds -> Blocked events resume"
Write-Host "   - Requires manual retry simulation or observation"
Write-Host ""
Write-Host "10. Test Max Retry Failure to DLT"
Write-Host "   - Fail with max retries -> DLT -> Unblock with DLT status"
Write-Host "   - Requires observing retry consumer behavior"
Write-Host ""
Write-Host "11. Test Timeout on Wait"
Write-Host "   - Fail -> Long wait -> Timeout -> DLT"
Write-Host "   - Adjust timeout in code or simulate delay"
Write-Host ""
Write-Host "12. Test Large Batch Stress (20 Events)"
Write-Host "   - Mixed successes, failures, dependencies"
Write-Host ""
Write-Host "13. Test Edge Cases (Empty, Single, Invalid)"
Write-Host "   - Empty batch, single event, duplicate keys, malformed JSON"
Write-Host ""
Write-Host "14. Test Post-Retry Order Preservation"
Write-Host "   - Fail in middle -> Retry -> Ensure subsequent order"
Write-Host ""
Write-Host "15. Test Multi-Key Interleaving with Failures"
Write-Host "   - Interleaved keys with selective failures"
Write-Host ""
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
    Write-Host "1. TAX101, TAX103, TAX105 -> Processed immediately (success)"
    Write-Host "2. TAX102, TAX104 -> Sent to Retry Topic (non-blocking for others)"
    Write-Host "3. Retry Consumer handles 102 & 104 asynchronously"
    Write-Host "4. Batch ACK after all independents done, retries offloaded"
}

# ================== SCENARIO 2 ==================
elseif ($Choice -eq "2") {

    $Batch = @(
        @{ key="TAX201"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainStart","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
        @{ key="TAX201"; value='{"eventType":"UPDATE","person":{"firstName":"UpdatedName","lastName":"ChainEnd","dateOfBirth":"1990-01-01","taxNumber":"TAX201"}}' },
        @{ key="TAX202"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX202"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX201 CREATE -> Fail -> Retry-1, set retrying flag"
    Write-Host "2. TAX201 UPDATE -> Detects retrying -> Waits on Redis latch"
    Write-Host "3. TAX202 CREATE -> Proceeds immediately (independent)"
    Write-Host "4. After retry success, UPDATE resumes"
    Write-Host "5. Batch ACK after all resolved"
}

# ================== SCENARIO 3 ==================
elseif ($Choice -eq "3") {

    $Batch = @(
        @{ key="TAX301"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX301"; value='{"eventType":"UPDATE","person":{"firstName":"UpdA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },
        @{ key="TAX301"; value='{"eventType":"DELETE","person":{"firstName":"DelA","lastName":"ChainA","dateOfBirth":"1990-01-01","taxNumber":"TAX301"}}' },

        @{ key="TAX302"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX302"}}' },
        @{ key="TAX303"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX303"}}' },

        @{ key="TAX304"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },
        @{ key="TAX304"; value='{"eventType":"UPDATE","person":{"firstName":"UpdB","lastName":"ChainB","dateOfBirth":"1990-01-01","taxNumber":"TAX304"}}' },

        @{ key="TAX305"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX305"}}' },
        @{ key="TAX306"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX306"}}' },
        @{ key="TAX307"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX307"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. Independent TAX302,303,305,306,307 -> Success immediately"
    Write-Host "2. TAX301 & TAX304 CREATE -> Fail -> Retry-1, set flags"
    Write-Host "3. Their UPDATE/DELETE -> Wait independently"
    Write-Host "4. After retries succeed, chains resume sequentially per key"
    Write-Host "5. Batch ACK after all"
}

# ================== SCENARIO 4 ==================
elseif ($Choice -eq "4") {
    $Batch = @(
        @{ key="TAX401"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX401"; value='{"eventType":"UPDATE","person":{"firstName":"retry","lastName":"FailUpdate","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX401"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX401"}}' },
        @{ key="TAX402"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Guy","dateOfBirth":"1990-01-01","taxNumber":"TAX402"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX401 CREATE -> Success"
    Write-Host "2. TAX401 UPDATE -> Fail -> Retry-1, set retrying flag"
    Write-Host "3. TAX401 DELETE -> Waits on latch (dependent)"
    Write-Host "4. TAX402 CREATE -> Success (independent)"
    Write-Host "5. After retry, DELETE resumes"
}

# ================== SCENARIO 5 ==================
elseif ($Choice -eq "5") {
    $Batch = @(
        @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Blocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
        @{ key="TAX501"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"One","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
        @{ key="TAX501"; value='{"eventType":"DELETE","person":{"firstName":"retry","lastName":"NonBlocker","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' },
        @{ key="TAX501"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Success","dateOfBirth":"1990-01-01","taxNumber":"TAX501"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Fail -> Block set"
    Write-Host "2. UPDATE -> Waits"
    Write-Host "3. After retry success, UPDATE processes"
    Write-Host "4. DELETE -> Fail -> Retry, set new block (if DELETE is blocking)"
    Write-Host "5. Next CREATE -> Waits if blocked, else proceeds"
    Write-Host "Note: Adjust if DELETE failures are non-blocking per domain"
}

# ================== SCENARIO 6 ==================
elseif ($Choice -eq "6") {
    $Batch = @(
        @{ key="TAX601"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Start","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },
        @{ key="TAX601"; value='{"eventType":"UPDATE","person":{"firstName":"fatal","lastName":"Crash","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' },
        @{ key="TAX601"; value='{"eventType":"DELETE","person":{"firstName":"Skipped","lastName":"End","dateOfBirth":"1990-01-01","taxNumber":"TAX601"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Success"
    Write-Host "2. UPDATE -> Fatal -> DLT, chain broken"
    Write-Host "3. DELETE -> Detected broken chain -> DLT without attempt"
}

# ================== SCENARIO 7 ==================
elseif ($Choice -eq "7") {
    $Batch = @(
        @{ key="TAX700"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockA","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },
        @{ key="TAX700"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"A","dateOfBirth":"1990-01-01","taxNumber":"TAX700"}}' },

        @{ key="TAX701"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },
        @{ key="TAX701"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"B","dateOfBirth":"1990-01-01","taxNumber":"TAX701"}}' },

        @{ key="TAX702"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"BlockC","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' },
        @{ key="TAX702"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"C","dateOfBirth":"1990-01-01","taxNumber":"TAX702"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX701 chain -> All success, proceeds"
    Write-Host "2. TAX700 & TAX702 CREATE -> Fail -> Flags set"
    Write-Host "3. Their UPDATE/DELETE -> Wait independently"
    Write-Host "4. No cross-key blocking; ACK after all resolve"
}

# ================== SCENARIO 8: All Success ==================
elseif ($Choice -eq "8") {
    $Batch = @(
        @{ key="TAX801"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"One","dateOfBirth":"1990-01-01","taxNumber":"TAX801"}}' },
        @{ key="TAX802"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"Two","dateOfBirth":"1990-01-01","taxNumber":"TAX802"}}' },
        @{ key="TAX803"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"Three","dateOfBirth":"1990-01-01","taxNumber":"TAX803"}}' },
        @{ key="TAX804"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Four","dateOfBirth":"1990-01-01","taxNumber":"TAX804"}}' },
        @{ key="TAX805"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"Five","dateOfBirth":"1990-01-01","taxNumber":"TAX805"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. All events process successfully in sequence"
    Write-Host "2. No retries or waits triggered"
    Write-Host "3. Immediate ACK"
}

# ================== SCENARIO 9: Retry Success Chain ==================
elseif ($Choice -eq "9") {
    $Batch = @(
        @{ key="TAX901"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"InitialFail","dateOfBirth":"1990-01-01","taxNumber":"TAX901"}}' },
        @{ key="TAX901"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"AfterRetry","dateOfBirth":"1990-01-01","taxNumber":"TAX901"}}' },
        @{ key="TAX901"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"Final","dateOfBirth":"1990-01-01","taxNumber":"TAX901"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Fail -> Retry-1"
    Write-Host "2. UPDATE & DELETE -> Wait on latch"
    Write-Host "3. Retry consumer succeeds (assume simulation) -> Notify SUCCESS"
    Write-Host "4. Batch resumes UPDATE then DELETE in order"
    Write-Host "Observe logs for retry success notification"
}

# ================== SCENARIO 10: Max Retry to DLT ==================
elseif ($Choice -eq "10") {
    $Batch = @(
        @{ key="TAX1001"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"PersistentFail","dateOfBirth":"1990-01-01","taxNumber":"TAX101"}}' },  # Assume this fails all retries
        @{ key="TAX1001"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"Blocked","dateOfBirth":"1990-01-01","taxNumber":"TAX101"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Fail -> Retry-1 -> Retry-2 -> Retry-3 -> DLT"
    Write-Host "2. UPDATE -> Waits -> After max, notify DLT -> UPDATE sent to DLT"
    Write-Host "3. Observe retry consumer logs for max retries"
    Write-Host "Note: Simulate persistent failure in service"
}

# ================== SCENARIO 11: Timeout on Wait ==================
elseif ($Choice -eq "11") {
    $Batch = @(
        @{ key="TAX1101"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"LongDelay","dateOfBirth":"1990-01-01","taxNumber":"TAX111"}}' },
        @{ key="TAX1101"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"TimeoutVictim","dateOfBirth":"1990-01-01","taxNumber":"TAX111"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Fail -> Retry (assume delayed or failed)"
    Write-Host "2. UPDATE -> Waits 60s -> Timeout -> Sent to DLT"
    Write-Host "3. Flag cleared"
    Write-Host "Note: Simulate by not resolving retry quickly"
}

# ================== SCENARIO 12: Large Batch Stress ==================
# elseif ($Choice -eq "12") {
#     $Batch = @()  # Generate 20 mixed events
#     for ($i = 1; $i -le 10; $i++) {
#         $tax = "TAX120$i"
#         $Batch += @{ key=$tax; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"'$tax'"}}' }
#     }
#     for ($i = 11; $i -le 15; $i++) {
#         $tax = "TAX120$i"
#         $Batch += @{ key=$tax; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"Fail","dateOfBirth":"1990-01-01","taxNumber":"'$tax'"}}' }
#         $Batch += @{ key=$tax; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"After","dateOfBirth":"1990-01-01","taxNumber":"'$tax'"}}' }
#     }
#
#     Send-To-Kafka -Messages $Batch
#
#     Write-Host ""
#     Write-Host "Expected Result:" -ForegroundColor Green
#     Write-Host "1. 10 successes process immediately"
#     Write-Host "2. 5 chains: CREATE fail -> UPDATE wait"
#     Write-Host "3. Stress sequential processing and memory"
#     Write-Host "4. ACK after all resolutions"
# }

# ================== SCENARIO 13: Edge Cases ==================
elseif ($Choice -eq "13") {
    $Batch = @(
        # Single success
        @{ key="TAX1301"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Single","dateOfBirth":"1990-01-01","taxNumber":"TAX1301"}}' },
        # Duplicate key success
        @{ key="TAX1302"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Dup","dateOfBirth":"1990-01-01","taxNumber":"TAX1302"}}' },
        @{ key="TAX1302"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Dup","dateOfBirth":"1990-01-01","taxNumber":"TAX1302"}}' },
        # Malformed (assume handler)
        @{ key="TAX1303"; value='{"eventType":"INVALID","person":{"firstName":"Bad","lastName":"Data"}}' }  # Missing fields
    )
    # Empty batch not sendable, but test single/empty via manual

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. Single -> Success"
    Write-Host "2. Duplicates -> Process in order (domain dependent)"
    Write-Host "3. Malformed -> Error classification (retry or fatal)"
    Write-Host "For empty: No send, or test consumer idle"
}

# ================== SCENARIO 14: Post-Retry Order ==================
elseif ($Choice -eq "14") {
    $Batch = @(
        @{ key="TAX1401"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Before","dateOfBirth":"1990-01-01","taxNumber":"TAX1401"}}' },
        @{ key="TAX1401"; value='{"eventType":"UPDATE","person":{"firstName":"retry","lastName":"MidFail","dateOfBirth":"1990-01-01","taxNumber":"TAX1401"}}' },
        @{ key="TAX1401"; value='{"eventType":"DELETE","person":{"firstName":"Success","lastName":"After","dateOfBirth":"1990-01-01","taxNumber":"TAX1401"}}' },
        @{ key="TAX1402"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"Indep","dateOfBirth":"1990-01-01","taxNumber":"TAX1402"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. CREATE -> Success"
    Write-Host "2. UPDATE -> Fail -> Retry"
    Write-Host "3. DELETE -> Wait"
    Write-Host "4. TAX1402 -> Success"
    Write-Host "5. After retry, DELETE processes (order preserved post-unblock)"
}

# ================== SCENARIO 15: Multi-Key Interleaving ==================
elseif ($Choice -eq "15") {
    $Batch = @(
        @{ key="TAX1501"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"A1","dateOfBirth":"1990-01-01","taxNumber":"TAX1501"}}' },
        @{ key="TAX1502"; value='{"eventType":"CREATE","person":{"firstName":"retry","lastName":"B1Fail","dateOfBirth":"1990-01-01","taxNumber":"TAX1502"}}' },
        @{ key="TAX1501"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"A2","dateOfBirth":"1990-01-01","taxNumber":"TAX1501"}}' },
        @{ key="TAX1502"; value='{"eventType":"UPDATE","person":{"firstName":"Success","lastName":"B2Wait","dateOfBirth":"1990-01-01","taxNumber":"TAX1502"}}' },
        @{ key="TAX1503"; value='{"eventType":"CREATE","person":{"firstName":"Success","lastName":"C1","dateOfBirth":"1990-01-01","taxNumber":"TAX1503"}}' }
    )

    Send-To-Kafka -Messages $Batch

    Write-Host ""
    Write-Host "Expected Result:" -ForegroundColor Green
    Write-Host "1. TAX1501 CREATE -> Success"
    Write-Host "2. TAX1502 CREATE -> Fail -> Retry"
    Write-Host "3. TAX1501 UPDATE -> Success (independent)"
    Write-Host "4. TAX1502 UPDATE -> Wait"
    Write-Host "5. TAX1503 CREATE -> Success"
    Write-Host "6. Order preserved: A1, B1 fail, A2, B2 wait, C1"
    Write-Host "7. After retry, B2 resumes"
}