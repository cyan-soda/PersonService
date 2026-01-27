# ================== CONFIGURATION ==================
$BootstrapServers = "localhost:9092"
$Topic = "tax.kafka.manual.batch"
$KafkaBinPath = "C:\Users\nhnha\Downloads\kafka\bin\windows"

# ================== CHECK PRODUCER =================
$ProducerBat = Join-Path $KafkaBinPath "kafka-console-producer.bat"
if (-not (Test-Path $ProducerBat)) {
    Write-Error "Cannot find kafka-console-producer.bat at $ProducerBat"
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

# ================== MAIN LOGIC ==================
Clear-Host
Write-Host "Generating 50 valid JSON events (TAX501 - TAX550)..."
Write-Host ""

$Batch = @()

for ($i = 1; $i -le 50; $i++) {
    $Num = 500 + $i
    $TaxId = "TAX$Num"
    $Amount = 10.0 + $i

    $JsonPayload = '{"eventType":"ADD","taxId":"' + $TaxId + '","amount":' + $Amount + '}'

    $Batch += @{ key=$TaxId; value=$JsonPayload }
}

Send-To-Kafka -Messages $Batch

Write-Host ""
Write-Host "Done. Sent 50 events (TAX501 to TAX550)." -ForegroundColor Green
