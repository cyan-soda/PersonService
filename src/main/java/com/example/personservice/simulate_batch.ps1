# CHANGE THIS to your actual Kafka installation folder
$KafkaPath = "C:\Users\nhnha\Downloads\kafka"
$ProducerCmd = "$KafkaPath\bin\windows\kafka-console-producer.bat"
$Broker = "localhost:9092"
$Topic = "tax.calculation.kafka"

function Send-KafkaMsg($Json) {
    # It's good practice to generate a unique eventId and current timestamp for each message
    $eventId = [guid]::NewGuid().ToString()
    $occurredOn = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'")

    # Inject these into the JSON string
    $fullJson = $Json -replace '}$', ",`"eventId`":`"$eventId`",`"eventType`":`"ADD`",`"occurredOn`":`"$occurredOn`"}"

    Write-Host "Sending: $fullJson"
    $fullJson | & $ProducerCmd --bootstrap-server $Broker --topic $Topic
}

Write-Host "Step 1: Sending healthy messages..." -ForegroundColor Green
Send-KafkaMsg '{"taxId":"TAX880", "amount":1.0}'
Send-KafkaMsg '{"taxId":"TAX880", "amount":2.0}'

Write-Host "Step 2: Sending 'Poison Pill' (Malformed JSON)..." -ForegroundColor Yellow
# This message has invalid JSON syntax (extra comma) which will fail deserialization
'{"taxId":"TAX888", "amount":4.0}' | & $ProducerCmd --bootstrap-server $Broker --topic $Topic

Write-Host "Step 3: Sending more valid messages..." -ForegroundColor Green
Send-KafkaMsg '{"taxId":"TAX880", "amount":3.0}'

Write-Host "--- Done! Check your Spring Boot Console logs and Kafka UI for the DLT ---" -ForegroundColor Cyan