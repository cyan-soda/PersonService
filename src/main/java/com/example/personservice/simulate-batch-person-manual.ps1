$topic = "person.kafka.manual.batch"
$bootstrapServer = "localhost:9092"

Write-Host "Sending Person Events to $topic with full JSON structure..."

# Generate IDs
$id_success = [Guid]::NewGuid().ToString()
$id_fail    = [Guid]::NewGuid().ToString()
$id_other   = [Guid]::NewGuid().ToString()

# Helper function to create the specific JSON object
function New-PersonEventJSON {
    param (
        [string]$eventType,
        [string]$personId,
        [string]$firstName,
        [string]$lastName,
        [string]$taxNumber
    )

    $now = Get-Date
    # Constructing the exact object structure
    $eventObj = @{
        eventId = [Guid]::NewGuid().ToString()
        eventType = $eventType
        # Java LocalDateTime serializes as array: [Year, Month, Day, Hour, Minute, Second, Nano]
        occurredOn = @($now.Year, $now.Month, $now.Day, $now.Hour, $now.Minute, $now.Second, $now.Millisecond * 1000000)
        person = @{
            id = $personId
            firstName = $firstName
            lastName = $lastName
            dateOfBirth = @(1990, 1, 1)
            taxNumber = $taxNumber
            taxDebt = 0
            createdAt = $null
            updatedAt = $null
            age = 35
        }
    }

    # Convert to JSON and compress to single line for Kafka Console Producer
    return $eventObj | ConvertTo-Json -Depth 5 -Compress
}

# Define the Batch of 5 Events
$events = @(
    # 1. Happy Path (Create John)
    @{ k=$id_success; v=$(New-PersonEventJSON "CREATE" $id_success "John" "Doe" "TAX100") },

    # 2. Failure Path (Create FailUser) -> This name should trigger the error in Java
    @{ k=$id_fail;    v=$(New-PersonEventJSON "CREATE" $id_fail "Fail" "User" "TAX999") },

    # 3. Happy Path (Create Jane)
    @{ k=$id_other;   v=$(New-PersonEventJSON "CREATE" $id_other "Jane" "Smith" "TAX200") },

    # 4. Happy Path Update (Update John)
    @{ k=$id_success; v=$(New-PersonEventJSON "UPDATE" $id_success "John" "DoeUpdated" "TAX100") },

    # 5. Dependency Skip (Update FailUser) -> Should be skipped in Non-Blocking mode because CREATE failed
    @{ k=$id_fail;    v=$(New-PersonEventJSON "UPDATE" $id_fail "Fail" "UserUpdated" "TAX999") }
)

# Send to Kafka
foreach ($e in $events) {
    $key = $e.k
    # Escape double quotes for the shell command
    $msg = $e.v.Replace('"', '\"')

    # Send using Podman
    echo "${key}:${msg}" | & "C:\Program Files\RedHat\Podman\podman.exe" exec -i broker `
      kafka-console-producer `
      --broker-list $bootstrapServer `
      --topic $topic `
      --property "parse.key=true" `
      --property "key.separator=:"

}

Write-Host "Done. 5 complex events sent."
