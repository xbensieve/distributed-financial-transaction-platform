# ==============================================================================
# DFTP — Production Kafka Broker SCRAM-SHA-512 & ACL Provisioning Script (PowerShell)
# Enforces broker-level authorization & least-privilege access control.
# ==============================================================================

param(
    [string]$BootstrapServer = "localhost:9092"
)

Write-Host "==============================================================================" -ForegroundColor Cyan
Write-Host "DFTP: Provisioning SCRAM Users and Broker ACLs on $BootstrapServer" -ForegroundColor Cyan
Write-Host "==============================================================================" -ForegroundColor Cyan

# 1. Provision SCRAM-SHA-512 Credentials
Write-Host "[1/4] Provisioning SCRAM-SHA-512 Users..." -ForegroundColor Yellow

docker exec dftp-kafka-broker kafka-configs --bootstrap-server $BootstrapServer `
    --entity-type users --entity-name transaction-service `
    --alter --add-config 'SCRAM-SHA-512=[password=tx-service-secret-512]'

docker exec dftp-kafka-broker kafka-configs --bootstrap-server $BootstrapServer `
    --entity-type users --entity-name account-service `
    --alter --add-config 'SCRAM-SHA-512=[password=acc-service-secret-512]'

docker exec dftp-kafka-broker kafka-configs --bootstrap-server $BootstrapServer `
    --entity-type users --entity-name ledger-service `
    --alter --add-config 'SCRAM-SHA-512=[password=ledger-service-secret-512]'

# 2. Provision Topics
Write-Host "[2/4] Ensuring Core Topics Exist..." -ForegroundColor Yellow
$topics = @("transaction-events", "account-events", "ledger-events", "transaction-events.DLT", "account-events.DLT", "ledger-events.DLT")
foreach ($topic in $topics) {
    docker exec dftp-kafka-broker kafka-topics --bootstrap-server $BootstrapServer `
        --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1
}

# 3. Provision Least-Privilege ACLs
Write-Host "[3/4] Configuring Broker-Enforced ACLs..." -ForegroundColor Yellow

# TRANSACTION-SERVICE
docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:transaction-service --operation Write --operation Describe `
    --topic transaction-events --topic transaction-events.DLT

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:transaction-service --operation Read --operation Describe `
    --topic account-events --topic ledger-events

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:transaction-service --operation Read `
    --group transaction-service-saga-group --group transaction-service-account-group

# ACCOUNT-SERVICE
docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:account-service --operation Write --operation Describe `
    --topic account-events --topic account-events.DLT

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:account-service --operation Read --operation Describe `
    --topic transaction-events

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:account-service --operation Read `
    --group account-service-saga-group

# LEDGER-SERVICE
docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:ledger-service --operation Write --operation Describe `
    --topic ledger-events --topic ledger-events.DLT

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:ledger-service --operation Read --operation Describe `
    --topic transaction-events

docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --add `
    --allow-principal User:ledger-service --operation Read `
    --group ledger-service-saga-group

# 4. Verification
Write-Host "[4/4] Listing Configured ACLs..." -ForegroundColor Yellow
docker exec dftp-kafka-broker kafka-acls --bootstrap-server $BootstrapServer --list

Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "DFTP: Kafka SCRAM Users and Least-Privilege ACLs Successfully Configured." -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Green
