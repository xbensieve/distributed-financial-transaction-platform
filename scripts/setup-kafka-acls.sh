#!/usr/bin/env bash
# ==============================================================================
# DFTP — Production Kafka Broker SCRAM-SHA-512 & ACL Provisioning Script
# Enforces broker-level authorization & least-privilege access control.
# ==============================================================================

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
CONFIG_FILE="${KAFKA_ADMIN_CONFIG:-/etc/kafka/adminclient.properties}"

echo "=============================================================================="
echo "DFTP: Provisioning SCRAM Users and Broker ACLs on ${BOOTSTRAP_SERVER}"
echo "=============================================================================="

# 1. Provision SCRAM-SHA-512 Credentials for Services
echo "[1/4] Provisioning SCRAM-SHA-512 Users..."

kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type users --entity-name transaction-service \
    --alter --add-config 'SCRAM-SHA-512=[password=tx-service-secret-512]'

kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type users --entity-name account-service \
    --alter --add-config 'SCRAM-SHA-512=[password=acc-service-secret-512]'

kafka-configs --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --entity-type users --entity-name ledger-service \
    --alter --add-config 'SCRAM-SHA-512=[password=ledger-service-secret-512]'

echo "[1/4] SCRAM Users Provisioned Successfully."

# 2. Provision Topics
echo "[2/4] Ensuring Core Topics Exist..."
for topic in transaction-events account-events ledger-events transaction-events.DLT account-events.DLT ledger-events.DLT; do
    kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create --if-not-exists --topic "${topic}" \
        --partitions 3 --replication-factor 1
done

# 3. Provision Least-Privilege ACLs per Microservice
echo "[3/4] Configuring Broker-Enforced ACLs..."

# ------------------------------------------------------------------------------
# TRANSACTION-SERVICE
# Produces to: transaction-events, transaction-events.DLT
# Consumes from: account-events, ledger-events
# Group: transaction-service-*
# ------------------------------------------------------------------------------
kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:transaction-service \
    --operation Write --operation Describe \
    --topic transaction-events --topic transaction-events.DLT

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:transaction-service \
    --operation Read --operation Describe \
    --topic account-events --topic ledger-events

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:transaction-service \
    --operation Read \
    --group transaction-service-saga-group \
    --group transaction-service-account-group

# ------------------------------------------------------------------------------
# ACCOUNT-SERVICE
# Produces to: account-events, account-events.DLT
# Consumes from: transaction-events
# Group: account-service-*
# ------------------------------------------------------------------------------
kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:account-service \
    --operation Write --operation Describe \
    --topic account-events --topic account-events.DLT

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:account-service \
    --operation Read --operation Describe \
    --topic transaction-events

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:account-service \
    --operation Read \
    --group account-service-saga-group

# ------------------------------------------------------------------------------
# LEDGER-SERVICE
# Produces to: ledger-events, ledger-events.DLT
# Consumes from: transaction-events
# Group: ledger-service-*
# ------------------------------------------------------------------------------
kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:ledger-service \
    --operation Write --operation Describe \
    --topic ledger-events --topic ledger-events.DLT

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:ledger-service \
    --operation Read --operation Describe \
    --topic transaction-events

kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --add \
    --allow-principal User:ledger-service \
    --operation Read \
    --group ledger-service-saga-group

# 4. Verify Provisioned ACLs
echo "[4/4] Listing Configured ACLs..."
kafka-acls --bootstrap-server "${BOOTSTRAP_SERVER}" --list

echo "=============================================================================="
echo "DFTP: Kafka SCRAM Users and Least-Privilege ACLs Successfully Configured."
echo "=============================================================================="
