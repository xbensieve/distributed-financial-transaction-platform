# Ledger Service

The Ledger Service is the authoritative owner of the financial accounting record in the Distributed Financial Transaction Platform.

## Core Responsibilities
- Maintain an immutable, double-entry financial ledger.
- Ingest `TransactionConfirmed` events to produce exact, balanced accounting entries (`postings`).
- Produce `LedgerTransactionPosted` events for downstream services via the Outbox pattern.

## Accounting Invariants
The Ledger strictly enforces the fundamental double-entry invariant:
```
SUM(Debits) = SUM(Credits)
```
This is enforced at two independent protection layers:

1. **Application Level (first barrier):** `LedgerApplicationService.postTransaction()` validates that the debit and credit amounts match before writing to the database. This catches obvious violations early and provides clear error messages.

2. **Database Level (final authority):** A PostgreSQL `DEFERRABLE INITIALLY DEFERRED` constraint trigger (`check_ledger_balance`) in `V1__init_ledger_schema.sql` guarantees that no transaction can commit unbalanced postings. This fires at `COMMIT` time and rejects the entire transaction if `SUM(DEBIT) - SUM(CREDIT) != 0`. The database is the final arbiter — even if the application-level check is bypassed, the database will reject the invalid state.

## Idempotency and Concurrency Semantics
Two independent deduplication layers protect against duplicate financial effects:

- **Transport Idempotency (Inbox — `eventId`):** The `TransactionEventConsumer` checks the `inbox_messages` table by `eventId` before processing. If the same `eventId` has already been processed, the event is silently acknowledged. This protects against Kafka delivering the same message multiple times (transport-level redelivery).

- **Business Idempotency (Database — `business_transaction_id`):** The `ledger_transactions` table has a `UNIQUE` constraint on `business_transaction_id`. If two messages with different `eventId`s carry the same `transactionId` (business intent), the second thread's `INSERT` will violate the unique constraint. `LedgerApplicationService` catches the resulting `DataIntegrityViolationException` and treats it as an idempotent success — it returns without throwing, preventing infinite Kafka retry loops.

Identity boundary:
```
eventId         = message identity (transport deduplication via Inbox)
transactionId   = business operation identity (financial deduplication via UNIQUE constraint)
```

## Immutability Policy
Accounting history is strictly immutable, enforced at two levels:

- **Application Level:** JPA entity fields (`LedgerTransaction`, `Posting`) are annotated with `updatable=false`, preventing accidental mutation through the application ORM layer.

- **Database Level:** PostgreSQL `BEFORE UPDATE` and `BEFORE DELETE` triggers defined in `V1__init_ledger_schema.sql` reject any attempt to modify or delete committed accounting records — including raw SQL executed outside the application. Any historical correction must be performed via a compensating (reversal) transaction, not by mutating existing records.

## Account Projection and Eventual Consistency
The Ledger maintains a local projection of accounts populated via `AccountCreated` events from the Account Service.

If a `TransactionConfirmed` event arrives for an account that the Ledger does not yet know about, the account is treated as **temporarily unknown** (not "definitely invalid"). The service throws a retryable `RuntimeException`, causing the Kafka consumer to redeliver the message according to the configured Spring Kafka consumer retry and backpressure policy. Once the `AccountCreated` event arrives and populates the local projection, the transaction will successfully process on the next retry.

If retries are exhausted, the message is routed to the Dead Letter Topic (DLT). Recovery from the DLT requires manual operational replay. Automatic recovery from DLT is **not** currently implemented.

## System Clearing Account (Temporary Workaround)

**Current Phase 05 Scope:** `TransactionConfirmed` events represent single-account intents. To fulfill the strict double-entry requirement, the Ledger automatically credits a `SYSTEM_CLEARING_ACCOUNT`:
```
UUID: 00000000-0000-0000-0000-000000000000
```
This is an explicit **temporary implementation workaround**. It is NOT a final domain model for multi-party settlement. Future phases will introduce an explicit multi-party transfer, clearing, or treasury domain definition to replace this workaround.

## Outbox Reliability and Kafka Semantics

The service uses an Outbox pattern for publishing `LedgerTransactionPosted` events:

1. `LedgerTransaction`, `Postings`, and `OutboxEvent` are committed atomically in one local PostgreSQL ACID transaction. If any of the three writes fails, the entire transaction rolls back.

2. The Outbox Relay (`OutboxRelay.java`), a `@Scheduled` polling process, claims PENDING outbox events and publishes them to Kafka. If the Kafka send succeeds, the event is marked PUBLISHED. If the send fails (e.g., Kafka unavailable), the event is reset to PENDING and retried on the next polling cycle.

3. **At-least-once publication:** If the relay publishes successfully but crashes before marking the event as PUBLISHED, the event will be republished on the next relay cycle (after the CLAIMED timeout window expires). Downstream consumer services must enforce their own inbox/idempotency checks to handle duplicate deliveries.

4. **Not exactly-once:** The system does NOT claim exactly-once Kafka publication. The guarantee is at-least-once publication with idempotent consumers.

## Event Key and Ordering
All Ledger outbox events for the same business transaction use `transactionId` as the Kafka message key (`OutboxRelay` sends with `aggregateId`). This provides **partition-scoped ordering** — all events for the same transaction are delivered to the same partition in order. **Global ordering across partitions is NOT guaranteed.**

## Correlation and Causation Chain
The Ledger preserves the full event lineage:
```
TransactionConfirmed (inbound)
  → eventId, transactionId, correlationId
    → LedgerTransactionPosted (outbound)
      → new eventId, same transactionId, same correlationId, causationId = inbound eventId
```
