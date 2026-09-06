# Transaction Service

The **Transaction Service** is the second business domain service on the Distributed Financial Transaction Platform. It establishes the domain for business transactions and coordinates with the Account Service via asynchronous messaging.

## Responsibilities
- **Transaction Intent**: Captures and validates requests to execute a transaction.
- **Transaction Identity**: Owns the unique `transactionId` business identifier for idempotency.
- **Transaction State**: Manages the state machine (PENDING, CONFIRMED, REJECTED).
- **Eventual Consistency**: Listens to Account events to maintain a local, eventually consistent projection of valid accounts.

## State Machine
The simplified state machine for Phase 04:
```text
PENDING -> CONFIRMED (if Account is valid in projection)
PENDING -> REJECTED  (if Account validation fails at business layer later, currently we reject synchronously at POST if missing)
```
*Note: We transition to CONFIRMED directly if the account is in the projection. True multi-step transitions will occur in later phases (e.g. Ledger).*

## API
- `POST /transactions`: Creates a transaction intent.
- `GET /transactions/{id}`: Retrieves transaction state.

## Database Ownership
The Transaction Service completely owns the `dftp_transactions` PostgreSQL database. It never accesses the `account-service` database.

## Events
**Consumed**:
- `AccountCreated` (from `account-events` topic). Used to populate the `account_references` local projection.

**Produced**:
- `TransactionConfirmed` (to `transaction-events` topic). Emitted atomically alongside the database transaction commit using the Outbox pattern.

## Idempotency Policy
Duplicate HTTP POSTs specifying the exact same `transactionId` will not result in a new database entry and will not emit duplicate business events. The service will safely return the original transaction state.

## Failure Behavior & Local Execution
- **HTTP Timeout / Client Retry**: Safe due to `transactionId` idempotency.
- **Kafka Unavailability**: Business transaction commits successfully; Outbox Relay will infinitely retry event publication.
- **Event Delay (Account)**: If an account is created but its event is delayed, the Transaction Service will reject transactions for it until the local projection is updated via Kafka.
