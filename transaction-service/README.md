# transaction-service

> Saga orchestrator, transaction lifecycle owner, reconciliation scanner, and batch settlement engine.

## Role in DFTP

The Transaction Service is the **Saga Orchestrator** — it owns the lifecycle of every distributed financial transfer across the platform. It does not own money; it owns _transaction intent and state progression_. Fund mutations are delegated to `account-service`, and accounting records to `ledger-service` via asynchronous Kafka events.

## Bounded Context

| Concern | Ownership |
| :--- | :--- |
| Transaction intent & identity | ✅ Authoritative |
| State machine progression | ✅ Authoritative |
| Account projections | Eventually consistent replica |
| Fund balances | ❌ Delegated to `account-service` |
| Accounting journal | ❌ Delegated to `ledger-service` |

## State Machine

```
PENDING ──→ SOURCE_HELD ──→ LEDGER_POSTED ──→ COMPLETED
   │              │
   ▼              ▼
 FAILED       COMPENSATING ──→ FAILED
```

`LEDGER_POSTED` is the **financial pivot point**. Once the ledger has committed the double-entry journal, no automatic Saga rollback is permitted. Post-pivot failures require explicit compensating transactions through reconciliation.

## API

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/transactions` | Create a transfer (requires `Idempotency-Key` header) |
| `GET` | `/transactions/{id}` | Retrieve by internal UUID |
| `GET` | `/transactions/by-transaction-id/{txId}` | Retrieve by business transaction ID |

**Port:** `8082`

## Domain Model

```java
Transaction {
    id: UUID                    // Internal PK
    transactionId: String       // Business identity (UNIQUE)
    ownerId: String             // Authenticated principal
    sourceAccountId: UUID       // Source account reference
    destinationAccountId: UUID  // Destination account reference
    amount: BigDecimal          // Transfer amount
    currency: String            // ISO currency code
    status: String              // State machine position
    version: Long               // Optimistic locking (@Version)
    createdAt: Instant
    updatedAt: Instant
}
```

## Database

**Schema:** `transaction_db` (PostgreSQL 16, Flyway-managed)

| Table | Purpose |
| :--- | :--- |
| `transactions` | Transaction entities with `UNIQUE(transaction_id)` and `@Version` |
| `account_references` | Eventually consistent projection of known accounts |
| `outbox_events` | Transactional Outbox (shared schema from `common-infrastructure`) |
| `inbox_messages` | Consumer deduplication (shared schema) |
| `shedlock` | Distributed job coordination |

## Events

**Consumed** (topics: `account-events`, `ledger-events`):

| Event | Source | Saga Transition |
| :--- | :--- | :--- |
| `FundsHeld` | account-service | PENDING → SOURCE_HELD |
| `FundsHoldRejected` | account-service | PENDING → FAILED |
| `LedgerTransactionPosted` | ledger-service | SOURCE_HELD → LEDGER_POSTED |
| `LedgerPostRejected` | ledger-service | SOURCE_HELD → COMPENSATING |
| `FundsSettled` | account-service | LEDGER_POSTED → COMPLETED |
| `HoldCompensated` | account-service | COMPENSATING → FAILED |
| `AccountCreated` | account-service | Updates local projection |

**Produced** (topic: `transaction-events`):

| Event | Trigger |
| :--- | :--- |
| `FundsHoldRequested` | Transaction created (PENDING) |
| `LedgerPostRequested` | Funds held (SOURCE_HELD) |
| `FundsSettlementRequested` | Ledger posted (LEDGER_POSTED) |
| `HoldCompensationRequested` | Ledger rejected (COMPENSATING) |

## Key Subsystems

| Package | Responsibility |
| :--- | :--- |
| `reconciliation/` | `ReconciliationScannerService` — cross-service drift detection (ShedLock-coordinated) |
| `settlement/` | Batch settlement engine with `SKIP LOCKED` claim |
| `backpressure/` | Kafka consumer backpressure management |
| `retention/` | `DataRetentionPurgeService` — bounded outbox/inbox cleanup |
| `security/` | JWT/OAuth2 integration, owner-scoped access control |

## Idempotency

- **HTTP:** `UNIQUE(transaction_id)` constraint prevents duplicate creation. Replay returns the existing transaction.
- **Events:** Inbox deduplication by `eventId` + state guards prevent stale/duplicate transitions.
- **Optimistic locking:** `@Version` prevents concurrent state overwrites.

## Test Suite

```
TransactionIntegrationTest             — CRUD, idempotency, account projection
TransactionSagaIntegrationTest         — Full Saga state transitions
TransactionAdversarialStateTest        — Illegal/out-of-order transitions
TransactionCompletionGuardTest         — Terminal state protection
TransactionCrashRecoveryTest           — Service restart mid-Saga
TransactionStaleEventProtectionTest    — Stale/replayed event rejection
load/ConcurrencyAndLoadSaturationIntegrationTest  — 50-thread burst, partition balance
lock/DistributedLockIntegrationTest    — ShedLock mutual exclusion proof
reconciliation/                        — Scanner correctness tests
settlement/                            — Batch claim + SKIP LOCKED tests
tracing/                               — W3C traceparent propagation
resilience/                            — Chaos scenarios
```
