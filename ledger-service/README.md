# ledger-service

> Immutable double-entry general ledger — the authoritative financial source of truth.

## Role in DFTP

The Ledger Service is the **final financial authority** in the platform. Every successful fund transfer culminates in an append-only, immutable double-entry journal entry where total debits strictly equal total credits. Account balances in `account-service` are operational projections; the ledger is the canonical accounting record.

Once a journal entry is committed, it cannot be modified or deleted — not by the application, not by raw SQL. Historical corrections are performed exclusively through compensating (reversal) transactions.

## Bounded Context

| Concern | Ownership |
| :--- | :--- |
| Double-entry journal entries | ✅ Authoritative (financial source of truth) |
| Posting immutability | ✅ Enforced at DB engine level |
| Account projections | Eventually consistent replica |
| Account balances | ❌ Operational state in `account-service` |
| Transfer intent | ❌ Owned by `transaction-service` |

## Accounting Invariant

For every committed ledger transaction:

```
∑ Debits − ∑ Credits ≡ 0.0000
```

This is enforced at **two independent levels**:

1. **Application:** `LedgerApplicationService.postTransaction()` validates debit/credit balance before writing.
2. **Database:** PostgreSQL `DEFERRABLE INITIALLY DEFERRED` constraint trigger `ensure_ledger_balance` fires at `COMMIT` time — rejecting the entire transaction if the posting group is unbalanced. The database is the final arbiter.

## Immutability

Enforced at **two independent levels**:

| Layer | Mechanism |
| :--- | :--- |
| Application | JPA fields annotated `updatable = false` |
| Database | `BEFORE UPDATE` and `BEFORE DELETE` triggers reject all mutations to `ledger_transactions` and `postings` |

## Domain Model

```java
LedgerTransaction {
    id: UUID                        // Internal PK
    ledgerTransactionId: String     // Ledger identity (UNIQUE, updatable=false)
    businessTransactionId: String   // Business tx identity (UNIQUE, updatable=false)
    status: String                  // POSTED
    createdAt: Instant              // updatable=false
}

Posting {
    id: UUID
    ledgerTransaction: LedgerTransaction  // FK (updatable=false)
    accountId: UUID                       // updatable=false
    amount: BigDecimal                    // updatable=false
    currency: String                      // updatable=false
    postingType: PostingType              // DEBIT | CREDIT (updatable=false)
    createdAt: Instant                    // updatable=false
}

PostingType { DEBIT, CREDIT }

AccountReference {
    id: UUID
    accountId: UUID                // Local projection from AccountCreated events
}
```

## API

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/ledger/transactions/{ledgerTransactionId}` | Retrieve by ledger ID |
| `GET` | `/ledger/transactions/by-transaction/{businessTransactionId}` | Retrieve by business tx ID |

No public write endpoints. Ledger mutations are **event-driven only**.

## Database

**Schema:** `ledger_db` (PostgreSQL 16, Flyway-managed)

| Table | Purpose |
| :--- | :--- |
| `ledger_transactions` | Journal entry headers with `UNIQUE(business_transaction_id)` |
| `postings` | Debit/credit line items with `amount > 0` constraint |
| `account_references` | Eventually consistent account projection |

**Triggers:**

| Trigger | Purpose |
| :--- | :--- |
| `ensure_ledger_balance` | Deferred constraint: `∑DEBIT = ∑CREDIT` per posting group at commit |
| `prevent_ledger_transaction_update` | Rejects `UPDATE` on `ledger_transactions` |
| `prevent_ledger_transaction_delete` | Rejects `DELETE` on `ledger_transactions` |
| `prevent_posting_update` | Rejects `UPDATE` on `postings` |
| `prevent_posting_delete` | Rejects `DELETE` on `postings` |

Shared tables from `common-infrastructure`: `outbox_events`, `inbox_messages`.

## Events

**Consumed** (topics: `transaction-events`, `account-events`):

| Event | Source | Action |
| :--- | :--- | :--- |
| `LedgerPostRequested` | transaction-service | Create double-entry journal posting |
| `AccountCreated` | account-service | Populate local account reference projection |

**Produced** (topic: `transaction-saga-events`):

| Event | Trigger |
| :--- | :--- |
| `LedgerTransactionPosted` | Journal entry committed successfully |
| `LedgerPostRejected` | Validation failed (amount ≤ 0, source = dest, etc.) |

## Idempotency Layers

1. **Transport (Inbox):** `eventId` deduplication via `inbox_messages` with `INSERT ... ON CONFLICT DO NOTHING`.
2. **Business (DB constraint):** `UNIQUE(business_transaction_id)` on `ledger_transactions`. A second event with a different `eventId` but the same `transactionId` catches `DataIntegrityViolationException` and treats it as idempotent success — preventing infinite Kafka retry loops.

**Critical distinction:**
```
eventId         = message identity     (transport deduplication via Inbox)
transactionId   = business identity    (financial deduplication via UNIQUE constraint)
```

## Account Projection & Eventual Consistency

The Ledger maintains a local projection of known accounts populated from `AccountCreated` events. If a `LedgerPostRequested` arrives for an unknown account:

- The service throws a **retryable** `RuntimeException`
- Kafka consumer retries with exponential backoff
- Once the `AccountCreated` event arrives, the posting succeeds on the next retry
- If retries are exhausted, the message is routed to the Dead Letter Topic (DLT)

## Event Lineage

The Ledger preserves the full causal chain:

```
LedgerPostRequested (inbound)
  → eventId, transactionId, correlationId
    → LedgerTransactionPosted (outbound)
      → new eventId
      → same transactionId
      → same correlationId
      → causationId = inbound eventId
```

## Test Suite

```
LedgerIntegrationTest                        — Full posting lifecycle, balance trigger, immutability
OutboxInboxReliabilityIntegrationTest        — Atomic outbox + inbox under failure
InboxIntegrationTest                         — Deduplication and concurrent delivery
OutboxIntegrationTest                        — Outbox claim lifecycle
KafkaRebalanceIntegrationTest                — Consumer group rebalance resilience
CiValidationTest                             — Build validation gate
```
