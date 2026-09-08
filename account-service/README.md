# account-service

> Account aggregate, two-phase fund reservation (hold/settle/compensate), and balance integrity guardian.

## Role in DFTP

The Account Service owns **all account state and balance mutations** in the platform. It is the authoritative source for `settledBalance` and `heldFunds`. No other service may directly read or write the `account_db` database.

Within the Saga, this service acts as a **reactive participant** — it receives commands from the Saga orchestrator (`transaction-service`) and responds with outcome events, never initiating transfers itself.

## Bounded Context

| Concern | Ownership |
| :--- | :--- |
| Account lifecycle | ✅ Authoritative |
| Settled balance & held funds | ✅ Authoritative |
| Fund hold / settle / compensate | ✅ Authoritative |
| Transfer intent & state machine | ❌ Delegated to `transaction-service` |
| Accounting journal | ❌ Delegated to `ledger-service` |

## Balance Model

```
Available Balance = Settled Balance − Held Funds
```

| Field | Meaning |
| :--- | :--- |
| `settledBalance` | Financially finalized amount |
| `heldFunds` | Amount reserved by in-flight Saga(s) |
| Available | Amount currently spendable by a new hold |

**Database constraints** — enforced at the PostgreSQL engine level, independent of application logic:

```sql
CHECK (settled_balance >= 0)
CHECK (held_funds >= 0)
```

## Fund Operations

| Operation | Mutation | Trigger |
| :--- | :--- | :--- |
| **Hold** | `heldFunds += amount` | `FundsHoldRequested` from transaction-service |
| **Settle** | `source.heldFunds -= amount`, `source.settledBalance -= amount`, `dest.settledBalance += amount` | `FundsSettlementRequested` |
| **Compensate** | `heldFunds -= amount` | `HoldCompensationRequested` |
| **Credit** | `settledBalance += amount` | Direct API call (test/admin helper) |

Each operation is guarded by **business idempotency** via `AccountOperation` records keyed on `(transaction_id, operation_type, account_id)`.

## API

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/accounts` | Create account (requires `transactionId` for idempotency) |
| `GET` | `/accounts/{accountId}` | Retrieve account and balances |
| `POST` | `/accounts/{accountId}/credit?amount=...` | Credit account (test/admin) |

**Port:** `8081`

## Domain Model

```java
Account {
    id: UUID                   // Internal PK
    transactionId: String      // Business identity (UNIQUE)
    ownerId: String            // Authenticated principal or "system-unassigned"
    status: String             // ACTIVE
    settledBalance: BigDecimal // CHECK >= 0
    heldFunds: BigDecimal      // CHECK >= 0
    version: Long              // Optimistic locking (@Version)
    createdAt: Instant
}

AccountOperation {
    id: UUID
    accountId: UUID
    transactionId: String
    operationType: String      // HOLD | SETTLE | COMPENSATE
    createdAt: Instant
}
```

## Database

**Schema:** `account_db` (PostgreSQL 16, Flyway-managed)

| Migration | Purpose |
| :--- | :--- |
| `V1` | Initial `accounts` + `account_operations` tables |
| `V2` | Add `settled_balance` and `held_funds` columns |
| `V3` | Add `CHECK` constraints for non-negative balances |

Shared tables from `common-infrastructure`: `outbox_events`, `inbox_messages`.

## Events

**Consumed** (topics: `account-events`, `transaction-events`):

| Event | Source | Action |
| :--- | :--- | :--- |
| `FundsHoldRequested` | transaction-service | Reserve funds (hold) |
| `FundsSettlementRequested` | transaction-service | Settle source → destination |
| `HoldCompensationRequested` | transaction-service | Release held funds |
| `AccountCreated` | self (replay dedup) | Inbox-protected self-consumption |

**Produced** (topic: `account-events`):

| Event | Trigger |
| :--- | :--- |
| `AccountCreated` | Account created via API |
| `FundsHeld` | Hold succeeded |
| `FundsHoldRejected` | Insufficient available funds |
| `FundsSettled` | Settlement completed |
| `HoldCompensated` | Hold released (compensation) |

## Idempotency Layers

1. **HTTP (business intent):** `UNIQUE(transaction_id)` on `accounts`. Duplicate `POST /accounts` returns existing account.
2. **Event transport:** Inbox deduplication by `eventId` (`INSERT ... ON CONFLICT DO NOTHING`).
3. **Business operation:** `AccountOperation` keyed on `(transactionId, operationType, accountId)`. Duplicate hold/settle/compensate is a no-op.
4. **Optimistic locking:** `@Version` prevents concurrent balance overwrites.

## Security

- Account creation respects `SecurityContextHolder`:
  - `ROLE_ADMIN`: May specify explicit `ownerId`
  - `ROLE_USER`: `ownerId` derived from authenticated principal (client-supplied value ignored)
  - Unauthenticated: `ownerId = "system-unassigned"`

## Test Suite

```
AccountIntegrationTest                  — CRUD, idempotent creation
AccountSagaIntegrationTest              — Hold → Settle → Compensate full lifecycle
AccountDatabaseInvariantsTest           — CHECK constraint enforcement (overdraft prevention)
AccountCompensationIdempotencyTest      — Duplicate compensation safety
AccountSettlementIdempotencyTest        — Concurrent settlement race conditions
AccountSecurityTest                     — RBAC, owner scoping
AccountBolaSecurityTest                 — BOLA/IDOR access control validation
```
