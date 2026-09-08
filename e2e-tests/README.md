# e2e-tests

> Multi-service integration tests exercising the full distributed Saga across real PostgreSQL and Kafka instances via Testcontainers.

## Purpose

This module validates that the three bounded contexts (`transaction-service`, `account-service`, `ledger-service`) interact correctly as a distributed system — not just as individual units. Every test boots **three independent Spring application contexts** communicating through real Kafka topics and real PostgreSQL databases. **Zero mocks** are used at integration boundaries.

## Infrastructure

| Component | Implementation |
| :--- | :--- |
| PostgreSQL | Testcontainers `postgres:16-alpine` |
| Kafka | Testcontainers `confluentinc/cp-kafka:7.6.1` |
| Spring contexts | 3 independent application contexts (one per service) |
| Event transport | Real Kafka production/consumption with Outbox relay |
| Deduplication | Real Inbox with `INSERT ... ON CONFLICT DO NOTHING` |

```java
@Testcontainers(disabledWithoutDocker = true)
```

Tests gracefully skip if Docker is unavailable.

## Test Suite: `EndToEndSagaIntegrationTest`

| Test ID | Scenario | What It Proves |
| :--- | :--- | :--- |
| **E2E-01** | Happy path transfer | Full Saga: PENDING → SOURCE_HELD → LEDGER_POSTED → COMPLETED. Source debited, destination credited, ledger balanced. |
| **E2E-02** | Insufficient funds | Hold rejected → transaction FAILED. Zero fund mutation. Held funds = 0. |
| **E2E-03** | Ledger rejection + compensation | Ledger rejects (source = dest). Saga compensates: hold released, transaction FAILED. |
| **E2E-04** | Transaction service restart after hold | Service killed after SOURCE_HELD. On restart, Saga resumes and completes. |
| **E2E-05** | Transaction service restart after ledger post | Service killed after LEDGER_POSTED. On restart, settlement completes. |
| **E2E-06** | DLT routing on poison message | Malformed event → routed to Dead Letter Topic. No business mutation. |
| **E2E-07** | Atomic Inbox concurrent deduplication | 10 concurrent identical events → exactly 1 processed, 9 deduplicated. |
| **E2E-08** | Outbox recovery lifecycle | Outbox event transitions: PENDING → CLAIMED → PUBLISHED. Crash recovery reclaims stale claims. |
| **E2E-09** | Event contract verification | All 11 active event types observed on Kafka topics with correct metadata. |

## Running

```bash
# Full E2E suite (requires Docker)
mvn test -pl e2e-tests -am

# Or via the reactor
mvn clean verify
```

## Important Notes

- **E2E-04 / E2E-05** prove recovery safety and replay correctness. Due to timing uncertainty in `@BeforeEach` shutdown, they prove that recovery _works_ — not that they trap a precisely targeted crash boundary.
- **E2E-07** is one of the strongest proofs in the entire project: 10 concurrent threads, 1 insert, 9 duplicates — atomic Inbox deduplication under real concurrency.
- **E2E-09** currently validates event _names_ on topics. Full per-event contract schema validation (all metadata fields, topic, key) is a future enhancement.
