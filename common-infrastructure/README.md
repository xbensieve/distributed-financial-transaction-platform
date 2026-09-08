# common-infrastructure

> Shared technical foundation for all DFTP microservices — event envelopes, outbox/inbox, Kafka configuration, observability, and security primitives.

## Architectural Boundary

This module provides **technical infrastructure only**. It must never contain business domain logic.

| ✅ Allowed | ❌ Forbidden |
| :--- | :--- |
| `EventEnvelope<T>`, event versioning | Account / Transaction / Ledger entities |
| Outbox entities, repository, relay, properties | Business aggregates |
| Inbox entities, repository | Financial rules (balance calculations, fees) |
| Kafka configuration, error handling, DLT | Domain-specific persistence models |
| W3C trace propagation, correlation filters | Cross-domain shared state |
| Global exception handling, API error model | Business validation logic |
| Distributed locking (ShedLock) configuration | |
| Security filters, rate limiting | |
| Micrometer metrics primitives | |

## Module Structure

```
com.dftp.common
├── event/              EventEnvelope<T>, event payload contracts, versioning
├── inbox/              InboxMessage, InboxMessageRepository (ON CONFLICT DO NOTHING)
├── outbox/             OutboxEvent, OutboxEventRepository (SKIP LOCKED), OutboxRelay, OutboxRelayProperties
├── kafka/              KafkaCommonConfig, DefaultErrorHandler, DLT, StringSerializer pipeline
├── observability/      TraceContextPropagator, CorrelationIdFilter, DftpMetrics
├── lock/               ShedLock configuration (usingDbTime)
├── security/           Security filters, authentication utilities
├── error/              ApiError, GlobalExceptionHandler
└── model/              Shared technical DTOs
```

## Key Components

### EventEnvelope\<T\>

Standardized event wrapper separating infrastructure metadata from business payload:

```java
EventEnvelope<T> {
    eventId: UUID            // Message identity (Inbox deduplication key)
    eventType: String        // Routing/dispatch identity
    eventVersion: String     // Schema evolution marker
    occurredAt: Instant      // Event timestamp
    correlationId: String    // End-to-end workflow correlation
    transactionId: String    // Business operation identity
    causationId: String      // Immediate preceding event ID
    producerService: String  // Origin service name
    payload: T               // Business data
}
```

`eventId ≠ transactionId` — this is a deliberate design decision ([ADR-008](../docs/decisions/ADR-008-event-envelope.md)).

### Transactional Outbox

Guarantees that events are published if and only if the local database transaction commits.

| Class | Responsibility |
| :--- | :--- |
| `OutboxEvent` | JPA entity with `traceparent`/`tracestate` columns for W3C trace propagation |
| `OutboxEventRepository` | `FOR UPDATE SKIP LOCKED` query for multi-instance polling |
| `OutboxRelay` | Scheduled relay: claim → publish to Kafka → mark `PUBLISHED` |
| `OutboxRelayProperties` | Configurable batch size and polling interval |
| `OutboxEventService` | Transactional claim logic with `REQUIRES_NEW` isolation |

**Claim query:**
```sql
SELECT * FROM outbox_events
WHERE status = 'PENDING'
   OR (status = 'CLAIMED' AND updated_at < (NOW() - INTERVAL '2 MINUTES'))
ORDER BY created_at ASC
LIMIT :batchSize
FOR UPDATE SKIP LOCKED
```

### Transactional Inbox

Consumer-side deduplication preventing duplicate financial effects from Kafka redeliveries.

| Class | Responsibility |
| :--- | :--- |
| `InboxMessage` | JPA entity implementing `Persistable<UUID>` (force insert semantics) |
| `InboxMessageRepository` | Atomic `INSERT ... ON CONFLICT (event_id) DO NOTHING` — returns 1 (new) or 0 (duplicate) |

### W3C Trace Propagation

`TraceContextPropagator` implements the full pipeline:

```
HTTP traceparent → outbox_events.traceparent column → Kafka RecordHeader → Consumer MDC
```

| Method | Purpose |
| :--- | :--- |
| `extractFromHttpRequest()` | Parse W3C `traceparent` at HTTP boundary |
| `injectIntoKafkaHeaders()` | Write `traceparent` as Kafka byte header |
| `extractFromKafkaHeaders()` | Read `traceparent` from Kafka consumer record |
| `populateMdc()` | Set SLF4J MDC keys (`traceparent`, `traceId`, `spanId`, `correlationId`) |
| `parseTraceparent()` | Validate W3C format; mint new context on malformed input |

### Kafka Configuration

`KafkaCommonConfig` provides:

- `DefaultErrorHandler` with exponential backoff (1s start, 10s max, 3 attempts)
- `DeadLetterPublishingRecoverer` for DLT routing
- Non-retryable classification: `IllegalArgumentException`, `JsonProcessingException`
- `ConcurrentKafkaListenerContainerFactory` with `AckMode.MANUAL`
- `StringSerializer` / `StringDeserializer` pipeline ([ADR-009](../docs/decisions/ADR-009-event-serialization.md))

## Flyway Migrations

**Technical schema `V0`** — shared across all services:

| Table | Purpose |
| :--- | :--- |
| `outbox_events` | Pending event queue with `status`, `traceparent`, `tracestate` |
| `inbox_messages` | Processed event deduplication with `UNIQUE(event_id)` |

## Test Suite

```
event/EventVersioningTest                     — Event version compatibility
kafka/DefaultErrorHandlerRetryTest            — Error handler configuration
kafka/SerializationRoundTripTest              — JSON roundtrip regression (Phase 06.10 incident)
observability/TraceContextPropagatorTest       — W3C traceparent parsing, malformed header handling
outbox/MultiInstanceOutboxRelayIntegrationTest — SKIP LOCKED concurrent worker proof
security/                                      — Security filter tests
```
