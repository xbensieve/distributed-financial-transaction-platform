# Account Service (Phase 03)

The Account Service is the first true business service implemented on the `common-infrastructure`.
It demonstrates the paved-road patterns for a distributed system, including the Outbox pattern, Inbox deduplication, Kafka event streaming, and Database idempotency.

## Responsibilities
- Manages the lifecycle of Accounts.
- Provides REST APIs to create and retrieve accounts.
- Emits domain events (`AccountCreated`) via Outbox when accounts are created.
- Consumes events idempotently via Inbox.

## API Endpoints
- `POST /accounts`: Creates a new account. Requires a `CreateAccountRequest` payload containing a unique `transactionId`.
- `GET /accounts/{accountId}`: Retrieves an existing account by its UUID.

## Database Schema
- Uses PostgreSQL.
- Flyway manages the schema (`db/migration`).
- `accounts`: Stores core account data and enforces a unique `transactionId` constraint.
- `outbox_events`: Part of `common-infrastructure`. Stores pending events to be published.
- `inbox_messages`: Part of `common-infrastructure`. Tracks processed event IDs to prevent duplicate processing.

## Events Produced
- `AccountCreated` (Topic: `account-events`)
    - Emitted when an account is successfully created.
    - Wrapped in `EventEnvelope` with unique `eventId` and `transactionId`.

## Events Consumed
- `AccountCreated` (Topic: `account-events`)
    - Consumed by `AccountEventConsumer`.
    - Protected against duplicate delivery via `InboxMessageRepository`.

## Idempotency Policy
- **HTTP Idempotency (Business Intent):** Creating an account with a previously seen `transactionId` will NOT create a duplicate account and will NOT emit a duplicate event. It will return the existing account data (HTTP 201 Created/200 OK semantics). This is enforced by a `UNIQUE` constraint in the database on `transactionId`.
- **Event Consumption Idempotency:** The consumer checks the `inbox_messages` table for the `eventId`. If it exists, the event is skipped and ACKed to Kafka.

## Local Execution
To run locally:
1. Ensure PostgreSQL and Kafka are running.
2. Build and run: `mvn spring-boot:run`
