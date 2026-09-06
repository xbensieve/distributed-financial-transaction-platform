# Common Infrastructure

This module acts as the shared technical foundation for all microservices in the Distributed Financial Transaction Platform.

## 🛑 STRICT ARCHITECTURAL BOUNDARY
To prevent distributed monolith coupling, this module **MUST NOT** contain any business domain logic.

**ALLOWED:**
- Technical Event Envelope (`EventEnvelope`)
- Outbox infrastructure (JPA Entities, Repositories, Polling Publishers)
- Inbox infrastructure (JPA Entities, Repositories)
- Technical Kafka configurations (Error Handlers, Deserializers)
- Technical error models (`ApiError`, `GlobalExceptionHandler`)
- Observability primitives (`CorrelationIdFilter`, Micrometer setup)

**STRICTLY FORBIDDEN:**
- Account domain entities or logic
- Transaction domain entities or logic
- Ledger domain entities or logic
- Business aggregates
- Financial business rules (e.g., balance calculations, fee structures)
- Shared persistence models between business domains

Any feature requiring business knowledge belongs in a specific service module (e.g., `account-service`), not here.
