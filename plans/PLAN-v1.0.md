# UniShare Streamer — Development Plan v1.0

**Document:** `PLAN-v1.0.md`  
**Project:** UniShare Streamer  
**Release:** 1.0.0  
**Status:** Planned  
**Developer:** Rakesh Choudhary  
**Main Supervisor:** Axiom

---

## 1. Introduction

UniShare Streamer is an event-processing microservice within the UniShare ecosystem. It uses Apache Kafka and Spring Boot to consume, validate, dispatch, and process events published by other UniShare services.

Version 1.0 establishes the foundation for asynchronous event processing, centralized audit-event handling, domain-specific event routing, and reliable message consumption.

The objective is to build a functional, maintainable service that can be extended with additional event types and processing capabilities in future releases.

## 2. Objectives

Version 1.0 will focus on the following objectives:

- Establish a working Kafka consumer.
- Define a consistent event envelope for communication between services.
- Validate incoming events before processing.
- Implement type-based event dispatching.
- Introduce audit-event processing.
- Establish extensible processing contracts for rideshare and housing events.
- Implement safe message acknowledgment and offset management.
- Provide structured logging, error handling, and health monitoring.
- Create a testable foundation for future event processors.

## 3. Scope of Version 1.0

### 3.1 Core Features

| Feature | Priority | Expected outcome |
|---|---|---|
| Kafka integration | P0 | Connect to a configured Kafka broker and consume events |
| Event envelope | P0 | Standardized event structure |
| Event validation | P0 | Reject or isolate malformed events |
| Event dispatcher | P0 | Route supported event types to the correct handlers |
| Audit-event processor | P0 | Process and persist supported audit events |
| Error handling | P0 | Handle processing failures without silently losing messages |
| Offset management | P0 | Acknowledge messages according to successful processing |
| Configuration management | P0 | Support local and production profiles |
| Health monitoring | P1 | Expose application and dependency health |
| Rideshare event handler | P1 | Implement initial supported rideshare event types |
| Housing event handler | P1 | Implement initial supported housing event types |
| Automated tests | P0 | Verify validation, dispatching, and processing behavior |

**Priority definitions**

- **P0:** Required for the initial release.
- **P1:** Included in Version 1.0 if the core release requirements are satisfied.

### 3.2 Out of Scope

The following are not mandatory for the first release:

- Exactly-once processing across Kafka and an external database.
- Complex event orchestration and distributed sagas.
- Multi-region Kafka deployment.
- Advanced event replay and administrative tooling.
- A dedicated management dashboard.
- Sophisticated schema-registry integration.
- Processing every possible domain event in the UniShare ecosystem.

These capabilities can be evaluated for future versions.

## 4. High-Level Architecture

The intended Version 1.0 processing flow is:

```text
UniShare Services
       |
       | Publish events
       v
   Apache Kafka
       |
       | Consume messages
       v
 UniShare Streamer
       |
       v
 Event Validation
       |
       v
 Event Dispatcher
       |
       +------------------+
       |         |        |
       v         v        v
     Audit    Rideshare  Housing
    Handler    Handler   Handler
       |         |        |
       +---------+--------+
                 |
                 v
         Required Persistence
```

The audit handler is the primary domain processor for the initial release. Rideshare and housing handlers will support only the event types explicitly implemented and tested.

## 5. Standard Event Contract

All supported events should follow a common envelope so producers and consumers can communicate consistently.

### Proposed Event Structure

```json
{
  "eventId": "019a0000-0000-7000-8000-000000000001",
  "eventType": "AUDIT_EVENT",
  "eventVersion": 1,
  "source": "unishare-backend",
  "occurredAt": "2026-10-19T00:00:00Z",
  "correlationId": "request-correlation-id",
  "payload": {
    "action": "USER_UPDATED",
    "entityType": "USER",
    "entityId": "user-id",
    "actorId": "actor-id"
  }
}
```

The example is illustrative. The final schema must match the actual producer contracts and audit requirements.

### Event Contract Requirements

- `eventId`: Unique identifier used for deduplication.
- `eventType`: Identifies the event and its intended handler.
- `eventVersion`: Supports controlled schema evolution.
- `source`: Identifies the producing service.
- `occurredAt`: Records when the event occurred.
- `correlationId`: Connects related operations across services.
- `payload`: Contains event-specific information.

The consumer must validate required fields and supported event versions before dispatching a message.

## 6. Kafka Integration

### 6.1 Topic Strategy

Use domain-oriented topics to separate event categories.

| Topic | Purpose |
|---|---|
| `unishare.audit-events` | Audit and activity-related events |
| `unishare.rideshare-events` | Ridesharing lifecycle events |
| `unishare.housing-events` | Housing-post lifecycle events |
| `unishare.streamer.dlt` | Messages that cannot be processed successfully after the configured recovery attempts |

Topic names are proposed defaults and must be aligned with the existing UniShare producer services before implementation.

### 6.2 Consumer Configuration

The service should support configurable:

- Kafka bootstrap servers.
- Consumer group ID.
- Topic names.
- Key and value deserializers.
- Maximum poll records.
- Poll interval and session settings.
- Offset reset behavior.
- Retry and recovery settings.
- Security and TLS settings when required by the environment.

Environment-specific configuration must remain outside source-controlled secrets.

### 6.3 Reliable Message Processing

Version 1.0 should follow these principles:

1. Validate the event before invoking domain logic.
2. Process the event successfully before committing its offset.
3. Retry transient failures using bounded retries and backoff.
4. Route messages that exhaust recovery attempts to a dead-letter topic where configured.
5. Preserve enough original message and error metadata for investigation.
6. Prevent duplicate side effects using idempotent processing.

Kafka provides at-least-once delivery behavior when consumers reprocess uncommitted messages. The implementation must therefore tolerate duplicate deliveries.

For database-backed handlers, offset commits and database transactions must be coordinated carefully. A crash after a database write but before an offset commit can cause the same event to be processed again.

### 6.4 Efficient Consumption

- Use consumer groups for scalable consumption.
- Select partition keys according to required ordering guarantees.
- Begin with conservative batch sizes and concurrency.
- Tune throughput using measured consumer lag and processing latency.
- Avoid blocking consumer threads with unnecessary long-running work.
- Monitor failed messages, retries, and processing duration.

Higher concurrency must not violate domain ordering or database consistency requirements.

## 7. Event Processing Components

### 7.1 Event Validator

Responsibilities:

- Validate the event envelope.
- Check mandatory fields.
- Verify supported event types and versions.
- Reject malformed or unsupported messages through the configured error-handling path.
- Produce actionable error logs without exposing sensitive data.

### 7.2 Event Dispatcher

Responsibilities:

- Identify the correct handler using `eventType`.
- Keep dispatching separate from domain business logic.
- Reject unknown event types safely.
- Allow new handlers to be registered without rewriting the consumer.

### 7.3 Audit Event Processor

Responsibilities:

- Process supported audit events.
- Preserve actor, action, resource, timestamp, and correlation metadata where applicable.
- Persist audit records using the approved storage model.
- Avoid duplicate audit records using a stable event identifier or equivalent idempotency mechanism.
- Keep audit processing independent of rideshare and housing business logic.

Audit records should not contain passwords, authentication tokens, or unnecessary sensitive payload data.

### 7.4 Rideshare Event Processor

The initial supported event types should be agreed with the rideshare producer service.

Potential events include:

- Ride created.
- Ride updated.
- Ride cancelled.

Each implemented event must have a defined payload contract and corresponding tests.

### 7.5 Housing Event Processor

The initial supported event types should be agreed with the housing producer service.

Potential events include:

- Housing post created.
- Housing post updated.
- Housing post published.
- Housing post removed.

Unsupported events must not be treated as successfully processed merely because the consumer received them.

## 8. Persistence and Idempotency

The persistence model must be agreed before implementing database-backed processors.

For audit processing, Version 1.0 should provide:

- A persistent audit record model.
- A stable reference to the originating event.
- Appropriate timestamps and correlation metadata.
- A uniqueness constraint or equivalent mechanism to prevent duplicate records.
- Transactional handling of audit writes.

PostgreSQL is the proposed persistence technology where a dedicated persistence layer is required.

The service must not assume that Kafka offset commits and PostgreSQL transactions form a single atomic transaction.

## 9. Configuration and Environments

The service should support the existing configuration profiles:

- `local` — Local development.
- `prod` — Production deployment.

Configuration should cover the application port, Kafka connection, consumer group, topic names, persistence settings, and monitoring.

Sensitive configuration must be supplied through environment variables or a suitable secret-management mechanism.

Local configuration files containing secrets must remain excluded from version control.

## 10. Logging and Monitoring

Version 1.0 should provide sufficient operational visibility to diagnose event-processing problems.

### Logging

Log relevant lifecycle information:

- Event received.
- Event validation failure.
- Handler selected.
- Processing success or failure.
- Retry attempts.
- Dead-letter routing.
- Processing duration.

Use event IDs and correlation IDs to connect related log entries. Avoid logging complete sensitive payloads.

### Health and Metrics

Use Spring Boot Actuator for health endpoints and expose suitable metrics where configured.

Important measurements include:

- Consumer processing throughput.
- Processing latency.
- Failed event count.
- Retry count.
- Dead-letter count.
- Kafka consumer lag.
- Database errors and connection health.

A successful application health check must not be interpreted as proof that every event is being processed correctly.

## 11. Testing Strategy

### Unit Tests

Test:

- Event envelope validation.
- Required-field validation.
- Supported and unsupported event types.
- Dispatcher-to-handler routing.
- Audit-event processing.
- Idempotency behavior.
- Retryable and non-retryable errors.

### Integration Tests

Verify:

- Kafka consumer configuration.
- Message serialization and deserialization.
- Successful consumption and processing.
- Offset behavior following processing failures.
- Dead-letter handling.
- Database persistence and duplicate-event handling.

Use an isolated test environment, such as Testcontainers, when appropriate.

### Acceptance Criteria

Version 1.0 is ready for release when:

- [ ] The application starts with the intended profile and configuration.
- [ ] The consumer connects to the configured Kafka broker.
- [ ] Valid events reach their intended handlers.
- [ ] Invalid events follow a defined failure path.
- [ ] Audit events are persisted correctly.
- [ ] Duplicate deliveries do not create duplicate audit records.
- [ ] Failed processing does not result in premature acknowledgment.
- [ ] Retry and dead-letter behavior is tested where configured.
- [ ] Health checks and useful logs are available.
- [ ] Automated tests pass.
- [ ] No credentials or secrets are committed to the repository.
- [ ] Setup and operational requirements are documented.

## 12. Implementation Milestones

### Milestone 1 — Foundation

- Finalize event envelope and topic names.
- Configure Kafka connectivity.
- Configure local and production profiles.
- Verify application startup and health endpoints.

### Milestone 2 — Event Consumption

- Implement the Kafka consumer.
- Add event deserialization and validation.
- Implement event dispatching.
- Add consumer and dispatcher tests.

### Milestone 3 — Audit Processing

- Finalize the audit-event contract.
- Implement audit processing and persistence.
- Add idempotency protection.
- Verify transaction and offset behavior.

### Milestone 4 — Reliability

- Configure bounded retries.
- Implement dead-letter handling.
- Add structured logs and relevant metrics.
- Test failures, duplicate delivery, and recovery.

### Milestone 5 — Domain Extensions

- Integrate the first agreed rideshare event types.
- Integrate the first agreed housing event types.
- Add tests for each implemented event handler.

### Milestone 6 — Release Preparation

- Run the complete test suite.
- Validate configuration and deployment requirements.
- Update the README and operational documentation.
- Verify the Version 1.0 acceptance criteria.
- Tag the release as `v1.0.0` when approved.

## 13. Future Enhancements

Potential post-v1.0 improvements include:

- Schema Registry integration.
- More advanced event replay tooling.
- Improved distributed tracing.
- Advanced consumer scaling and backpressure controls.
- Additional domain processors.
- Enhanced operational dashboards.
- Stronger transactional workflows where business requirements justify them.

These features should be prioritized based on actual workload, reliability requirements, and operational needs.

## 14. Development Ownership

| Role | Name | Responsibility |
|---|---|---|
| Developer | Rakesh Choudhary | Implementation, testing, debugging, and maintenance |
| Main Supervisor | Axiom | Technical supervision, architectural decisions, code review, and development direction |

Development should follow small, reviewable changes with tests and documentation for every significant processing capability.

## 15. Definition of Done

A feature is complete only when:

1. Its event contract and expected behavior are documented.
2. Its implementation is complete.
3. Success and failure cases are tested.
4. Retry, acknowledgment, and idempotency implications are understood.
5. Logs and metrics provide adequate operational visibility.
6. Configuration and security requirements are satisfied.
7. The implementation has been reviewed and accepted.

---

**Release goal:** Deliver a reliable and extensible Kafka event-processing foundation for UniShare, with audit processing as the first core capability and a clear path toward rideshare, housing, and future event types.

**Document version:** 1.0.0  
**Status:** Planned — implementation and acceptance criteria must be verified before release.