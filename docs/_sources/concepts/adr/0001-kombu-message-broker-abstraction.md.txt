# ADR-0001: Kombu-Based Message Broker Abstraction

## Status

Accepted

## Context

The pipeline stages (data monitor → job builder → dispatcher) need to pass messages
between plugins. The communication layer must be swappable so that development and
testing can use an in-memory transport while production uses RabbitMQ (or another AMQP
broker).

## Decision

Use [Kombu](https://docs.celeryq.dev/projects/kombu/) as the message transport
abstraction. Kombu supports multiple backends (AMQP, Redis, in-memory, SQS, etc.) behind
a single API, which allows the integration test suite to run with `memory://` while
production deployments target a real RabbitMQ instance.

A `MessageBrokerManager` wraps the Kombu connection pool and exposes `emit()` and
`consume()` through the `Service` facade so that plugins never hold a broker reference
directly.

## Alternatives Considered

- **Direct pika/aio-pika**: Low-level AMQP client. Simpler dependency graph, but no
  in-memory backend; all tests would require a running broker.
- **Celery**: Higher-level task queue. Adds significant complexity and opinions about
  task serialization that conflict with the existing `Job` domain type.

## Trade-offs Accepted

- Kombu is untyped (`py.typed` marker absent) — all imports carry `# type: ignore[import-untyped]`.
- Connection-error handling and retry logic must be implemented manually
  (`rabbit_mq_watcher.py` implements exponential backoff on `OperationalError`).

## Consequences

- **Dependency**: All plugins transitively depend on Kombu through the
  `MessageBrokerManager` facade. Plugin authors never import Kombu directly, keeping
  the broker abstraction sealed behind the `Service` layer.
- **Testability**: The `memory://` transport enables the full integration test suite to
  run without a broker daemon. CI pipelines require no external services for the core
  test matrix.
- **Type safety**: Kombu's lack of a `py.typed` marker means every Kombu import carries
  `# type: ignore[import-untyped]`. This is a known, accepted limitation on type safety
  within the broker module.
- **Resilience**: Connection-error handling and exponential-backoff retry logic live in
  `rabbit_mq_watcher.py` rather than being provided by the library. This gives the team
  full control over reconnection policy but must be maintained as Kombu evolves.
