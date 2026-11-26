# Design Document: Common Kafka Module

## Overview

The `kafka` module provides a thin, explicit wrapper over IBM Sarama (`v1.43.3`) using `log/slog`, opt-in Prometheus metrics, and opt-in OpenTelemetry tracing. It keeps topic routing, group selection, and business handlers in the application while enforcing safe offset-acknowledgement, retry/DLQ, configuration validation, and shutdown invariants.

## Ownership & Architecture

- **`BrokerConfig`**: Cluster endpoints (`host:port`), `ClientID`, SASL/TLS authentication, and socket `DialTimeout` / `ReadTimeout` / `WriteTimeout` (30s defaults).
- **`ProducerConfig`**: `RequiredAcks`, `Idempotent`, `Compression`, `MaxRetries`, `RetryBackoff`, `MaxMessageBytes`, `Timeout`. Produces via `Producer` (`NewProducer`).
- **`ConsumerConfig`**: `GroupID`, `InitialOffset` (`oldest`/`newest`), `BalanceStrategy` (`roundrobin`/`sticky`/`range`), `SessionTimeout`, `HeartbeatInterval`, `RebalanceTimeout`, `MaxProcessingTime` (Sarama delivery channel backpressure, not handler deadline), `CommitMode` (`periodic`/`sync`), `CommitInterval`, `HandlerTimeout` (per-attempt deadline), and `ShutdownTimeout` (drain budget).
- **`Consumer` (`NewConsumer`)**: Owns exactly one `sarama.ConsumerGroup` per `Run`, serving all pre-registered `Subscription` entries (`Topic`, `Handler`, `HandlerTimeout`, `RetryPolicy`, `Interceptors`). Multiple `Consumer` instances may coexist per process for hybrid group isolation.

## Delivery & Failure Model

1. **Sequential within a partition, concurrent across partitions**: `ConsumeClaim` processes messages synchronously in partition offset order without spawning detached per-message goroutines. Source `Message` slices and metadata must be treated as read-only; for DLQ-enabled subscriptions, an immutable snapshot is captured before any interceptor or handler runs.
2. **Explicit dispositions**: `processMessage` returns `outcomeProcessed`, `outcomeDeadLettered`, `outcomeDiscarded`, `outcomeInterrupted`, or `outcomeTerminalFailure`. Offsets are marked only for `outcomeProcessed`, `outcomeDeadLettered`, or `outcomeDiscarded` when `session.Context().Err() == nil`.
3. **Cancellation precedence**: Session cancellation (rebalance or shutdown) interrupts processing immediately and leaves unresolved offsets unmarked.
4. **Retries & DLQ**:
   - Bounded retries run up to `1 + MaxRetries` attempts with capped exponential backoff and full positive-floor jitter (`[delay/2, delay]`).
   - `RetryStrategyDLQ` publishes the pre-handler snapshot with `x-original-*` metadata headers (`x-original-error` holds a safe failure classification). When DLQ publication fails, `DLQFailureDiscard` marks and continues, `DLQFailureRetry` retries only the DLQ write with backoff, and `DLQFailureStop` cancels the consumer run with a terminal error without marking the failed offset.
5. **No exactly-once claims**: Kafka commit and handler/DLQ side effects are separate operations; duplicates are possible on crashes or rebalances, so handlers must be idempotent.

## Lifecycle & Graceful Shutdown

- `Subscribe` registers and validates immutable subscription descriptors without broker I/O.
- `Run` is single-shot: it transitions `Registered -> Starting`, creates one `sarama.ConsumerGroup` (failing fast and returning the wrapped error with `State=failed`, `Ready=false` if client creation fails—without an internal startup retry loop), drains `Errors()` continuously until client close, and runs `Consume` with jittered reconnect backoff for transient post-startup errors and immediate termination for unrecoverable errors (auth, config, unexpected closed group).
- **Initial-offset replay caveats**: `InitialOffset` defaults to `"newest"`. When an unresolved message is left unmarked and the group has no valid committed offset yet (or offsets expired past retention and reset), restarting with `"newest"` skips earlier unmarked records; critical streams requiring replay must set `InitialOffset = "oldest"`. Existing valid committed offsets remain authoritative.
- On `Close()` or parent context cancellation, the `admissionGate` closes as the atomic cutoff before stop notifications, readiness updates, or logging, allowing already-admitted in-flight messages to finish within `ShutdownTimeout`. Idle `ConsumeClaim` goroutines wait on `session.Context().Done()` rather than returning early and prematurely aborting active partitions in the same Sarama session.
- After the drain completes or expires, the internal group context is cancelled, `Cleanup` runs (performing a best-effort `session.Commit()` in `CommitSync` mode), the Sarama client is closed, and all owned goroutines are joined.

## Observability & Driver Limitations

- **Safe diagnostics**: Library logs, `Health().LastError`, trace error events/status, and `x-original-error` emit bounded failure classifications rather than raw panic values or arbitrary handler errors; wrapped errors are preserved for callers (`errors.Is`/`errors.As`), and handlers/DLQ ACLs remain responsible for payload/application-log sensitivity.
- **Metrics**: Registered explicitly on a caller-supplied `prometheus.Registerer` (`NewMetrics`), assuming at most one `Consumer` member per `group_id` per process for `kafka_consumer_ready`. Lag ownership is guarded atomically under `lagMu`. Sarama `session.Commit()` returns no error; `kafka_commit_errors_total` counts only positively identified commit errors from `Errors()`. `kafka_consumer_lag` reflects in-process observed claim backlog (`HighWaterMarkOffset - nextOffset`) and is cleaned up per claim owner on `Cleanup`.
- **Tracing**: Configured explicitly via `TracingConfig` (`TracerProvider` + `Propagator`), injecting/extracting W3C trace context on copied headers without touching globals.
- **Synchronous Producer**: Context cancellation is checked before JSON marshaling and before `SendMessage`, but in-flight synchronous Sarama sends cannot be aborted mid-call. Nil keys remain nil `sarama.Encoder` interfaces for Sarama random partitioning.
