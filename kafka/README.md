# Kafka Module

Common Go Kafka module built on IBM Sarama (`v1.43.3`) and `log/slog`, separating cluster connectivity (`BrokerConfig`) from producer (`ProducerConfig`), consumer group (`ConsumerConfig`), and per-topic `Subscription` configuration.

## Installation

```bash
go get github.com/routerarchitects/ra-common-mods/kafka@v0.2.0
```

## Quick Start

### Producer (`NewProducer`)

Use one `Producer` instance per `(BrokerConfig, ProducerConfig)`. `Publish`, `PublishWithHeaders`, and `PublishJSON` are synchronous and concurrency-safe across topics.

```go
brokerCfg := kafka.DefaultBrokerConfig()
brokerCfg.Brokers = []string{"localhost:9092"}
brokerCfg.ClientID = "orders-service"

producerCfg := kafka.DefaultProducerConfig()
prod, err := kafka.NewProducer(brokerCfg, producerCfg)
if err != nil {
	log.Fatal(err)
}
defer prod.Close()

if err := prod.PublishJSON(ctx, "orders.created", "order-123", map[string]any{"id": "order-123"}); err != nil {
	log.Fatal(err)
}
```

### Single-Topic & Multi-Topic Consumer (`NewConsumer`)

A `Consumer` is one consumer group member (identified by `ConsumerConfig.GroupID`). Topic names are distinct from group IDs: register one or more topic subscriptions via `Subscribe`, then start the group once with `Run`.

```go
brokerCfg := kafka.DefaultBrokerConfig()
brokerCfg.Brokers = []string{"localhost:9092"}
brokerCfg.ClientID = "orders-worker"

consumerCfg := kafka.DefaultConsumerConfig()
consumerCfg.GroupID = "orders-worker-group" // distinct from topic names
consumerCfg.InitialOffset = "oldest"        // deliberate choice for critical streams

consumer, err := kafka.NewConsumer(brokerCfg, consumerCfg, logger)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

if err := consumer.Subscribe(kafka.Subscription{
	Topic:       "orders.created",
	Handler:     handleOrderCreated,
	RetryPolicy: kafka.DefaultRetryPolicy(),
}); err != nil {
	log.Fatal(err)
}

if err := consumer.Subscribe(kafka.Subscription{
	Topic:          "orders.audit",
	Handler:        handleOrderAudit,
	HandlerTimeout: 10 * time.Second,
	RetryPolicy: &kafka.RetryPolicy{
		Strategy:         kafka.RetryStrategyDLQ,
		MaxRetries:       3,
		InitialDelay:     100 * time.Millisecond,
		MaxDelay:         5 * time.Second,
		Multiplier:       2.0,
		DLQTopic:         "orders.audit.dlq",
		DLQProducer:      dlqProducer,
		DLQFailurePolicy: kafka.DLQFailureStop,
	},
}); err != nil {
	log.Fatal(err)
}

if err := consumer.Run(ctx); err != nil {
	log.Fatal(err)
}
```

### Hybrid Multi-Group Deployment

When topics require isolated group settings, rebalance lifecycles, or failure domains, construct multiple `Consumer` instances sharing the same `BrokerConfig` (each creates its own Sarama group client; never share a `sarama.Client` across consumer groups). Keep shared dependencies (DB pools, logger, shared DLQ `Producer`) open until all consumers have stopped.

See [examples/hybrid/main.go](examples/hybrid/main.go) for a complete runnable hybrid example.

## Delivery, Retry & DLQ Semantics

- **Sequential per partition, concurrent across partitions**: Handlers registered on a `Consumer` must be thread-safe across partitions, idempotent across replays, and treat source `Message` slices/metadata (`Key`, `Value`, `Headers`, `Topic`, `Partition`, `Offset`, `Timestamp`) as read-only. For DLQ-enabled subscriptions, the library captures an immutable snapshot of the source record before invoking interceptors or handlers.
- **Explicit dispositions**: A message offset is marked only when the handler returns `nil` before session/attempt expiry, a DLQ write is acknowledged, or an applicable discard policy (`ignore` after retries, or `dlq` with `DLQFailureDiscard`) triggers while the session is active.
- **Unresolved failures, startup contract & initial-offset replay**:
  - Session cancellations (shutdown or rebalance) interrupt processing without marking the unresolved record. Under `DLQFailureStop`, the unresolved offset is left unmarked, later offsets in the partition are not processed, and `Run` returns the terminal error.
  - **Startup failure contract**: `Run` is single-shot. If initial Sarama consumer-group creation fails, `Run` fails fast—setting `State=failed`, `Ready=false`, cleaning up, and returning the wrapped error—without an internal startup retry loop. The application/supervisor is responsible for recreating the consumer or restarting the pod. (By contrast, transient errors *after* client creation during `Consume` are retried with jittered backoff.)
  - **Initial-offset replay limitations**: `InitialOffset` defaults to `"newest"` when no valid committed offset exists. If an unresolved record is left unmarked and the consumer group has **no valid committed offset** yet (or a previously committed offset has expired past broker retention and Sarama resets), restarting with `"newest"` skips earlier unmarked records and resumes from log end. Configure `InitialOffset = "oldest"` deliberately for critical streams that must replay unconsumed records when no committed offset exists; existing valid committed offsets always remain authoritative.
- **Retry policies**:
  - `RetryStrategyLogAndIgnore` (`ignore`): retries up to `1 + MaxRetries` attempts (`MaxRetries=0` executes once), then logs, records discard, and marks the offset. Intentionally lossy.
  - `RetryStrategyDLQ` (`dlq`): retries up to `1 + MaxRetries` attempts, then publishes the captured original snapshot to `DLQTopic` preserving key, payload, nil-vs-empty slices, and non-reserved headers while replacing `x-original-*` metadata headers (`x-original-topic`, `x-original-partition`, `x-original-offset`, `x-original-timestamp`, `x-original-consumer-group`, `x-original-error`, `x-original-attempts`). `x-original-error` records a safe failure classification (e.g. `handler_error`, `handler_timeout`, `handler_panic`), while DLQ payload protection/ACLs remain the application's responsibility. Requires explicit `DLQFailurePolicy`:
    - `DLQFailureDiscard`: logs/meters discard and marks the source offset.
    - `DLQFailureRetry`: retries only the DLQ write with jittered backoff until acknowledged or session cancellation.
    - `DLQFailureStop`: leaves the source offset unmarked and stops `Run` with an error.
  - `RetryStrategyInfinite` (`infinite`): retries with capped jittered exponential backoff until success or session cancellation.
- **Timeouts & Shutdown**:
  - `ConsumerConfig.MaxProcessingTime` is Sarama's message-channel backpressure setting, not a handler deadline.
  - `ConsumerConfig.HandlerTimeout` / `Subscription.HandlerTimeout` applies a cooperative `context.WithTimeout` per attempt. Handlers must respect `ctx.Done()`.
  - `ConsumerConfig.ShutdownTimeout` sets the graceful drain budget on `Close()` or parent context cancellation. Unresolved handlers exceeding the budget are cancelled and left unmarked.
  - Never call `Close()` from inside a handler running on the same consumer instance; cancel the parent context instead.
- **Synchronous Producer**: `Publish` checks `ctx.Err()` before marshaling/sending, coordinates safely with `Close()`, and cannot cancel an already in-flight Sarama synchronous request.

## Observability (Opt-In)

- **Health**: `consumer.Health()` returns a concurrency-safe `ConsumerHealth` snapshot (`State`, `Ready`, `AssignedClaims`, `LastError`, timestamps). `Ready` is `true` after session `Setup` (even with 0 claims) and `false` during startup, rebalance, drain, or terminal failure.
- **Safe Diagnostics**: Library logs, `Health().LastError`, tracing error events/status, and `x-original-error` emit bounded failure classifications rather than raw panic values or arbitrary handler error strings. Programmatic callers still receive original wrapped errors from `Run`/`Publish` for `errors.Is`/`errors.As`; application error messages and caller-side logging must avoid embedding secrets.
- **Metrics**: Call `kafka.NewMetrics(reg)` with a caller-supplied `prometheus.Registerer` (no global registration) and pass `kafka.WithProducerMetrics(m)` / `kafka.WithConsumerMetrics(m)`.
  - *Instrumentation model*: Run at most one `Consumer` member per `group_id` per process when sharing `Metrics`, as `kafka_consumer_ready` is labelled by `group_id`.
  - *Commit error caveat*: Sarama `v1.43.3`'s `session.Commit()` returns no error; `kafka_commit_errors_total` counts only positively identified commit errors from `Errors()` (`ErrOffsetMetadataTooLarge`, `ErrInvalidCommitOffsetSize`, `ErrUnstableOffsetCommit`).
  - *Lag caveat*: `kafka_consumer_lag` is the observed in-process claim backlog (`HighWaterMarkOffset - nextOffset`), not broker-committed lag across offline partitions.
- **Tracing**: Pass `kafka.TracingConfig{TracerProvider: tp, Propagator: prop}` via `WithProducerTracing` / `WithConsumerTracing` (no global provider installation) or `NewTracingInterceptor(cfg)`.

## Upgrading from `v0.1.0` to `v0.2.0`

| `v0.1.0` API | `v0.2.0` API |
|---|---|
| `kafka.Config` | `BrokerConfig` + `ProducerConfig` / `ConsumerConfig` (use `Default*Config()` or `env` tags) |
| `NewProducer(cfg)` | `NewProducer(brokerCfg, producerCfg, opts...)` |
| `NewConsumer(cfg, logger)` | `NewConsumer(brokerCfg, consumerCfg, logger, opts...)` |
| `consumer.Subscribe(ctx, topic, h, opts)` | `consumer.Subscribe(Subscription{...})` then `consumer.Run(ctx)` |
| `SubscribeMultiple(ctx, topics, h, opts)` | Call `consumer.Subscribe(...)` per topic, then `consumer.Run(ctx)` |
| `SubscribeOptions.AutoCommit` (`-1` sentinel) | `ConsumerConfig.CommitMode` (`periodic` / `sync`) & `CommitInterval` |
| `MetricsInterceptor()` / `TracingInterceptor()` | `NewMetrics` + `With*Metrics`, `TracingConfig` + `With*Tracing` / `NewTracingInterceptor` |

### Key Semantic Changes

1. `RetryStrategyLogAndIgnore` now performs bounded retries (`1 + MaxRetries`, default `3`) before discarding. Set `MaxRetries: 0` if a single attempt is desired.
2. Unresolved interrupted messages (rebalance or shutdown timeout) are no longer marked; handlers must be idempotent.
3. `RetryStrategyDLQ` requires an explicit `DLQFailurePolicy` (`discard`, `retry`, or `stop`) and rejects producers configured with `RequiredAcks=0`.
4. Per-attempt handler timeout defaults to `30s`; review long-running handlers and configure `HandlerTimeout` accordingly.
5. Configuration validation is strict (unknown `InitialOffset`, unknown `Compression`, conflicting `Idempotent`/`RequiredAcks`/`MaxRetries`, partial TLS cert/key pairs, invalid broker addresses).
6. Keep existing `GroupID` values unchanged to preserve committed offsets. Moving topics between groups does not copy offsets automatically; coordinate assignor strategy changes (`roundrobin` / `sticky` / `range`) across group replicas.
7. Close consumers before closing shared databases, loggers, or DLQ producers.

## Local Integration Testing

Integration tests are guarded by the `integration` build tag and target `KAFKA_TEST_BROKERS` (default `localhost:9092`):

```bash
# Start a pinned local Kafka KRaft broker
docker run -d --name kafka-local -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
  apache/kafka:3.7.2

# Run unit + race tests
go test -race ./...

# Run real Kafka integration tests (optionally set KAFKA_TEST_CONTAINER=kafka-local for live outage test IT11)
KAFKA_TEST_BROKERS=localhost:9092 KAFKA_TEST_CONTAINER=kafka-local \
  go test -tags=integration -race -count=1 -v ./...

# Stop and remove container
docker rm -f kafka-local
```
