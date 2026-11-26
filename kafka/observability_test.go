package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestMetricsOptInAndAccuracy(t *testing.T) {
	// UT31: Verify nothing is registered on prometheus.DefaultRegisterer.
	beforeFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if _, err := NewMetrics(nil); err == nil {
		t.Fatalf("expected error for nil registerer")
	}

	afterFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeFamilies) != len(afterFamilies) {
		t.Fatalf("NewMetrics must not register into prometheus.DefaultRegisterer")
	}

	// Producer metrics: success + cancelled failure.
	fp := &fakeSyncProducer{}
	prod, err := NewProducer(testBrokerConfig(), DefaultProducerConfig(),
		WithProducerMetrics(m),
		func(o *producerOptions) {
			o.syncProducerFactory = func([]string, *sarama.Config) (syncProducerClient, error) { return fp, nil }
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := prod.Publish(context.Background(), "orders", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancelP := context.WithCancel(context.Background())
	cancelP()
	_ = prod.Publish(cancelCtx, "orders", []byte("k"), []byte("v"))
	_ = prod.Close()

	if got := testutil.ToFloat64(m.publishErrorsTotal.WithLabelValues("orders", "cancelled")); got != 1 {
		t.Fatalf("expected publish error cancelled=1, got %v", got)
	}

	// Consumer metrics: retries, success, discard, DLQ, commit attempts, commit errors, lag cleanup, rebalances, ready.
	fg := newFakeConsumerGroup()
	var sessionNum atomic.Int32
	fg.consumeFn = func(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		n := sessionNum.Add(1)
		if n == 1 {
			c := newFakeClaim("orders", 0,
				&sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 10}, // fails once then succeeds
				&sarama.ConsumerMessage{Topic: "orders", Partition: 0, Offset: 11}, // exhausts -> DLQ
			)
			c.hwm.Store(15)
			close(c.msgs)
			_, err := runFakeSession(ctx, handler, c)
			return err
		}
		// Second session verifies rebalance counter and lag cleanup.
		fg.errCh <- &sarama.ConsumerError{Topic: "orders", Partition: 0, Err: sarama.ErrOffsetMetadataTooLarge}
		time.Sleep(15 * time.Millisecond)
		c := newFakeClaim("orders", 0)
		_, err := runFakeSession(ctx, handler, c)
		return err
	}

	cfg := testConsumerConfig("grp-metrics")
	cfg.CommitMode = CommitSync
	gc, err := NewConsumer(testBrokerConfig(), cfg, discardLogger(),
		WithConsumerMetrics(m),
		func(o *consumerOptions) {
			o.consumerGroupFactory = func([]string, string, *sarama.Config) (consumerGroupClient, error) { return fg, nil }
			o.sleepFn = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
			o.jitterFn = func(d time.Duration) time.Duration { return d }
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	dlq := &mockDLQProducer{}
	var calls atomic.Int32
	runCtx, runCancel := context.WithCancel(context.Background())
	if err := gc.Subscribe(Subscription{
		Topic: "orders",
		RetryPolicy: &RetryPolicy{
			Strategy:         RetryStrategyDLQ,
			MaxRetries:       1,
			InitialDelay:     time.Millisecond,
			MaxDelay:         time.Millisecond,
			Multiplier:       1,
			DLQTopic:         "orders-dlq",
			DLQProducer:      dlq,
			DLQFailurePolicy: DLQFailureDiscard,
		},
		Handler: func(ctx context.Context, msg *Message) error {
			c := calls.Add(1)
			if msg.Offset == 10 && c == 1 {
				return errors.New("transient")
			}
			if msg.Offset == 11 {
				return errors.New("permanent")
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	go func() {
		for sessionNum.Load() < 2 {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(25 * time.Millisecond)
		runCancel()
	}()

	if err := gc.Run(runCtx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := testutil.ToFloat64(m.messagesProcessedTotal.WithLabelValues("grp-metrics", "orders")); got != 1 {
		t.Fatalf("messagesProcessedTotal = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.messagesFailedTotal.WithLabelValues("grp-metrics", "orders")); got != 3 {
		t.Fatalf("messagesFailedTotal = %v, want 3 (1 on offset 10 + 2 on offset 11)", got)
	}
	if got := testutil.ToFloat64(m.messagesRetriedTotal.WithLabelValues("grp-metrics", "orders")); got != 2 {
		t.Fatalf("messagesRetriedTotal = %v, want 2", got)
	}
	if got := testutil.ToFloat64(m.dlqMessagesTotal.WithLabelValues("grp-metrics", "orders", "success")); got != 1 {
		t.Fatalf("dlqMessagesTotal success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.rebalancesTotal.WithLabelValues("grp-metrics")); got < 1 {
		t.Fatalf("rebalancesTotal = %v, want >= 1", got)
	}
	if got := testutil.ToFloat64(m.commitAttemptsTotal.WithLabelValues("grp-metrics")); got != 3 {
		t.Fatalf("commitAttemptsTotal = %v, want 3 (2 per-msg + 1 cleanup)", got)
	}
	if got := testutil.ToFloat64(m.commitErrorsTotal.WithLabelValues("grp-metrics")); got != 1 {
		t.Fatalf("commitErrorsTotal = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.consumerReady.WithLabelValues("grp-metrics")); got != 0 {
		t.Fatalf("consumerReady after Run = %v, want 0", got)
	}

	// Verify stale lag series was removed on Cleanup.
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, fam := range families {
		if fam.GetName() == "kafka_consumer_lag" && len(fam.GetMetric()) > 0 {
			t.Fatalf("expected kafka_consumer_lag series to be cleaned up on Cleanup, still has %d", len(fam.GetMetric()))
		}
	}
}

func TestStructuredLogsNoInfoSuccessFloodOrSecrets(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx2, cancel2 := context.WithCancel(context.Background())
	fg2 := newFakeConsumerGroup()
	fg2.consumeFn = func(runCtx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		c := newFakeClaim("telemetry", 0,
			&sarama.ConsumerMessage{Topic: "telemetry", Partition: 0, Offset: 1, Value: []byte("SECRET_PAYLOAD_ABC")},
			&sarama.ConsumerMessage{Topic: "telemetry", Partition: 0, Offset: 2, Value: []byte("SECRET_PAYLOAD_DEF")},
		)
		_, err := runFakeSession(runCtx, handler, c)
		return err
	}
	gc2, err := NewConsumer(testBrokerConfig(), testConsumerConfig("grp-logs"), logger, func(o *consumerOptions) {
		o.consumerGroupFactory = func([]string, string, *sarama.Config) (consumerGroupClient, error) { return fg2, nil }
	})
	if err != nil {
		t.Fatal(err)
	}
	var processed atomic.Int32
	if err := gc2.Subscribe(Subscription{
		Topic:        "telemetry",
		RetryPolicy:  &RetryPolicy{Strategy: RetryStrategyLogAndIgnore, MaxRetries: 0},
		Interceptors: []Interceptor{LoggingInterceptor(logger)},
		Handler: func(ctx context.Context, msg *Message) error {
			if processed.Add(1) == 1 {
				return nil
			}
			return errors.New("processing error")
		},
	}); err != nil {
		t.Fatal(err)
	}
	go func() {
		for processed.Load() < 2 {
			time.Sleep(5 * time.Millisecond)
		}
		time.Sleep(15 * time.Millisecond)
		cancel2()
	}()
	if err := gc2.Run(ctx2); err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if strings.Contains(output, "SECRET_PAYLOAD") {
		t.Fatalf("logs must not include raw message payloads")
	}
	// Verify no per-message info-level success logs were emitted.
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}
		if entry["level"] == "INFO" && entry["outcome"] == "processed" {
			t.Fatalf("success message logged at INFO level: %s", line)
		}
	}
	if !strings.Contains(output, `"outcome":"discarded"`) || !strings.Contains(output, `"group_id":"grp-logs"`) {
		t.Fatalf("expected discard log with group_id and outcome fields, got:\n%s", output)
	}
}

func TestOpenTelemetryTracingAndDisabledMode(t *testing.T) {
	// UT33: Disabled tracing mode is safe and adds no headers.
	fpDisabled := &fakeSyncProducer{}
	pDisabled, err := NewProducer(testBrokerConfig(), DefaultProducerConfig(),
		WithProducerTracing(TracingConfig{}),
		func(o *producerOptions) {
			o.syncProducerFactory = func([]string, *sarama.Config) (syncProducerClient, error) { return fpDisabled, nil }
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := pDisabled.Publish(context.Background(), "t", []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	_ = pDisabled.Close()
	if len(fpDisabled.sent[0].Headers) != 0 {
		t.Fatalf("disabled tracing should not inject headers")
	}

	// Enabled tracing with SpanRecorder and W3C TraceContext propagator.
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracingCfg := TracingConfig{
		TracerProvider: tp,
		Propagator:     propagation.TraceContext{},
	}

	fp := &fakeSyncProducer{}
	prod, err := NewProducer(testBrokerConfig(), DefaultProducerConfig(),
		WithProducerTracing(tracingCfg),
		func(o *producerOptions) {
			o.syncProducerFactory = func([]string, *sarama.Config) (syncProducerClient, error) { return fp, nil }
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	callerHeaders := []RecordHeader{{Key: "x-app", Value: []byte("1")}}
	if err := prod.PublishWithHeaders(context.Background(), "orders", []byte("k"), []byte("secret-payload"), callerHeaders); err != nil {
		t.Fatal(err)
	}
	if len(callerHeaders) != 1 {
		t.Fatalf("caller header slice must not be mutated by tracing injection")
	}

	sentMsg := fp.sent[0]
	var saramaHeaders []*sarama.RecordHeader
	var hasTraceParent bool
	for _, h := range sentMsg.Headers {
		hc := h
		saramaHeaders = append(saramaHeaders, &hc)
		if string(h.Key) == "traceparent" {
			hasTraceParent = true
		}
	}
	if !hasTraceParent {
		t.Fatalf("expected W3C traceparent header injected into sent message")
	}

	// Consume the produced message and verify parent trace propagation + panic span recording.
	fg := newFakeConsumerGroup()
	ctx, cancel := context.WithCancel(context.Background())
	fg.consumeFn = func(runCtx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
		c := newFakeClaim("orders", 0, &sarama.ConsumerMessage{
			Topic:     "orders",
			Partition: 0,
			Offset:    50,
			Headers:   saramaHeaders,
			Value:     []byte("secret-payload"),
		})
		_, err := runFakeSession(runCtx, handler, c)
		return err
	}

	gc, err := NewConsumer(testBrokerConfig(), testConsumerConfig("grp-trace"), discardLogger(),
		WithConsumerTracing(tracingCfg),
		func(o *consumerOptions) {
			o.consumerGroupFactory = func([]string, string, *sarama.Config) (consumerGroupClient, error) { return fg, nil }
			o.sleepFn = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
			o.jitterFn = func(d time.Duration) time.Duration { return d }
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	var attempt atomic.Int32
	if err := gc.Subscribe(Subscription{
		Topic:       "orders",
		RetryPolicy: &RetryPolicy{Strategy: RetryStrategyLogAndIgnore, MaxRetries: 1, InitialDelay: time.Millisecond, MaxDelay: time.Millisecond, Multiplier: 1},
		Handler: func(hCtx context.Context, msg *Message) error {
			sc := trace.SpanContextFromContext(hCtx)
			if !sc.IsValid() {
				t.Fatalf("expected valid extracted/child SpanContext inside handler")
			}
			if attempt.Add(1) == 1 {
				panic("boom-in-handler")
			}
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := gc.Run(ctx); err != nil {
		t.Fatal(err)
	}

	ended := recorder.Ended()
	if len(ended) != 3 {
		t.Fatalf("expected 3 ended spans (1 publish + 2 consume attempts), got %d", len(ended))
	}
	pubSpan := ended[0]
	c1 := ended[1]
	c2 := ended[2]
	if pubSpan.SpanContext().TraceID() != c1.SpanContext().TraceID() ||
		pubSpan.SpanContext().TraceID() != c2.SpanContext().TraceID() {
		t.Fatalf("expected producer and consumer spans to share TraceID")
	}
	if c1.Status().Code != codes.Error {
		t.Fatalf("expected panic attempt span to have Error status, got %v", c1.Status().Code)
	}
	if c2.Status().Code == codes.Error {
		t.Fatalf("expected retry success span not to have Error status")
	}
}
