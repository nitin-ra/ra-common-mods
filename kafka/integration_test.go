//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/routerarchitects/ra-common-mods/kafka"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("KAFKA_TEST_BROKERS"))
	if raw == "" {
		raw = "localhost:9092"
	}
	parts := strings.Split(raw, ",")
	var brokers []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			brokers = append(brokers, p)
		}
	}
	if len(brokers) == 0 {
		t.Fatal("KAFKA_TEST_BROKERS yielded no brokers")
	}

	// Verify broker reachability; fail visibly when integration suite is run.
	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.ReadTimeout = 5 * time.Second
	cfg.Net.WriteTimeout = 5 * time.Second
	var admin sarama.ClusterAdmin
	var err error
	deadline := time.Now().Add(25 * time.Second)
	for {
		admin, err = sarama.NewClusterAdmin(brokers, cfg)
		if err == nil {
			if _, lErr := admin.ListTopics(); lErr == nil {
				_ = admin.Close()
				break
			} else {
				err = lErr
				_ = admin.Close()
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Kafka broker(s) %v unreachable for integration tests: %v", brokers, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return brokers
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func createTestTopic(t *testing.T, brokers []string, topic string, partitions int32) {
	t.Helper()
	cfg := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		t.Fatalf("create cluster admin: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		t.Fatalf("create topic %s: %v", topic, err)
	}

	// Wait until metadata shows all partitions with leaders.
	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		t.Fatalf("create metadata client: %v", err)
	}
	defer client.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		_ = client.RefreshMetadata(topic)
		parts, pErr := client.Partitions(topic)
		if pErr == nil && int32(len(parts)) == partitions {
			allReady := true
			for _, p := range parts {
				if _, lErr := client.Leader(topic, p); lErr != nil {
					allReady = false
					break
				}
			}
			if allReady {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %s metadata not ready in time", topic)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func committedOffset(t *testing.T, brokers []string, groupID, topic string, partition int32) (int64, bool) {
	t.Helper()
	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		t.Fatalf("cluster admin: %v", err)
	}
	defer admin.Close()

	resp, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: {partition}})
	if err != nil {
		t.Fatalf("ListConsumerGroupOffsets: %v", err)
	}
	block := resp.GetBlock(topic, partition)
	if block == nil || block.Offset < 0 {
		return -1, false
	}
	return block.Offset, true
}

func waitForCondition(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func brokerCfgFor(brokers []string) kafka.BrokerConfig {
	b := kafka.DefaultBrokerConfig()
	b.Brokers = brokers
	b.ClientID = "kafka-it"
	return b
}

func consumerCfgFor(groupID string) kafka.ConsumerConfig {
	c := kafka.DefaultConsumerConfig()
	c.GroupID = groupID
	c.InitialOffset = "oldest"
	c.CommitInterval = 300 * time.Millisecond
	c.ShutdownTimeout = 5 * time.Second
	return c
}

type faultInjectingProducer struct {
	inner     kafka.Producer
	failCount atomic.Int32
	failAll   atomic.Bool
}

func (f *faultInjectingProducer) Publish(ctx context.Context, topic string, key, value []byte) error {
	return f.PublishWithHeaders(ctx, topic, key, value, nil)
}

func (f *faultInjectingProducer) PublishWithHeaders(ctx context.Context, topic string, key, value []byte, headers []kafka.RecordHeader) error {
	if f.failAll.Load() {
		return errors.New("injected DLQ failure")
	}
	if f.failCount.Load() > 0 {
		if f.failCount.Add(-1) >= 0 {
			return errors.New("injected transient DLQ failure")
		}
	}
	return f.inner.PublishWithHeaders(ctx, topic, key, value, headers)
}

func (f *faultInjectingProducer) PublishJSON(ctx context.Context, topic string, key string, data interface{}) error {
	return f.inner.PublishJSON(ctx, topic, key, data)
}

func (f *faultInjectingProducer) Close() error { return nil }

func TestIT01_BasicProduceConsumeJSONHeadersAndTracing(t *testing.T) {
	brokers := integrationBrokers(t)
	topicA := uniqueName("it01-a")
	topicB := uniqueName("it01-b")
	createTestTopic(t, brokers, topicA, 1)
	createTestTopic(t, brokers, topicB, 1)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	tCfg := kafka.TracingConfig{TracerProvider: tp, Propagator: propagation.TraceContext{}}

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig(), kafka.WithProducerTracing(tCfg))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	ctx := context.Background()
	if err := prod.PublishWithHeaders(ctx, topicA, []byte("k1"), []byte("v1"), []kafka.RecordHeader{{Key: "x-hdr", Value: []byte("abc")}}); err != nil {
		t.Fatalf("PublishWithHeaders: %v", err)
	}
	if err := prod.PublishJSON(ctx, topicB, "k2", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("PublishJSON: %v", err)
	}

	// Verify cancelled context error wrapping
	cctx, ccancel := context.WithCancel(ctx)
	ccancel()
	if err := prod.Publish(cctx, topicA, []byte("k"), []byte("v")); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	gc, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(uniqueName("grp-it01")), testLogger(), kafka.WithConsumerTracing(tCfg))
	if err != nil {
		t.Fatal(err)
	}

	var gotA, gotB atomic.Bool
	if err := gc.Subscribe(kafka.Subscription{
		Topic: topicA,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			v, ok := msg.GetHeader("x-hdr")
			if string(msg.Key) == "k1" && string(msg.Value) == "v1" && ok && string(v) == "abc" {
				gotA.Store(true)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := gc.Subscribe(kafka.Subscription{
		Topic: topicB,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if string(msg.Key) == "k2" && strings.Contains(string(msg.Value), `"hello":"world"`) {
				gotB.Store(true)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- gc.Run(runCtx) }()

	waitForCondition(t, 20*time.Second, "IT01 messages", func() bool {
		return gotA.Load() && gotB.Load()
	})
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(recorder.Ended()) < 4 {
		t.Fatalf("expected >=4 spans (2 publish + 2 consume), got %d", len(recorder.Ended()))
	}
}

func TestIT02_IT03_MultiTopicIsolationTransientRetryAndMetrics(t *testing.T) {
	brokers := integrationBrokers(t)
	topic1 := uniqueName("it02-t1")
	topic2 := uniqueName("it02-t2")
	createTestTopic(t, brokers, topic1, 1)
	createTestTopic(t, brokers, topic2, 1)

	reg := prometheus.NewRegistry()
	metrics, err := kafka.NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig(), kafka.WithProducerMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	if err := prod.Publish(context.Background(), topic1, []byte("k1"), []byte("retry-me")); err != nil {
		t.Fatal(err)
	}
	if err := prod.Publish(context.Background(), topic2, []byte("k2"), []byte("ok")); err != nil {
		t.Fatal(err)
	}

	groupID := uniqueName("grp-it02")
	gc, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(groupID), testLogger(), kafka.WithConsumerMetrics(metrics))
	if err != nil {
		t.Fatal(err)
	}

	var t1Attempts atomic.Int32
	var t1Done, t2Done atomic.Bool
	if err := gc.Subscribe(kafka.Subscription{
		Topic: topic1,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:     kafka.RetryStrategyLogAndIgnore,
			MaxRetries:   3,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
			Multiplier:   2,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if t1Attempts.Add(1) < 3 {
				return errors.New("transient error")
			}
			t1Done.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := gc.Subscribe(kafka.Subscription{
		Topic:       topic2,
		RetryPolicy: &kafka.RetryPolicy{Strategy: kafka.RetryStrategyLogAndIgnore, MaxRetries: 0},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			t2Done.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- gc.Run(runCtx) }()

	waitForCondition(t, 20*time.Second, "IT02/IT03 processing and offset commit", func() bool {
		if !t1Done.Load() || !t2Done.Load() {
			return false
		}
		o1, ok1 := committedOffset(t, brokers, groupID, topic1, 0)
		o2, ok2 := committedOffset(t, brokers, groupID, topic2, 0)
		return ok1 && o1 == 1 && ok2 && o2 == 1
	})
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if t1Attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts on topic1, got %d", t1Attempts.Load())
	}
	// Verify via registry gather that processed and retried metrics are non-zero.
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, f := range fams {
		seen[f.GetName()] = true
	}
	if !seen["kafka_messages_processed_total"] || !seen["kafka_messages_retried_total"] {
		t.Fatalf("expected processed and retried metrics in registry, got %v", seen)
	}
}

func TestIT04_BoundedRetriesAndRealDLQ(t *testing.T) {
	brokers := integrationBrokers(t)
	srcTopic := uniqueName("it04-src")
	dlqTopic := uniqueName("it04-dlq")
	createTestTopic(t, brokers, srcTopic, 1)
	createTestTopic(t, brokers, dlqTopic, 1)

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	if err := prod.PublishWithHeaders(context.Background(), srcTopic, []byte("bad-key"), []byte("bad-val"),
		[]kafka.RecordHeader{{Key: "x-trace-test", Value: []byte("preserve-me")}}); err != nil {
		t.Fatal(err)
	}

	srcGroup := uniqueName("grp-it04-src")
	gc, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(srcGroup), testLogger())
	if err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int32
	if err := gc.Subscribe(kafka.Subscription{
		Topic: srcTopic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:         kafka.RetryStrategyDLQ,
			MaxRetries:       2,
			InitialDelay:     20 * time.Millisecond,
			MaxDelay:         100 * time.Millisecond,
			Multiplier:       2,
			DLQTopic:         dlqTopic,
			DLQProducer:      prod,
			DLQFailurePolicy: kafka.DLQFailureStop,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			attempts.Add(1)
			return errors.New("permanent validation failure")
		},
	}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- gc.Run(runCtx) }()

	waitForCondition(t, 20*time.Second, "source offset committed after DLQ", func() bool {
		off, ok := committedOffset(t, brokers, srcGroup, srcTopic, 0)
		return ok && off == 1
	})
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expected 3 attempts (1 + MaxRetries=2), got %d", attempts.Load())
	}

	// Verify DLQ record content and metadata via a fresh consumer.
	dlqVerify, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(uniqueName("grp-it04-dlq")), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	var dlqMsg *kafka.Message
	var dlqMu sync.Mutex
	dlqCtx, dlqCancel := context.WithCancel(context.Background())
	if err := dlqVerify.Subscribe(kafka.Subscription{
		Topic: dlqTopic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			dlqMu.Lock()
			dlqMsg = msg
			dlqMu.Unlock()
			dlqCancel()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := dlqVerify.Run(dlqCtx); err != nil {
		t.Fatalf("dlqVerify Run: %v", err)
	}

	if dlqMsg == nil || string(dlqMsg.Key) != "bad-key" || string(dlqMsg.Value) != "bad-val" {
		t.Fatalf("unexpected DLQ record: %+v", dlqMsg)
	}
	checkHdr := func(k, want string) {
		v, ok := dlqMsg.GetHeader(k)
		if !ok || string(v) != want {
			t.Fatalf("DLQ header %s = %q (present=%v), want %q", k, string(v), ok, want)
		}
	}
	checkHdr("x-trace-test", "preserve-me")
	checkHdr(kafka.HeaderOriginalTopic, srcTopic)
	checkHdr(kafka.HeaderOriginalPartition, "0")
	checkHdr(kafka.HeaderOriginalOffset, "0")
	checkHdr(kafka.HeaderOriginalConsumerGroup, srcGroup)
	checkHdr(kafka.HeaderOriginalAttempts, "3")
	if v, ok := dlqMsg.GetHeader(kafka.HeaderOriginalError); !ok || !strings.Contains(string(v), "permanent validation failure") {
		t.Fatalf("unexpected x-original-error: %q", string(v))
	}
}

func TestIT05_DLQFailDiscardAndStopOffsetSafety(t *testing.T) {
	brokers := integrationBrokers(t)
	srcTopic := uniqueName("it05-src")
	dlqTopic := uniqueName("it05-dlq")
	createTestTopic(t, brokers, srcTopic, 1)
	createTestTopic(t, brokers, dlqTopic, 1)

	realProd, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer realProd.Close()

	// Produce 2 messages: offset 0 (fails) and offset 1 (would succeed).
	for i := 0; i < 2; i++ {
		if err := realProd.Publish(context.Background(), srcTopic, []byte("k"), []byte(fmt.Sprintf("msg-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	faultProd := &faultInjectingProducer{inner: realProd}
	faultProd.failAll.Store(true)

	stopGroup := uniqueName("grp-it05-stop")
	cc := consumerCfgFor(stopGroup)
	cc.CommitMode = kafka.CommitSync
	gcStop, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := gcStop.Subscribe(kafka.Subscription{
		Topic: srcTopic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:         kafka.RetryStrategyDLQ,
			MaxRetries:       0,
			DLQTopic:         dlqTopic,
			DLQProducer:      faultProd,
			DLQFailurePolicy: kafka.DLQFailureStop,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if string(msg.Value) == "msg-0" {
				return errors.New("fail msg-0")
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	if err := gcStop.Run(context.Background()); err == nil {
		t.Fatalf("expected terminal error from DLQFailureStop")
	}
	if off, ok := committedOffset(t, brokers, stopGroup, srcTopic, 0); ok && off > 0 {
		t.Fatalf("expected no committed offset past unresolved msg-0, got %d", off)
	}

	// Restart with same group and DLQ healed -> msg-0 and msg-1 both delivered (msg-0 dead-lettered, msg-1 processed).
	faultProd.failAll.Store(false)
	gcResume, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	var mu sync.Mutex
	resCtx, resCancel := context.WithCancel(context.Background())
	if err := gcResume.Subscribe(kafka.Subscription{
		Topic: srcTopic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:         kafka.RetryStrategyDLQ,
			MaxRetries:       0,
			DLQTopic:         dlqTopic,
			DLQProducer:      faultProd,
			DLQFailurePolicy: kafka.DLQFailureStop,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			mu.Lock()
			seen = append(seen, string(msg.Value))
			mu.Unlock()
			if string(msg.Value) == "msg-0" {
				return errors.New("fail msg-0")
			}
			resCancel()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := gcResume.Run(resCtx); err != nil {
		t.Fatalf("resume Run: %v", err)
	}
	if !slices.Equal(seen, []string{"msg-0", "msg-1"}) {
		t.Fatalf("expected replay of [msg-0 msg-1], got %v", seen)
	}

	// Verify DLQFailureDiscard advances past failure.
	faultProd.failAll.Store(true)
	discardGroup := uniqueName("grp-it05-discard")
	gcDiscard, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(discardGroup), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	var msg1Processed atomic.Bool
	dCtx, dCancel := context.WithCancel(context.Background())
	if err := gcDiscard.Subscribe(kafka.Subscription{
		Topic: srcTopic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:         kafka.RetryStrategyDLQ,
			MaxRetries:       0,
			DLQTopic:         dlqTopic,
			DLQProducer:      faultProd,
			DLQFailurePolicy: kafka.DLQFailureDiscard,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if string(msg.Value) == "msg-0" {
				return errors.New("fail msg-0")
			}
			msg1Processed.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	dDone := make(chan error, 1)
	go func() { dDone <- gcDiscard.Run(dCtx) }()
	waitForCondition(t, 20*time.Second, "discard continues to msg-1 and commits", func() bool {
		if !msg1Processed.Load() {
			return false
		}
		off, ok := committedOffset(t, brokers, discardGroup, srcTopic, 0)
		return ok && off == 2
	})
	dCancel()
	if err := <-dDone; err != nil {
		t.Fatalf("discard Run: %v", err)
	}
}

func TestIT06_DLQRetryBlocksAffectedPartitionUntilRecovered(t *testing.T) {
	brokers := integrationBrokers(t)
	srcTopic := uniqueName("it06-src")
	dlqTopic := uniqueName("it06-dlq")
	createTestTopic(t, brokers, srcTopic, 2)
	createTestTopic(t, brokers, dlqTopic, 1)

	realProd, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer realProd.Close()

	// Send one failing record and several succeeding records across keys so both partitions get work.
	sc := sarama.NewConfig()
	sc.Producer.Return.Successes = true
	rawProd, err := sarama.NewSyncProducer(brokers, sc)
	if err != nil {
		t.Fatal(err)
	}
	sc.Producer.Partitioner = sarama.NewManualPartitioner
	_ = rawProd.Close()

	// Use manual partitioner Sarama SyncProducer so partition 0 and partition 1 are deterministic.
	manProd, err := sarama.NewSyncProducer(brokers, sc)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = manProd.SendMessage(&sarama.ProducerMessage{Topic: srcTopic, Partition: 0, Value: sarama.StringEncoder("p0-fail")})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = manProd.SendMessage(&sarama.ProducerMessage{Topic: srcTopic, Partition: 0, Value: sarama.StringEncoder("p0-after")})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = manProd.SendMessage(&sarama.ProducerMessage{Topic: srcTopic, Partition: 1, Value: sarama.StringEncoder("p1-ok")})
	if err != nil {
		t.Fatal(err)
	}
	_ = manProd.Close()

	faultProd := &faultInjectingProducer{inner: realProd}
	faultProd.failAll.Store(true)

	groupID := uniqueName("grp-it06")
	gc, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(groupID), testLogger())
	if err != nil {
		t.Fatal(err)
	}

	var p1Done, p0AfterDone atomic.Bool
	if err := gc.Subscribe(kafka.Subscription{
		Topic: srcTopic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:         kafka.RetryStrategyDLQ,
			MaxRetries:       0,
			InitialDelay:     100 * time.Millisecond,
			MaxDelay:         200 * time.Millisecond,
			Multiplier:       1.5,
			DLQTopic:         dlqTopic,
			DLQProducer:      faultProd,
			DLQFailurePolicy: kafka.DLQFailureRetry,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			switch string(msg.Value) {
			case "p0-fail":
				return errors.New("send me to dlq")
			case "p0-after":
				p0AfterDone.Store(true)
			case "p1-ok":
				p1Done.Store(true)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- gc.Run(runCtx) }()

	// Partition 1 must make progress while Partition 0 waits on DLQ retry!
	waitForCondition(t, 20*time.Second, "partition 1 processes while partition 0 retries DLQ", func() bool {
		return p1Done.Load()
	})
	if p0AfterDone.Load() {
		t.Fatalf("p0-after must not run before p0-fail DLQ succeeds")
	}
	if !gc.Health().Ready {
		t.Fatalf("expected consumer ready during partition DLQ retry")
	}

	// Heal DLQ -> p0-fail transfers and p0-after completes.
	faultProd.failAll.Store(false)
	waitForCondition(t, 20*time.Second, "p0-after completes after DLQ heals", func() bool {
		return p0AfterDone.Load()
	})
	cancel()
	if err := <-runDone; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestIT07_RebalanceDuringRetryPreservesUnresolvedOffset(t *testing.T) {
	brokers := integrationBrokers(t)
	topic := uniqueName("it07-reb")
	createTestTopic(t, brokers, topic, 2)

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	for i := 0; i < 4; i++ {
		if err := prod.Publish(context.Background(), topic, []byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	groupID := uniqueName("grp-it07")
	cCfg := consumerCfgFor(groupID)
	cCfg.SessionTimeout = 6 * time.Second
	cCfg.HeartbeatInterval = 1500 * time.Millisecond
	cCfg.RebalanceTimeout = 6 * time.Second

	gc1, err := kafka.NewConsumer(brokerCfgFor(brokers), cCfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	completed := map[string]int{}
	rebalanceTrigger := make(chan struct{})
	var triggerOnce sync.Once

	if err := gc1.Subscribe(kafka.Subscription{
		Topic: topic,
		RetryPolicy: &kafka.RetryPolicy{
			Strategy:     kafka.RetryStrategyInfinite,
			InitialDelay: 500 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   1,
		},
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if string(msg.Value) == "v-0" {
				triggerOnce.Do(func() { close(rebalanceTrigger) })
				// Block/retry on gc1 until session cancels due to rebalance!
				<-ctx.Done()
				return ctx.Err()
			}
			mu.Lock()
			completed[string(msg.Value)]++
			mu.Unlock()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- gc1.Run(ctx1) }()

	<-rebalanceTrigger

	// Join second member to trigger rebalance, then stop gc1 so gc2 takes over all partitions and completes v-0..v-3.
	gc2, err := kafka.NewConsumer(brokerCfgFor(brokers), cCfg, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := gc2.Subscribe(kafka.Subscription{
		Topic: topic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			mu.Lock()
			completed[string(msg.Value)]++
			mu.Unlock()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- gc2.Run(ctx2) }()

	// Wait briefly for rebalance then gracefully stop gc1 so v-0 is guaranteed to move to gc2 if it hadn't already.
	time.Sleep(1500 * time.Millisecond)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("gc1 Run: %v", err)
	}

	waitForCondition(t, 20*time.Second, "all 4 messages completed after rebalance", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(completed) == 4
	})
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("gc2 Run: %v", err)
	}
}

func TestIT08_GracefulShutdownAndRestartPeriodicAndSync(t *testing.T) {
	brokers := integrationBrokers(t)
	for _, mode := range []kafka.CommitMode{kafka.CommitPeriodic, kafka.CommitSync} {
		t.Run(string(mode), func(t *testing.T) {
			topic := uniqueName("it08-" + string(mode))
			createTestTopic(t, brokers, topic, 2)

			sc := sarama.NewConfig()
			sc.Producer.Return.Successes = true
			sc.Producer.Partitioner = sarama.NewManualPartitioner
			rawProd, err := sarama.NewSyncProducer(brokers, sc)
			if err != nil {
				t.Fatal(err)
			}
			// Only partition 0 gets messages; partition 1 remains idle!
			_, _, err = rawProd.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: 0, Value: sarama.StringEncoder("m0")})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = rawProd.SendMessage(&sarama.ProducerMessage{Topic: topic, Partition: 0, Value: sarama.StringEncoder("m1")})
			if err != nil {
				t.Fatal(err)
			}
			_ = rawProd.Close()

			groupID := uniqueName("grp-it08-" + string(mode))
			cc := consumerCfgFor(groupID)
			cc.CommitMode = mode
			cc.CommitInterval = 200 * time.Millisecond
			cc.ShutdownTimeout = 5 * time.Second

			gc1, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
			if err != nil {
				t.Fatal(err)
			}

			m0Started := make(chan struct{})
			m0Release := make(chan struct{})
			var processed []string
			var mu sync.Mutex

			if err := gc1.Subscribe(kafka.Subscription{
				Topic: topic,
				Handler: func(ctx context.Context, msg *kafka.Message) error {
					if string(msg.Value) == "m0" {
						close(m0Started)
						<-m0Release
					}
					mu.Lock()
					processed = append(processed, string(msg.Value))
					mu.Unlock()
					return nil
				},
			}); err != nil {
				t.Fatal(err)
			}

			ctx1, cancel1 := context.WithCancel(context.Background())
			done1 := make(chan error, 1)
			go func() { done1 <- gc1.Run(ctx1) }()

			<-m0Started
			cancel1() // Shutdown while m0 is active and partition 1 is idle!
			time.Sleep(150 * time.Millisecond)
			close(m0Release)
			if err := <-done1; err != nil {
				t.Fatalf("gc1 Run: %v", err)
			}

			if !slices.Equal(processed, []string{"m0"}) {
				t.Fatalf("expected only m0 admitted/finished during drain, got %v", processed)
			}

			// Restart with same group ID -> m1 must be replayed/delivered, m0 already committed!
			gc2, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			var resumed []string
			ctx2, cancel2 := context.WithCancel(context.Background())
			if err := gc2.Subscribe(kafka.Subscription{
				Topic: topic,
				Handler: func(ctx context.Context, msg *kafka.Message) error {
					mu.Lock()
					resumed = append(resumed, string(msg.Value))
					mu.Unlock()
					cancel2()
					return nil
				},
			}); err != nil {
				t.Fatal(err)
			}
			if err := gc2.Run(ctx2); err != nil {
				t.Fatalf("gc2 Run: %v", err)
			}
			if !slices.Equal(resumed, []string{"m1"}) {
				t.Fatalf("expected m1 on restart (no skip, no m0 replay), got %v", resumed)
			}
		})
	}
}

func TestIT09_HybridMultipleGroupsIndependentDelivery(t *testing.T) {
	brokers := integrationBrokers(t)
	sharedTopic := uniqueName("it09-shared")
	onlyBTopic := uniqueName("it09-onlyb")
	createTestTopic(t, brokers, sharedTopic, 1)
	createTestTopic(t, brokers, onlyBTopic, 1)

	bCfg := brokerCfgFor(brokers)
	dlqProd, err := kafka.NewProducer(bCfg, kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer dlqProd.Close()

	if err := dlqProd.Publish(context.Background(), sharedTopic, []byte("k"), []byte("shared-msg")); err != nil {
		t.Fatal(err)
	}
	if err := dlqProd.Publish(context.Background(), onlyBTopic, []byte("k"), []byte("onlyb-msg")); err != nil {
		t.Fatal(err)
	}

	gA, err := kafka.NewConsumer(bCfg, consumerCfgFor(uniqueName("grp-it09-a")), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	gB, err := kafka.NewConsumer(bCfg, consumerCfgFor(uniqueName("grp-it09-b")), testLogger())
	if err != nil {
		t.Fatal(err)
	}

	var gotAShared, gotBShared, gotBOnly atomic.Bool
	if err := gA.Subscribe(kafka.Subscription{
		Topic: sharedTopic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			gotAShared.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := gB.Subscribe(kafka.Subscription{
		Topic: sharedTopic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			gotBShared.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := gB.Subscribe(kafka.Subscription{
		Topic: onlyBTopic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			gotBOnly.Store(true)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	doneA := make(chan error, 1)
	doneB := make(chan error, 1)
	go func() { doneA <- gA.Run(ctxA) }()
	go func() { doneB <- gB.Run(ctxB) }()

	waitForCondition(t, 20*time.Second, "both groups receive independent deliveries", func() bool {
		return gotAShared.Load() && gotBShared.Load() && gotBOnly.Load()
	})
	cancelA()
	if err := <-doneA; err != nil {
		t.Fatalf("gA Run: %v", err)
	}
	// Shared producer must still be usable after group A stops!
	if err := dlqProd.Publish(context.Background(), onlyBTopic, []byte("k2"), []byte("still-open")); err != nil {
		t.Fatalf("shared producer closed unexpectedly after gA stopped: %v", err)
	}
	cancelB()
	if err := <-doneB; err != nil {
		t.Fatalf("gB Run: %v", err)
	}
}

func TestIT10_MultipleReplicasAndAssignors(t *testing.T) {
	brokers := integrationBrokers(t)
	for _, strategy := range []string{"roundrobin", "sticky", "range"} {
		t.Run(strategy, func(t *testing.T) {
			topic := uniqueName("it10-" + strategy)
			createTestTopic(t, brokers, topic, 4)

			prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer prod.Close()

			const total = 12
			for i := 0; i < total; i++ {
				if err := prod.Publish(context.Background(), topic, []byte(fmt.Sprintf("k-%d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
					t.Fatal(err)
				}
			}

			groupID := uniqueName("grp-it10-" + strategy)
			cc := consumerCfgFor(groupID)
			cc.BalanceStrategy = strategy
			cc.SessionTimeout = 6 * time.Second
			cc.HeartbeatInterval = 1500 * time.Millisecond

			var mu sync.Mutex
			seen := map[string]int{}
			partSeq := map[int32]int64{}

			makeMember := func() kafka.Consumer {
				gc, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
				if err != nil {
					t.Fatal(err)
				}
				if err := gc.Subscribe(kafka.Subscription{
					Topic: topic,
					Handler: func(ctx context.Context, msg *kafka.Message) error {
						mu.Lock()
						defer mu.Unlock()
						seen[string(msg.Value)]++
						if prev, ok := partSeq[msg.Partition]; ok && msg.Offset != prev+1 {
							t.Errorf("non-sequential offset on partition %d: prev=%d cur=%d", msg.Partition, prev, msg.Offset)
						}
						partSeq[msg.Partition] = msg.Offset
						return nil
					},
				}); err != nil {
					t.Fatal(err)
				}
				return gc
			}

			m1 := makeMember()
			m2 := makeMember()
			ctx, cancel := context.WithCancel(context.Background())
			d1 := make(chan error, 1)
			d2 := make(chan error, 1)
			go func() { d1 <- m1.Run(ctx) }()
			go func() { d2 <- m2.Run(ctx) }()

			waitForCondition(t, 25*time.Second, "all messages consumed by 2 replicas", func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(seen) == total
			})
			cancel()
			if err := <-d1; err != nil {
				t.Fatalf("m1 Run: %v", err)
			}
			if err := <-d2; err != nil {
				t.Fatalf("m2 Run: %v", err)
			}
		})
	}
}

func TestIT11_BrokerOutageAndRestoration(t *testing.T) {
	container := strings.TrimSpace(os.Getenv("KAFKA_TEST_CONTAINER"))
	if container == "" {
		t.Skip("skipping outage test: set KAFKA_TEST_CONTAINER to the Docker Kafka container name to test live pause/unpause")
	}

	brokers := integrationBrokers(t)
	topic := uniqueName("it11-outage")
	createTestTopic(t, brokers, topic, 1)

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	gc, err := kafka.NewConsumer(brokerCfgFor(brokers), consumerCfgFor(uniqueName("grp-it11")), testLogger())
	if err != nil {
		t.Fatal(err)
	}

	var gotBefore, gotAfter atomic.Bool
	if err := gc.Subscribe(kafka.Subscription{
		Topic: topic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			switch string(msg.Value) {
			case "before":
				gotBefore.Store(true)
			case "after":
				gotAfter.Store(true)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- gc.Run(ctx) }()

	if err := prod.Publish(context.Background(), topic, []byte("k"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 15*time.Second, "before outage message", gotBefore.Load)

	// Pause broker container to simulate outage, then unpause to restore.
	if out, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Fatalf("docker pause %s: %v (%s)", container, err, string(out))
	}
	time.Sleep(2 * time.Second)
	if out, err := exec.Command("docker", "unpause", container).CombinedOutput(); err != nil {
		t.Fatalf("docker unpause %s: %v (%s)", container, err, string(out))
	}

	waitForCondition(t, 25*time.Second, "publish & consume after restoration", func() bool {
		if !gotAfter.Load() {
			_ = prod.Publish(context.Background(), topic, []byte("k"), []byte("after"))
		}
		return gotAfter.Load()
	})

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run after outage recovery: %v", err)
	}
}

func TestIT12_GroupOffsetContinuityOnRestart(t *testing.T) {
	brokers := integrationBrokers(t)
	topic := uniqueName("it12-continuity")
	createTestTopic(t, brokers, topic, 1)

	prod, err := kafka.NewProducer(brokerCfgFor(brokers), kafka.DefaultProducerConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	if err := prod.Publish(context.Background(), topic, []byte("k1"), []byte("first")); err != nil {
		t.Fatal(err)
	}

	groupID := uniqueName("grp-it12")
	cc := consumerCfgFor(groupID)
	cc.CommitMode = kafka.CommitSync

	c1, err := kafka.NewConsumer(brokerCfgFor(brokers), cc, testLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	var gotFirst atomic.Bool
	done1 := make(chan error, 1)
	if err := c1.Subscribe(kafka.Subscription{
		Topic: topic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			if string(msg.Value) == "first" {
				gotFirst.Store(true)
			}
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	go func() { done1 <- c1.Run(ctx1) }()

	waitForCondition(t, 20*time.Second, "first consumer processes first message and commits", func() bool {
		if !gotFirst.Load() {
			return false
		}
		off, ok := committedOffset(t, brokers, groupID, topic, 0)
		return ok && off == 1
	})
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("first consumer Run: %v", err)
	}

	// Publish second message and verify a restarted Consumer with same group ID resumes from offset 1 (no replay of "first").
	if err := prod.Publish(context.Background(), topic, []byte("k2"), []byte("second")); err != nil {
		t.Fatal(err)
	}

	// Even with InitialOffset="newest", an existing valid committed offset (1) must be preserved.
	cc2 := consumerCfgFor(groupID)
	cc2.InitialOffset = "newest"
	c2, err := kafka.NewConsumer(brokerCfgFor(brokers), cc2, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	var values []string
	var mu sync.Mutex
	ctx2, cancel2 := context.WithCancel(context.Background())
	if err := c2.Subscribe(kafka.Subscription{
		Topic: topic,
		Handler: func(ctx context.Context, msg *kafka.Message) error {
			mu.Lock()
			values = append(values, string(msg.Value))
			mu.Unlock()
			cancel2()
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c2.Run(ctx2); err != nil {
		t.Fatalf("second consumer Run: %v", err)
	}
	if !slices.Equal(values, []string{"second"}) {
		t.Fatalf("expected continuity from committed offset with only [second], got %v", values)
	}
}
