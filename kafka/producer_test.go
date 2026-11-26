package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type fakeSyncProducer struct {
	mu       sync.Mutex
	sent     []*sarama.ProducerMessage
	sendFn   func(*sarama.ProducerMessage) (int32, int64, error)
	closed   atomic.Bool
	closeCnt atomic.Int32
	closeErr error
}

func (f *fakeSyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.mu.Lock()
	f.sent = append(f.sent, msg)
	fn := f.sendFn
	f.mu.Unlock()
	if fn != nil {
		return fn(msg)
	}
	return 0, int64(len(f.sent)), nil
}

func (f *fakeSyncProducer) Close() error {
	f.closed.Store(true)
	f.closeCnt.Add(1)
	return f.closeErr
}

func newTestProducer(t *testing.T, f *fakeSyncProducer, mutate func(*ProducerConfig)) Producer {
	t.Helper()
	b := DefaultBrokerConfig()
	b.Brokers = []string{"localhost:9092"}
	p := DefaultProducerConfig()
	if mutate != nil {
		mutate(&p)
	}
	prod, err := NewProducer(b, p, func(o *producerOptions) {
		o.syncProducerFactory = func([]string, *sarama.Config) (syncProducerClient, error) {
			return f, nil
		}
	})
	if err != nil {
		t.Fatalf("NewProducerWithConfig: %v", err)
	}
	return prod
}

func TestProducerContextAndConcurrencyAndClose(t *testing.T) {
	t.Run("cancelled context before Publish and PublishJSON skips SendMessage", func(t *testing.T) {
		f := &fakeSyncProducer{}
		p := newTestProducer(t, f, nil)
		defer p.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := p.Publish(ctx, "topic-a", []byte("k"), []byte("v")); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}

		// PublishJSON must check context before marshaling even an unmarshalable value.
		unmarshalable := make(chan int)
		if err := p.PublishJSON(ctx, "topic-a", "k", unmarshalable); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled before JSON marshal, got %v", err)
		}

		deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer deadlineCancel()
		if err := p.PublishWithHeaders(deadlineCtx, "topic-a", []byte("k"), []byte("v"), nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}

		if len(f.sent) != 0 {
			t.Fatalf("expected 0 sends on cancelled contexts, got %d", len(f.sent))
		}
	})

	t.Run("preserves real send outcome after late context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		f := &fakeSyncProducer{
			sendFn: func(msg *sarama.ProducerMessage) (int32, int64, error) {
				// Simulate caller ctx expiring while SendMessage is already in flight.
				cancel()
				return 1, 42, nil
			},
		}
		p := newTestProducer(t, f, nil)
		defer p.Close()

		if err := p.Publish(ctx, "topic-a", []byte("k"), []byte("v")); err != nil {
			t.Fatalf("expected nil when started SendMessage succeeded, got %v", err)
		}
	})

	t.Run("empty topic rejected and headers copied without mutating caller slice", func(t *testing.T) {
		f := &fakeSyncProducer{}
		p := newTestProducer(t, f, nil)
		defer p.Close()

		if err := p.Publish(context.Background(), " ", []byte("k"), []byte("v")); err == nil {
			t.Fatalf("expected error for blank topic")
		}

		callerHeaders := []RecordHeader{{Key: "h1", Value: []byte("v1")}}
		if err := p.PublishWithHeaders(context.Background(), "topic-a", []byte("k"), []byte("v"), callerHeaders); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		callerHeaders[0].Value[0] = 'X'

		if string(f.sent[0].Headers[0].Value) != "v1" {
			t.Fatalf("caller mutation affected sent header")
		}
	})

	t.Run("close coordinates with active sends and is idempotent", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		f := &fakeSyncProducer{
			sendFn: func(msg *sarama.ProducerMessage) (int32, int64, error) {
				close(started)
				<-release
				return 0, 1, nil
			},
		}
		p := newTestProducer(t, f, nil)

		var wg sync.WaitGroup
		wg.Add(1)
		var sendErr error
		go func() {
			defer wg.Done()
			sendErr = p.Publish(context.Background(), "topic-a", []byte("k"), []byte("v"))
		}()

		<-started
		closeDone := make(chan error, 2)
		go func() { closeDone <- p.Close() }()
		go func() { closeDone <- p.Close() }()

		// Close must wait for active SendMessage before closing underlying producer.
		select {
		case err := <-closeDone:
			t.Fatalf("Close returned before active SendMessage completed: %v", err)
		case <-time.After(20 * time.Millisecond):
		}

		close(release)
		wg.Wait()
		if sendErr != nil {
			t.Fatalf("active send failed: %v", sendErr)
		}
		for i := 0; i < 2; i++ {
			if err := <-closeDone; err != nil {
				t.Fatalf("Close returned error: %v", err)
			}
		}
		if f.closeCnt.Load() != 1 {
			t.Fatalf("expected single underlying Close, got %d", f.closeCnt.Load())
		}

		if err := p.Publish(context.Background(), "topic-a", []byte("k"), []byte("v")); !errors.Is(err, ErrProducerClosed) {
			t.Fatalf("expected ErrProducerClosed, got %v", err)
		}
	})
}
