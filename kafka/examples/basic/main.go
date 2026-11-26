package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/routerarchitects/ra-common-mods/kafka"
	"github.com/routerarchitects/ra-common-mods/logger"
)

// Event represents a sample message structure
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
	Worker    int       `json:"worker"`
}

type Config struct {
	Log      logger.Config        `json:"log"`
	Broker   kafka.BrokerConfig   `json:"broker"`
	Producer kafka.ProducerConfig `json:"producer" envPrefix:"KAFKA_PRODUCER_"`
	Consumer kafka.ConsumerConfig `json:"consumer" envPrefix:"KAFKA_CONSUMER_"`
}

func main() {
	var appCfg Config
	if err := env.Parse(&appCfg); err != nil {
		panic(err)
	}

	aLog, shutdown, err := logger.Init(appCfg.Log)
	if err != nil {
		panic(err)
	}
	defer shutdown()

	aLog.Info("Logger Initiated")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	brokerCfg := appCfg.Broker
	if len(brokerCfg.Brokers) == 0 {
		brokerCfg.Brokers = []string{"localhost:9092"}
	}
	brokerCfg.ClientID = "kafka-example"

	producerCfg := appCfg.Producer
	consumerCfg := appCfg.Consumer
	consumerCfg.GroupID = "example-consumer-group"

	topic := "example-topic"

	go runProducer(ctx, brokerCfg, producerCfg, topic, aLog)

	time.Sleep(2 * time.Second)

	go runConsumer(ctx, brokerCfg, consumerCfg, topic, aLog)

	<-sigterm
	aLog.Info("Shutting down gracefully...")
	cancel()

	time.Sleep(2 * time.Second)
	aLog.Info("Shutdown complete")
}

// runProducer creates a producer and spawns producer goroutines to publish messages.
func runProducer(ctx context.Context, brokerCfg kafka.BrokerConfig, producerCfg kafka.ProducerConfig, topic string, logger *slog.Logger) {
	producer, err := kafka.NewProducer(brokerCfg, producerCfg)
	if err != nil {
		logger.Error("Failed to create producer", "error", err)
		return
	}
	defer producer.Close()

	logger.Info("Producer started")

	// Spawn producer goroutines.
	var wg sync.WaitGroup
	numProducers := 1

	for i := 1; i <= numProducers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			messageCount := 0
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()

		workerLoop:
			for {
				select {
				case <-ctx.Done():
					logger.Info("Producer worker shutting down",
						"worker", workerID,
						"messages_sent", messageCount,
					)
					return

				case <-ticker.C:
					if messageCount >= 20 {
						logger.Info("Producer worker reached max message limit",
							"worker", workerID,
							"messages_sent", messageCount,
						)
						break workerLoop
					}
					// Create event
					event := Event{
						ID:        fmt.Sprintf("event-%d-%d", workerID, messageCount),
						Type:      "user.action",
						Payload:   fmt.Sprintf("Message from worker %d, count %d", workerID, messageCount),
						Timestamp: time.Now(),
						Worker:    workerID,
					}

					// Publish as JSON
					err := producer.PublishJSON(ctx, topic, event.ID, event)
					if err != nil {
						logger.Error("Failed to publish message",
							"worker", workerID,
							"error", err,
						)
					} else {
						messageCount++
						logger.Info("Published message",
							"worker", workerID,
							"event_id", event.ID,
							"total_sent", messageCount,
						)
					}
				}
			}
		}(i)
	}

	// Wait for all producers to finish
	wg.Wait()
	logger.Info("All producer workers stopped")
}

// runConsumer creates a consumer and processes messages.
func runConsumer(ctx context.Context, brokerCfg kafka.BrokerConfig, consumerCfg kafka.ConsumerConfig, topic string, logger *slog.Logger) {
	consumer, err := kafka.NewConsumer(brokerCfg, consumerCfg, logger)
	if err != nil {
		logger.Error("Failed to create consumer", "error", err)
		return
	}
	defer consumer.Close()

	logger.Info("Consumer started")

	// Track statistics
	stats := &consumerStats{
		processedByWorker: make(map[int]int),
	}

	// Define message handler
	handler := func(ctx context.Context, msg *kafka.Message) error {
		// Decode the event
		var event Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			logger.Error("Failed to unmarshal event",
				"error", err,
				"value", string(msg.Value),
			)
			return err
		}

		// Simulate processing time
		processingTime := time.Duration(50+event.Worker*10) * time.Millisecond
		time.Sleep(processingTime)

		// Track stats
		stats.mu.Lock()
		stats.total++
		stats.processedByWorker[event.Worker]++
		currentTotal := stats.total
		stats.mu.Unlock()

		if event.ID == "event-1-5" {
			// Simulate occasional processing failure
			err := fmt.Errorf("simulated processing error for message index %s", event.ID)
			logger.Error("Processing failed for message", "event_id", event.ID,
				"event_type", event.Type,
				"producer_worker", event.Worker,
				"processing_time", processingTime,
				"total_processed", currentTotal,
				"error", err,
			)
			return err
		} else {
			logger.Info("Processed message",
				"event_id", event.ID,
				"event_type", event.Type,
				"producer_worker", event.Worker,
				"processing_time", processingTime,
				"total_processed", currentTotal,
			)
		}

		return nil
	}

	// Register subscription
	if err := consumer.Subscribe(kafka.Subscription{
		Topic:   topic,
		Handler: handler,
		Interceptors: []kafka.Interceptor{
			kafka.RecoveryInterceptor(logger),
		},
		RetryPolicy: kafka.DefaultRetryPolicy(),
	}); err != nil {
		logger.Error("Subscribe failed", "error", err)
		return
	}

	// Start stats reporter
	go reportStats(ctx, stats, logger)

	// Run consumer (blocking until context is cancelled)
	err = consumer.Run(ctx)
	if err != nil && err != context.Canceled {
		logger.Error("Consumer error", "error", err)
	}

	// Print final statistics
	stats.mu.Lock()
	logger.Info("Consumer stopped",
		"total_processed", stats.total,
		"by_worker", stats.processedByWorker,
	)
	stats.mu.Unlock()
}

// consumerStats tracks consumer statistics
type consumerStats struct {
	mu                sync.Mutex
	total             int
	processedByWorker map[int]int
}

// reportStats periodically reports consumer statistics
func reportStats(ctx context.Context, stats *consumerStats, logger *slog.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats.mu.Lock()
			total := stats.total
			byWorker := make(map[int]int)
			for k, v := range stats.processedByWorker {
				byWorker[k] = v
			}
			stats.mu.Unlock()

			logger.Info("=== Consumer Statistics ===",
				"total_messages_processed", total,
				"messages_by_producer_worker", byWorker,
			)
		}
	}
}
