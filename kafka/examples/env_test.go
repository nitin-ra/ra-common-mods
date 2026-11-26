package examples_test

import (
	"testing"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/routerarchitects/ra-common-mods/kafka"
)

func TestEnvParsingBrokerProducerConsumerConfig(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("KAFKA_CLIENT_ID", "svc-test")
	t.Setenv("KAFKA_DIAL_TIMEOUT", "12s")
	t.Setenv("KAFKA_READ_TIMEOUT", "15s")
	t.Setenv("KAFKA_WRITE_TIMEOUT", "20s")
	t.Setenv("KAFKA_CONSUMER_GROUP_ID", "grp-env")
	t.Setenv("KAFKA_CONSUMER_INITIAL_OFFSET", "oldest")
	t.Setenv("KAFKA_CONSUMER_BALANCE_STRATEGY", "sticky")
	t.Setenv("KAFKA_CONSUMER_REBALANCE_TIMEOUT", "45s")
	t.Setenv("KAFKA_CONSUMER_COMMIT_MODE", "sync")
	t.Setenv("KAFKA_CONSUMER_HANDLER_TIMEOUT", "25s")
	t.Setenv("KAFKA_CONSUMER_SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("KAFKA_PRODUCER_COMPRESSION", "gzip")
	t.Setenv("KAFKA_PRODUCER_MAX_RETRIES", "5")

	type appConfig struct {
		Broker   kafka.BrokerConfig
		Consumer kafka.ConsumerConfig `envPrefix:"KAFKA_CONSUMER_"`
		Producer kafka.ProducerConfig `envPrefix:"KAFKA_PRODUCER_"`
	}
	var app appConfig
	if err := env.Parse(&app); err != nil {
		t.Fatalf("env.Parse(app): %v", err)
	}
	if len(app.Broker.Brokers) != 2 || app.Broker.ClientID != "svc-test" ||
		app.Broker.DialTimeout != 12*time.Second || app.Broker.ReadTimeout != 15*time.Second || app.Broker.WriteTimeout != 20*time.Second ||
		app.Consumer.GroupID != "grp-env" || app.Consumer.InitialOffset != "oldest" ||
		app.Consumer.BalanceStrategy != "sticky" || app.Consumer.RebalanceTimeout != 45*time.Second ||
		app.Consumer.CommitMode != kafka.CommitSync || app.Consumer.HandlerTimeout != 25*time.Second ||
		app.Consumer.ShutdownTimeout != 15*time.Second || app.Producer.Compression != "gzip" ||
		app.Producer.MaxRetries != 5 {
		t.Fatalf("unexpected composed parsed config: %+v", app)
	}
}
