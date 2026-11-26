package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

// ErrConsumerAlreadyRunning is returned when Subscribe or Run is called on a consumer that has already started.
var ErrConsumerAlreadyRunning = errors.New("kafka: consumer has already started")

// ErrConsumerClosed is returned when operating on a closed consumer.
var ErrConsumerClosed = errors.New("kafka: consumer is closed")

// ErrNoSubscriptions is returned when Run is called before any topic subscriptions are registered.
var ErrNoSubscriptions = errors.New("kafka: no topic subscriptions registered")

var errGracefulShutdown = errors.New("kafka: graceful shutdown")

type admissionGate struct {
	mu       sync.Mutex
	stopping bool
	inFlight int
	drained  chan struct{}
}

func newAdmissionGate() *admissionGate {
	return &admissionGate{}
}

func (g *admissionGate) TryAdmit(sessionCtx context.Context) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping || sessionCtx.Err() != nil {
		return false
	}
	g.inFlight++
	return true
}

func (g *admissionGate) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inFlight--
	if g.inFlight < 0 {
		g.inFlight = 0
	}
	if g.stopping && g.inFlight == 0 && g.drained != nil {
		close(g.drained)
		g.drained = nil
	}
}

func (g *admissionGate) CloseAndDrainedCh() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopping = true
	if g.inFlight == 0 {
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	if g.drained == nil {
		g.drained = make(chan struct{})
	}
	return g.drained
}

func (g *admissionGate) IsStopping() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopping
}

func isIdentifiableCommitError(err error) bool {
	if err == nil {
		return false
	}
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		switch kerr {
		case sarama.ErrOffsetMetadataTooLarge,
			sarama.ErrInvalidCommitOffsetSize,
			sarama.ErrUnstableOffsetCommit:
			return true
		}
	}
	var ces sarama.ConsumerErrors
	if errors.As(err, &ces) {
		for _, ce := range ces {
			if ce != nil && isIdentifiableCommitError(ce) {
				return true
			}
		}
	}
	return false
}

func isTerminalSaramaError(err error) bool {
	if err == nil {
		return false
	}

	var cfgErr sarama.ConfigurationError
	if errors.As(err, &cfgErr) {
		return true
	}

	if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, sarama.ErrClosedClient) {
		return true
	}

	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return isTerminalKError(kerr)
	}

	var ces sarama.ConsumerErrors
	if errors.As(err, &ces) {
		for _, ce := range ces {
			if ce != nil && isTerminalSaramaError(ce) {
				return true
			}
		}
	}

	return false
}

func isTerminalKError(kerr sarama.KError) bool {
	switch kerr {
	case sarama.ErrTopicAuthorizationFailed,
		sarama.ErrGroupAuthorizationFailed,
		sarama.ErrClusterAuthorizationFailed,
		sarama.ErrTransactionalIDAuthorizationFailed,
		sarama.ErrDelegationTokenAuthorizationFailed,
		sarama.ErrSASLAuthenticationFailed,
		sarama.ErrUnsupportedSASLMechanism,
		sarama.ErrIllegalSASLState,
		sarama.ErrUnsupportedVersion,
		sarama.ErrUnsupportedForMessageFormat,
		sarama.ErrInconsistentGroupProtocol,
		sarama.ErrInvalidGroupId,
		sarama.ErrInvalidSessionTimeout,
		sarama.ErrInvalidTopic,
		sarama.ErrPolicyViolation,
		sarama.ErrGroupMaxSizeReached,
		sarama.ErrFencedInstancedId,
		sarama.ErrInvalidConfig,
		sarama.ErrInvalidRequiredAcks,
		sarama.ErrUnsupportedCompressionType,
		sarama.ErrSecurityDisabled:
		return true
	default:
		return false
	}
}

func classifyConsumerError(err error) string {
	if err == nil {
		return "none"
	}
	if isIdentifiableCommitError(err) {
		return "commit"
	}
	if isTerminalSaramaError(err) {
		return "terminal"
	}
	return "transient"
}

func formatConsumerError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}
