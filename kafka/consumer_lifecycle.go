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

type handlerPanicError struct {
	typeName string
	wrapped  error
}

func newHandlerPanicError(r any) error {
	pe := &handlerPanicError{
		typeName: fmt.Sprintf("%T", r),
	}
	if err, ok := r.(error); ok {
		pe.wrapped = err
	}
	return pe
}

func (e *handlerPanicError) Error() string {
	return "handler panic recovered"
}

func (e *handlerPanicError) Unwrap() error {
	return e.wrapped
}

type dlqPublishError struct {
	err error
}

func (e *dlqPublishError) Error() string {
	return "dlq publication failed"
}

func (e *dlqPublishError) Unwrap() error {
	return e.err
}

func classifyFailureCategory(err error) string {
	if err == nil {
		return "none"
	}
	var pe *handlerPanicError
	if errors.As(err, &pe) {
		return "handler_panic"
	}
	var de *dlqPublishError
	if errors.As(err, &de) {
		return "dlq_publish_failed"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "handler_timeout"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, ErrProducerClosed):
		return "producer_closed"
	case errors.Is(err, sarama.ErrClosedConsumerGroup), errors.Is(err, sarama.ErrClosedClient):
		return "consumer_closed"
	}
	var cfgErr sarama.ConfigurationError
	if errors.As(err, &cfgErr) {
		return "configuration_error"
	}
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return fmt.Sprintf("kafka_error_%d", int16(kerr))
	}
	return "handler_error"
}

func safeConsumerDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	class := classifyConsumerError(err)
	var de *dlqPublishError
	if errors.As(err, &de) {
		return class + ": dlq_publish_failed"
	}
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		return fmt.Sprintf("%s: kafka_error_%d", class, int16(kerr))
	}
	var cfgErr sarama.ConfigurationError
	if errors.As(err, &cfgErr) {
		return class + ": configuration_error"
	}
	switch {
	case errors.Is(err, sarama.ErrClosedConsumerGroup), errors.Is(err, sarama.ErrClosedClient):
		return class + ": consumer_closed"
	case errors.Is(err, context.DeadlineExceeded):
		return class + ": deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return class + ": context_canceled"
	default:
		return class + ": consumer_error"
	}
}

func formatConsumerError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}
