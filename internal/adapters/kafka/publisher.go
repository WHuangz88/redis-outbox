package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
	"kafka-demo/internal/domain"
)

// KafkaPublisher implements EventPublisher and DLQPublisher.
type KafkaPublisher struct {
	writer   *kafka.Writer
	failNext bool
	mu       sync.Mutex
}

// NewKafkaPublisher initializes a Kafka writer configured for dynamic topic routing.
func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Balancer: &kafka.LeastBytes{},
		},
	}
}

// SimulateFailure triggers a publish failure on the next invocation for resilience testing.
func (p *KafkaPublisher) SimulateFailure(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext = fail
}

func (p *KafkaPublisher) checkFailure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failNext {
		p.failNext = false
		return domain.ErrPublishFailed
	}
	return nil
}

// Publish writes a message to the specified topic, mapping input map headers to Kafka headers.
func (p *KafkaPublisher) Publish(ctx context.Context, topic string, key string, value []byte, headers map[string]string) error {
	if err := p.checkFailure(); err != nil {
		return err
	}

	var kafkaHeaders []kafka.Header
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	// Dynamic routing is enabled by setting the Topic directly in the kafka.Message structure
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: kafkaHeaders,
	})
}

// PublishDLQ routes a failed message payload into a DLQ topic with the failure error attached in headers.
func (p *KafkaPublisher) PublishDLQ(ctx context.Context, topic string, key string, value []byte, err error, headers map[string]string) error {
	dlqHeaders := make(map[string]string)
	for k, v := range headers {
		dlqHeaders[k] = v
	}
	if err != nil {
		dlqHeaders["x-error-message"] = err.Error()
	}
	return p.Publish(ctx, topic, key, value, dlqHeaders)
}

// Close gracefully flushes and shutdowns the underlying Kafka writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
