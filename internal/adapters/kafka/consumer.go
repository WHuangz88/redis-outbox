package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"kafka-demo/internal/domain"
	"kafka-demo/internal/ports"
)

// kafkaReader defines the subset of kafka.Reader methods we use, allowing mocking in tests.
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaConsumer manages consumption from a Kafka topic and routes messages to worker routines.
type KafkaConsumer struct {
	reader      kafkaReader
	logger      ports.Logger
	metrics     ports.Metrics
	dlq         ports.DLQPublisher
	dlqTopic    string
	workerCount int
	maxRetries  int
	retryDelay  time.Duration
	wg          sync.WaitGroup
}

// NewKafkaConsumer builds and configures the Kafka Reader adapter.
func NewKafkaConsumer(
	brokers []string,
	topic, group string,
	logger ports.Logger,
	metrics ports.Metrics,
	dlq ports.DLQPublisher,
	dlqTopic string,
	workerCount, maxRetries int,
	retryDelay time.Duration,
) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  group,
			Topic:    topic,
			MinBytes: 10e3, // 10KB
			MaxBytes: 10e6, // 10MB
		}),
		logger:      logger,
		metrics:     metrics,
		dlq:         dlq,
		dlqTopic:    dlqTopic,
		workerCount: workerCount,
		maxRetries:  maxRetries,
		retryDelay:  retryDelay,
	}
}

// SetReader overrides the default reader (useful for injecting mocks).
func (c *KafkaConsumer) SetReader(r kafkaReader) {
	c.reader = r
}

// Consume starts the worker pool and runs the main event polling loop.
func (c *KafkaConsumer) Consume(ctx context.Context, handler func(ctx context.Context, key []byte, value []byte, headers map[string]string) error) error {
	jobs := make(chan kafka.Message, c.workerCount*2)

	// Launch worker pool
	for i := 0; i < c.workerCount; i++ {
		c.wg.Add(1)
		go c.worker(ctx, i, jobs, handler)
	}

	c.logger.Info(ctx, "Kafka Consumer started worker pool", "worker_count", c.workerCount)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			// Check if loop is terminating due to context cancellation
			if ctx.Err() != nil {
				break
			}
			c.logger.Error(ctx, "Failed to fetch Kafka message", err)
			c.metrics.IncKafkaConsumeError()
			
			// Simple backoff to avoid hot loop on connection outages
			select {
			case <-ctx.Done():
				break
			case <-time.After(1 * time.Second):
			}
			continue
		}

		select {
		case jobs <- msg:
		case <-ctx.Done():
			break
		}
	}

	// Close channel and wait for all workers to finish processing current items
	close(jobs)
	c.wg.Wait()
	return nil
}

func (c *KafkaConsumer) worker(
	cancelCtx context.Context,
	workerID int,
	jobs <-chan kafka.Message,
	handler func(ctx context.Context, key []byte, value []byte, headers map[string]string) error,
) {
	defer c.wg.Done()

	for msg := range jobs {
		// 1. Parse and extract metadata headers
		headers := make(map[string]string)
		for _, h := range msg.Headers {
			headers[h.Key] = string(h.Value)
		}

		correlationID := headers["correlation_id"]
		if correlationID == "" {
			correlationID = headers["request_id"]
		}

		// Inject Correlation ID into thread execution context
		workerCtx := domain.WithCorrelationID(context.Background(), correlationID)

		c.logger.Info(workerCtx, "Worker processing message", "worker_id", workerID, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)

		// 2. Process the message wrapping it with retries and DLQ routing
		err := c.processWithRetry(workerCtx, msg, headers, handler)
		if err != nil {
			c.logger.Error(workerCtx, "Permanently failed to process message. Routing to DLQ.", err, "offset", msg.Offset)
			c.metrics.IncOrdersFailed()

			// Route message to DLQ
			dlqErr := c.dlq.PublishDLQ(workerCtx, c.dlqTopic, string(msg.Key), msg.Value, err, headers)
			if dlqErr != nil {
				c.logger.Error(workerCtx, "Failed to route message to DLQ", dlqErr)
				c.metrics.IncKafkaPublishError()
			}
		}

		// 3. Commit the consumer offset.
		// Use a decoupled context to allow commits to succeed even if the main application ctx is cancelled.
		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.reader.CommitMessages(commitCtx, msg); err != nil {
			c.logger.Error(workerCtx, "Failed to commit consumer offset", err, "offset", msg.Offset)
		}
		cancel()
	}
}

func (c *KafkaConsumer) processWithRetry(
	ctx context.Context,
	msg kafka.Message,
	headers map[string]string,
	handler func(ctx context.Context, key []byte, value []byte, headers map[string]string) error,
) error {
	delay := c.retryDelay
	var lastErr error

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			c.logger.Info(ctx, "Retrying Kafka message processing", "attempt", attempt, "delay_ms", delay.Milliseconds(), "error", lastErr.Error())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}

		err := handler(ctx, msg.Key, msg.Value, headers)
		if err == nil {
			return nil
		}

		// Handle unique domain error scenarios:
		// A. Duplicate Event: Already processed by Inbox pattern, mark as successful no-op.
		if err == domain.ErrDuplicateEvent {
			c.logger.Info(ctx, "Duplicate event skipped", "event_id", headers["event_id"])
			c.metrics.IncDuplicateEvent()
			return nil
		}

		// B. Out of stock: Permanent business violation, route straight to DLQ.
		if err == domain.ErrOutOfStock {
			c.logger.Error(ctx, "Business error: Inventory exhausted, routing order to DLQ", err)
			c.metrics.IncInventoryOutOfStock()
			return err
		}

		lastErr = err
	}

	return lastErr
}

// Close closes the underlying Kafka reader connection.
func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
