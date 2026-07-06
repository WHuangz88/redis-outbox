package worker

import (
	"context"
	"sync"
	"time"

	"kafka-demo/internal/domain"
	"kafka-demo/internal/ports"
)

// OutboxWorker handles background polling of outbox messages, publishing them to the broker.
type OutboxWorker struct {
	repo         ports.Repository
	publisher    ports.EventPublisher
	logger       ports.Logger
	metrics      ports.Metrics
	topic        string
	pollInterval time.Duration
	maxRetries   int
	retryDelay   time.Duration
	wg           sync.WaitGroup
}

// NewOutboxWorker initializes the background Outbox polling worker.
func NewOutboxWorker(
	repo ports.Repository,
	publisher ports.EventPublisher,
	logger ports.Logger,
	metrics ports.Metrics,
	topic string,
	pollInterval time.Duration,
	maxRetries int,
	retryDelay time.Duration,
) *OutboxWorker {
	return &OutboxWorker{
		repo:         repo,
		publisher:    publisher,
		logger:       logger,
		metrics:      metrics,
		topic:        topic,
		pollInterval: pollInterval,
		maxRetries:   maxRetries,
		retryDelay:   retryDelay,
	}
}

// Start launches the Outbox background loop, processing items until the context is cancelled.
func (w *OutboxWorker) Start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		ticker := time.NewTicker(w.pollInterval)
		defer ticker.Stop()

		w.logger.Info(ctx, "Outbox background worker started", "poll_interval", w.pollInterval)

		for {
			select {
			case <-ctx.Done():
				w.logger.Info(ctx, "Outbox worker stopping background polling...")
				return
			case <-ticker.C:
				w.processOutbox(ctx)
			}
		}
	}()
}

func (w *OutboxWorker) processOutbox(ctx context.Context) {
	// 1. Fetch pending outbox rows
	entries, err := w.repo.GetUnprocessedOutbox(ctx)
	if err != nil {
		w.logger.Error(ctx, "Outbox worker: failed to fetch pending events", err)
		return
	}

	for _, entry := range entries {
		// Decouple context for tracing so logs are correlation aware
		entryCtx := domain.WithCorrelationID(ctx, entry.CorrelationID)

		w.logger.Info(entryCtx, "Outbox worker: found pending event, publishing to Kafka",
			"event_id", entry.EventID,
			"event_type", entry.EventType,
		)

		headers := map[string]string{
			"correlation_id": entry.CorrelationID,
			"event_id":       entry.EventID,
			"event_type":     entry.EventType,
		}

		// 2. Publish to Kafka with Retry Backoff Strategy
		err = w.publishWithRetry(entryCtx, entry, headers)
		if err != nil {
			w.logger.Error(entryCtx, "Outbox worker: permanently failed to publish event. Backing off.", err, "event_id", entry.EventID)
			w.metrics.IncKafkaPublishError()
			continue
		}

		// 3. Mark Outbox event as published inside transaction bounds
		txCtx, txErr := w.repo.BeginTx(entryCtx)
		if txErr != nil {
			w.logger.Error(entryCtx, "Outbox worker: failed to begin database tx", txErr)
			continue
		}

		if markErr := w.repo.MarkOutboxProcessed(txCtx, entry.EventID); markErr != nil {
			_ = w.repo.RollbackTx(txCtx)
			w.logger.Error(entryCtx, "Outbox worker: failed to mark outbox row as processed", markErr)
			continue
		}

		if commitErr := w.repo.CommitTx(txCtx); commitErr != nil {
			_ = w.repo.RollbackTx(txCtx)
			w.logger.Error(entryCtx, "Outbox worker: failed to commit database tx", commitErr)
			continue
		}

		w.logger.Info(entryCtx, "Outbox worker: message successfully marked as published in database", "event_id", entry.EventID)
	}
}

func (w *OutboxWorker) publishWithRetry(ctx context.Context, entry *domain.OutboxEntry, headers map[string]string) error {
	delay := w.retryDelay
	var lastErr error

	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if attempt > 0 {
			w.logger.Info(ctx, "Outbox worker retrying Kafka publish", "attempt", attempt, "delay_ms", delay.Milliseconds(), "error", lastErr.Error())
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2 // Double the interval for exponential backoff
		}

		err := w.publisher.Publish(ctx, w.topic, entry.EventID, entry.Payload, headers)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return lastErr
}

// Wait blocks until the outbox worker has gracefully exited.
func (w *OutboxWorker) Wait() {
	w.wg.Wait()
}
