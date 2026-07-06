package ports

import (
	"context"
	"time"

	"kafka-demo/internal/domain"
)

// Repository defines DB actions. It supports explicit transaction handling.
type Repository interface {
	GetInventory(ctx context.Context, name string) (*domain.Product, error)
	ReserveInventory(ctx context.Context, name string, qty int) error
	SaveOrder(ctx context.Context, order *domain.Order) error
	SaveInbox(ctx context.Context, eventID string) error
	SaveOutbox(ctx context.Context, entry *domain.OutboxEntry) error
	GetUnprocessedOutbox(ctx context.Context) ([]*domain.OutboxEntry, error)
	MarkOutboxProcessed(ctx context.Context, eventID string) error
	IsInboxProcessed(ctx context.Context, eventID string) (bool, error)

	// Transaction boundaries
	BeginTx(ctx context.Context) (context.Context, error)
	CommitTx(ctx context.Context) error
	RollbackTx(ctx context.Context) error
}

// Cache defines the cache-aside storage interface.
type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// EventPublisher defines the message broker publishing interface.
type EventPublisher interface {
	Publish(ctx context.Context, topic string, key string, value []byte, headers map[string]string) error
}

// EventConsumer defines the message broker consumption interface.
type EventConsumer interface {
	Consume(ctx context.Context, handler func(ctx context.Context, key []byte, value []byte, headers map[string]string) error) error
}

// Logger defines structured logging behaviors.
type Logger interface {
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, err error, keysAndValues ...interface{})
}

// Metrics defines observability hooks.
type Metrics interface {
	IncOrdersCreated()
	IncOrdersFailed()
	IncInventoryReserved()
	IncInventoryOutOfStock()
	IncCacheHit()
	IncCacheMiss()
	IncDuplicateEvent()
	IncKafkaPublishError()
	IncKafkaConsumeError()
}

// DLQPublisher defines dead letter queue publishing capability.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, topic string, key string, value []byte, err error, headers map[string]string) error
}
