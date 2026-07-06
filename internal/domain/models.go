package domain

import (
	"context"
	"errors"
	"time"
)

// Product represents a stock item in the inventory.
type Product struct {
	Name  string `json:"name"`
	Stock int    `json:"stock"`
}

// OrderStatus defines the lifecycle status of an order.
type OrderStatus string

const (
	OrderStatusPending   OrderStatus = "PENDING"
	OrderStatusCompleted OrderStatus = "COMPLETED"
	OrderStatusFailed    OrderStatus = "FAILED"
)

// Order represents an order placed by a customer.
type Order struct {
	ID        string      `json:"id"`
	Product   string      `json:"product"`
	Qty       int         `json:"qty"`
	Status    OrderStatus `json:"status"`
	CreatedAt time.Time   `json:"created_at"`
}

// OrderCreatedEvent is the event payload published when an order is created.
type OrderCreatedEvent struct {
	EventID       string `json:"event_id"`
	OrderID       string `json:"order_id"`
	Product       string `json:"product"`
	Qty           int    `json:"qty"`
	CorrelationID string `json:"correlation_id"`
}

// OutboxStatus defines the lifecycle of an outbox message.
type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

// OutboxEntry represents a message stored in the database outbox table.
type OutboxEntry struct {
	EventID       string       `json:"event_id"`
	EventType     string       `json:"event_type"`
	Payload       []byte       `json:"payload"`
	Status        OutboxStatus `json:"status"`
	CorrelationID string       `json:"correlation_id"`
	CreatedAt     time.Time    `json:"created_at"`
	PublishedAt   *time.Time   `json:"published_at,omitempty"`
}

// InboxEntry represents an event that has been processed to ensure idempotency.
type InboxEntry struct {
	EventID     string    `json:"event_id"`
	ProcessedAt time.Time `json:"processed_at"`
}

// Typed Errors to avoid magic strings and allow specific error handling behavior
var (
	ErrOutOfStock        = errors.New("inventory: out of stock")
	ErrProductNotFound   = errors.New("inventory: product not found")
	ErrDuplicateEvent    = errors.New("idempotency: duplicate event already processed")
	ErrCacheMiss         = errors.New("cache: key not found")
	ErrPublishFailed     = errors.New("broker: failed to publish message")
	ErrRepositoryFailure = errors.New("database: repository operation failed")
	ErrTransactionActive = errors.New("database: transaction is already active")
	ErrNoTransaction     = errors.New("database: no active transaction")
	ErrInvalidPayload    = errors.New("validation: invalid message payload")
)

type ContextKey string

const CorrelationIDKey ContextKey = "correlation_id"

// GetCorrelationID extracts the correlation ID from context.
func GetCorrelationID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(CorrelationIDKey).(string); ok {
		return val
	}
	return ""
}

// WithCorrelationID returns a new context with the correlation ID.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, CorrelationIDKey, id)
}

// Simple Context interface helper for fetching standard values
type ContextKeyString string

