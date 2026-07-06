package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"kafka-demo/internal/domain"
	"kafka-demo/internal/ports"
)

// OrderService orchestrates the business logic layer.
type OrderService struct {
	repo     ports.Repository
	cache    ports.Cache
	logger   ports.Logger
	metrics  ports.Metrics
	cacheTTL time.Duration
}

// NewOrderService instantiates the OrderService.
func NewOrderService(
	repo ports.Repository,
	cache ports.Cache,
	logger ports.Logger,
	metrics ports.Metrics,
	cacheTTL time.Duration,
) *OrderService {
	return &OrderService{
		repo:     repo,
		cache:    cache,
		logger:   logger,
		metrics:  metrics,
		cacheTTL: cacheTTL,
	}
}

// CreateOrder registers a new order and writes an OrderCreated outbox event within a transaction.
func (s *OrderService) CreateOrder(ctx context.Context, orderID string, productName string, qty int) error {
	s.logger.Info(ctx, "Creating order", "order_id", orderID, "product_id", productName, "qty", qty, "operation", "CreateOrder")
	startTime := time.Now()

	eventID := uuid.NewString()

	// Begin DB Transaction
	txCtx, err := s.repo.BeginTx(ctx)
	if err != nil {
		s.logger.Error(ctx, "Failed to begin transaction", err, "operation", "CreateOrder")
		s.metrics.IncOrdersFailed()
		return err
	}

	// 1. Save Order Model
	order := &domain.Order{
		ID:        orderID,
		Product:   productName,
		Qty:       qty,
		Status:    domain.OrderStatusPending,
		CreatedAt: time.Now(),
	}
	if err := s.repo.SaveOrder(txCtx, order); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to save order", err, "order_id", orderID, "operation", "CreateOrder")
		s.metrics.IncOrdersFailed()
		return err
	}

	// 2. Prepare Outbox Event
	event := domain.OrderCreatedEvent{
		EventID:       eventID,
		OrderID:       orderID,
		Product:       productName,
		Qty:           qty,
		CorrelationID: domain.GetCorrelationID(txCtx),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to marshal order event JSON", err, "order_id", orderID, "operation", "CreateOrder")
		s.metrics.IncOrdersFailed()
		return err
	}

	outboxEntry := &domain.OutboxEntry{
		EventID:       eventID,
		EventType:     "OrderCreated",
		Payload:       payload,
		Status:        domain.OutboxStatusPending,
		CorrelationID: event.CorrelationID,
		CreatedAt:     time.Now(),
	}

	// 3. Save Outbox Event
	if err := s.repo.SaveOutbox(txCtx, outboxEntry); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to save outbox entry", err, "order_id", orderID, "operation", "CreateOrder")
		s.metrics.IncOrdersFailed()
		return err
	}

	// Commit Transaction
	if err := s.repo.CommitTx(txCtx); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to commit order creation transaction", err, "order_id", orderID, "operation", "CreateOrder")
		s.metrics.IncOrdersFailed()
		return err
	}

	s.metrics.IncOrdersCreated()
	s.logger.Info(ctx, "Order created successfully inside transaction",
		"order_id", orderID,
		"event_id", eventID,
		"duration_ms", time.Since(startTime).Milliseconds(),
		"operation", "CreateOrder",
	)

	return nil
}

// ProcessOrderCreated handles asynchronous event processing, updating stock and ensuring idempotency via Inbox.
func (s *OrderService) ProcessOrderCreated(ctx context.Context, event domain.OrderCreatedEvent) error {
	s.logger.Info(ctx, "Processing OrderCreated event",
		"event_id", event.EventID,
		"order_id", event.OrderID,
		"product_id", event.Product,
		"qty", event.Qty,
		"operation", "ProcessOrderCreated",
	)
	startTime := time.Now()

	// 1. Idempotency Check (Inbox Pattern)
	processed, err := s.repo.IsInboxProcessed(ctx, event.EventID)
	if err != nil {
		s.logger.Error(ctx, "Inbox validation check failed", err, "event_id", event.EventID, "operation", "ProcessOrderCreated")
		return err
	}
	if processed {
		s.logger.Info(ctx, "Event already processed, skipping", "event_id", event.EventID, "operation", "ProcessOrderCreated")
		return domain.ErrDuplicateEvent
	}

	// Begin DB Transaction
	txCtx, err := s.repo.BeginTx(ctx)
	if err != nil {
		s.logger.Error(ctx, "Failed to begin processing transaction", err, "event_id", event.EventID, "operation", "ProcessOrderCreated")
		return err
	}

	// 2. Reserve Inventory
	err = s.repo.ReserveInventory(txCtx, event.Product, event.Qty)
	if err != nil {
		if err == domain.ErrOutOfStock {
			// Update order status to failed on out of stock scenario
			s.logger.Error(txCtx, "Product out of stock. Updating order status to FAILED", err, "product_id", event.Product, "operation", "ProcessOrderCreated")
			
			failedOrder := &domain.Order{
				ID:        event.OrderID,
				Product:   event.Product,
				Qty:       event.Qty,
				Status:    domain.OrderStatusFailed,
				CreatedAt: time.Now(),
			}
			_ = s.repo.SaveOrder(txCtx, failedOrder)
			
			// Commit failure status update
			_ = s.repo.CommitTx(txCtx)
			return domain.ErrOutOfStock
		}
		// General database failure, trigger rollback
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to reserve inventory due to database error", err, "product_id", event.Product, "operation", "ProcessOrderCreated")
		return err
	}

	// 3. Mark Order as Completed
	completedOrder := &domain.Order{
		ID:        event.OrderID,
		Product:   event.Product,
		Qty:       event.Qty,
		Status:    domain.OrderStatusCompleted,
		CreatedAt: time.Now(),
	}
	if err := s.repo.SaveOrder(txCtx, completedOrder); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to update order status to COMPLETED", err, "order_id", event.OrderID, "operation", "ProcessOrderCreated")
		return err
	}

	// 4. Save Inbox entry
	if err := s.repo.SaveInbox(txCtx, event.EventID); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to register inbox entry", err, "event_id", event.EventID, "operation", "ProcessOrderCreated")
		return err
	}

	// Commit Transaction
	if err := s.repo.CommitTx(txCtx); err != nil {
		_ = s.repo.RollbackTx(txCtx)
		s.logger.Error(txCtx, "Failed to commit processing transaction", err, "event_id", event.EventID, "operation", "ProcessOrderCreated")
		return err
	}

	// 5. Invalidate Product Cache (Redis Delete after successful database commit)
	// We handle Cache failures gracefully to ensure system remains operational (Cache Aside Resilience)
	if cacheErr := s.cache.Delete(ctx, "product:"+event.Product); cacheErr != nil {
		s.logger.Error(ctx, "Failed to invalidate product cache", cacheErr, "product_id", event.Product, "operation", "ProcessOrderCreated")
	} else {
		s.logger.Info(ctx, "Product cache invalidated", "product_id", event.Product, "operation", "ProcessOrderCreated")
	}

	s.metrics.IncInventoryReserved()
	s.logger.Info(ctx, "Event processed and inventory reserved successfully",
		"event_id", event.EventID,
		"order_id", event.OrderID,
		"duration_ms", time.Since(startTime).Milliseconds(),
		"operation", "ProcessOrderCreated",
	)

	return nil
}

// GetProduct retrieves a product. Implements Cache-Aside pattern (resilient to Redis failures).
func (s *OrderService) GetProduct(ctx context.Context, name string) (*domain.Product, error) {
	s.logger.Info(ctx, "Fetching product details", "product_id", name, "operation", "GetProduct")
	startTime := time.Now()

	// 1. Attempt reading from cache (Redis Get)
	cacheVal, cacheErr := s.cache.Get(ctx, "product:"+name)
	if cacheErr == nil {
		var product domain.Product
		if err := json.Unmarshal([]byte(cacheVal), &product); err == nil {
			s.metrics.IncCacheHit()
			s.logger.Info(ctx, "CACHE HIT", "product_id", name, "stock", product.Stock, "operation", "GetProduct")
			return &product, nil
		}
	}

	if cacheErr != domain.ErrCacheMiss && cacheErr != nil {
		// Log cache failures, but continue to failover to database (High availability)
		s.logger.Error(ctx, "Cache backend error. Failing over to DB repo", cacheErr, "product_id", name, "operation", "GetProduct")
	} else {
		s.logger.Info(ctx, "CACHE MISS", "product_id", name, "operation", "GetProduct")
	}

	// 2. Fetch from Database Repository (Failover)
	product, err := s.repo.GetInventory(ctx, name)
	if err != nil {
		s.logger.Error(ctx, "Failed to fetch product from repository", err, "product_id", name, "operation", "GetProduct")
		return nil, err
	}

	s.metrics.IncCacheMiss()

	// 3. Update Cache asynchronously (or non-blocking) to optimize performance.
	// Gracefully catch failures so they do not impact returning the correct DB data.
	prodBytes, jsonErr := json.Marshal(product)
	if jsonErr == nil {
		if setErr := s.cache.Set(ctx, "product:"+name, string(prodBytes), s.cacheTTL); setErr != nil {
			s.logger.Error(ctx, "Failed to update cache background", setErr, "product_id", name, "operation", "GetProduct")
		} else {
			s.logger.Info(ctx, "First Cache set", "product_id", name, "duration_ms", time.Since(startTime).Milliseconds(), "operation", "GetProduct")
		}
	}

	return product, nil
}
