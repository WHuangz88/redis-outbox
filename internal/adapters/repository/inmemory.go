package repository

import (
	"context"
	"sync"
	"time"

	"kafka-demo/internal/domain"
)

type txContextKey struct{}

var txKey = txContextKey{}

// txState represents a snapshot of the in-memory state during a transaction.
type txState struct {
	products map[string]*domain.Product
	orders   map[string]*domain.Order
	inbox    map[string]*domain.InboxEntry
	outbox   map[string]*domain.OutboxEntry
}

// InMemoryRepository implements ports.Repository.
type InMemoryRepository struct {
	mu       sync.RWMutex
	products map[string]*domain.Product
	orders   map[string]*domain.Order
	inbox    map[string]*domain.InboxEntry
	outbox   map[string]*domain.OutboxEntry
	failNext bool
	txMu     sync.Mutex
}

// NewInMemoryRepository instantiates the repository and seeds default inventory.
func NewInMemoryRepository() *InMemoryRepository {
	repo := &InMemoryRepository{
		products: make(map[string]*domain.Product),
		orders:   make(map[string]*domain.Order),
		inbox:    make(map[string]*domain.InboxEntry),
		outbox:   make(map[string]*domain.OutboxEntry),
	}

	// Seed products (matching the original requirements)
	repo.products["iphone"] = &domain.Product{
		Name:  "iphone",
		Stock: 5,
	}

	return repo
}

// SimulateFailure toggles a one-time database failure for error paths testing.
func (r *InMemoryRepository) SimulateFailure(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failNext = fail
}

func (r *InMemoryRepository) checkFailure() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failNext {
		r.failNext = false // Auto-reset after triggered
		return domain.ErrRepositoryFailure
	}
	return nil
}

// BeginTx returns a new context containing a clone of the database state.
func (r *InMemoryRepository) BeginTx(ctx context.Context) (context.Context, error) {
	if err := r.checkFailure(); err != nil {
		return nil, err
	}
	if ctx.Value(txKey) != nil {
		return nil, domain.ErrTransactionActive
	}

	// Serialize transactions to avoid concurrency-related write skew
	r.txMu.Lock()

	r.mu.RLock()
	state := r.cloneState()
	r.mu.RUnlock()

	return context.WithValue(ctx, txKey, state), nil
}

// CommitTx updates the main database state with the transactional changes.
func (r *InMemoryRepository) CommitTx(ctx context.Context) error {
	stateVal := ctx.Value(txKey)
	if stateVal == nil {
		return domain.ErrNoTransaction
	}
	state := stateVal.(*txState)

	r.mu.Lock()
	r.products = state.products
	r.orders = state.orders
	r.inbox = state.inbox
	r.outbox = state.outbox
	r.mu.Unlock()

	r.txMu.Unlock()

	// Check failure after releasing the lock to prevent deadlocks on simulated failures
	if err := r.checkFailure(); err != nil {
		return err
	}

	return nil
}

// RollbackTx simply releases the transaction state without persisting changes.
func (r *InMemoryRepository) RollbackTx(ctx context.Context) error {
	stateVal := ctx.Value(txKey)
	if stateVal == nil {
		return domain.ErrNoTransaction
	}
	
	r.txMu.Unlock()

	// Check failure after releasing the lock
	if err := r.checkFailure(); err != nil {
		return err
	}

	return nil
}

// GetInventory fetches inventory item from the database.
func (r *InMemoryRepository) GetInventory(ctx context.Context, name string) (*domain.Product, error) {
	if err := r.checkFailure(); err != nil {
		return nil, err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		p, ok := state.products[name]
		if !ok {
			return nil, domain.ErrProductNotFound
		}
		return &domain.Product{Name: p.Name, Stock: p.Stock}, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.products[name]
	if !ok {
		return nil, domain.ErrProductNotFound
	}
	return &domain.Product{Name: p.Name, Stock: p.Stock}, nil
}

// ReserveInventory atomically decrements product stock or returns ErrOutOfStock.
func (r *InMemoryRepository) ReserveInventory(ctx context.Context, name string, qty int) error {
	if err := r.checkFailure(); err != nil {
		return err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		p, ok := state.products[name]
		if !ok {
			return domain.ErrProductNotFound
		}
		if p.Stock < qty {
			return domain.ErrOutOfStock
		}
		p.Stock -= qty
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.products[name]
	if !ok {
		return domain.ErrProductNotFound
	}
	if p.Stock < qty {
		return domain.ErrOutOfStock
	}
	p.Stock -= qty
	return nil
}

// SaveOrder inserts an order record.
func (r *InMemoryRepository) SaveOrder(ctx context.Context, order *domain.Order) error {
	if err := r.checkFailure(); err != nil {
		return err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		state.orders[order.ID] = order
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.orders[order.ID] = order
	return nil
}

// SaveInbox records event processing for duplicate safety.
func (r *InMemoryRepository) SaveInbox(ctx context.Context, eventID string) error {
	if err := r.checkFailure(); err != nil {
		return err
	}
	entry := &domain.InboxEntry{
		EventID:     eventID,
		ProcessedAt: time.Now(),
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		state.inbox[eventID] = entry
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.inbox[eventID] = entry
	return nil
}

// SaveOutbox inserts a message to be published asynchronously.
func (r *InMemoryRepository) SaveOutbox(ctx context.Context, entry *domain.OutboxEntry) error {
	if err := r.checkFailure(); err != nil {
		return err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		state.outbox[entry.EventID] = entry
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.outbox[entry.EventID] = entry
	return nil
}

// GetUnprocessedOutbox fetches all pending outbox entries.
func (r *InMemoryRepository) GetUnprocessedOutbox(ctx context.Context) ([]*domain.OutboxEntry, error) {
	if err := r.checkFailure(); err != nil {
		return nil, err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		var list []*domain.OutboxEntry
		for _, e := range state.outbox {
			if e.Status == domain.OutboxStatusPending {
				list = append(list, e)
			}
		}
		return list, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []*domain.OutboxEntry
	for _, e := range r.outbox {
		if e.Status == domain.OutboxStatusPending {
			list = append(list, e)
		}
	}
	return list, nil
}

// MarkOutboxProcessed marks a message as completed after publishing.
func (r *InMemoryRepository) MarkOutboxProcessed(ctx context.Context, eventID string) error {
	if err := r.checkFailure(); err != nil {
		return err
	}
	now := time.Now()
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		e, ok := state.outbox[eventID]
		if !ok {
			return domain.ErrRepositoryFailure
		}
		e.Status = domain.OutboxStatusPublished
		e.PublishedAt = &now
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.outbox[eventID]
	if !ok {
		return domain.ErrRepositoryFailure
	}
	e.Status = domain.OutboxStatusPublished
	e.PublishedAt = &now
	return nil
}

// IsInboxProcessed returns whether an event has been processed.
func (r *InMemoryRepository) IsInboxProcessed(ctx context.Context, eventID string) (bool, error) {
	if err := r.checkFailure(); err != nil {
		return false, err
	}
	if stateVal := ctx.Value(txKey); stateVal != nil {
		state := stateVal.(*txState)
		_, ok := state.inbox[eventID]
		return ok, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.inbox[eventID]
	return ok, nil
}

func (r *InMemoryRepository) cloneState() *txState {
	clone := &txState{
		products: make(map[string]*domain.Product),
		orders:   make(map[string]*domain.Order),
		inbox:    make(map[string]*domain.InboxEntry),
		outbox:   make(map[string]*domain.OutboxEntry),
	}

	for k, v := range r.products {
		clone.products[k] = &domain.Product{
			Name:  v.Name,
			Stock: v.Stock,
		}
	}
	for k, v := range r.orders {
		clone.orders[k] = &domain.Order{
			ID:        v.ID,
			Product:   v.Product,
			Qty:       v.Qty,
			Status:    v.Status,
			CreatedAt: v.CreatedAt,
		}
	}
	for k, v := range r.inbox {
		clone.inbox[k] = &domain.InboxEntry{
			EventID:     v.EventID,
			ProcessedAt: v.ProcessedAt,
		}
	}
	for k, v := range r.outbox {
		clone.outbox[k] = &domain.OutboxEntry{
			EventID:       v.EventID,
			EventType:     v.EventType,
			Payload:       append([]byte(nil), v.Payload...),
			Status:        v.Status,
			CorrelationID: v.CorrelationID,
			CreatedAt:     v.CreatedAt,
			PublishedAt:   v.PublishedAt,
		}
	}
	return clone
}
