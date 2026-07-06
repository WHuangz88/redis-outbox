package metrics

import "sync/atomic"

// MemoryMetrics implements the ports.Metrics interface using safe atomic counters for verification in tests.
type MemoryMetrics struct {
	ordersCreated       uint64
	ordersFailed        uint64
	inventoryReserved   uint64
	inventoryOutOfStock uint64
	cacheHit            uint64
	cacheMiss           uint64
	duplicateEvent      uint64
	kafkaPublishError   uint64
	kafkaConsumeError   uint64
}

// NewMemoryMetrics creates a thread-safe local metrics tracker.
func NewMemoryMetrics() *MemoryMetrics {
	return &MemoryMetrics{}
}

func (m *MemoryMetrics) IncOrdersCreated() {
	atomic.AddUint64(&m.ordersCreated, 1)
}

func (m *MemoryMetrics) GetOrdersCreated() uint64 {
	return atomic.LoadUint64(&m.ordersCreated)
}

func (m *MemoryMetrics) IncOrdersFailed() {
	atomic.AddUint64(&m.ordersFailed, 1)
}

func (m *MemoryMetrics) GetOrdersFailed() uint64 {
	return atomic.LoadUint64(&m.ordersFailed)
}

func (m *MemoryMetrics) IncInventoryReserved() {
	atomic.AddUint64(&m.inventoryReserved, 1)
}

func (m *MemoryMetrics) GetInventoryReserved() uint64 {
	return atomic.LoadUint64(&m.inventoryReserved)
}

func (m *MemoryMetrics) IncInventoryOutOfStock() {
	atomic.AddUint64(&m.inventoryOutOfStock, 1)
}

func (m *MemoryMetrics) GetInventoryOutOfStock() uint64 {
	return atomic.LoadUint64(&m.inventoryOutOfStock)
}

func (m *MemoryMetrics) IncCacheHit() {
	atomic.AddUint64(&m.cacheHit, 1)
}

func (m *MemoryMetrics) GetCacheHit() uint64 {
	return atomic.LoadUint64(&m.cacheHit)
}

func (m *MemoryMetrics) IncCacheMiss() {
	atomic.AddUint64(&m.cacheMiss, 1)
}

func (m *MemoryMetrics) GetCacheMiss() uint64 {
	return atomic.LoadUint64(&m.cacheMiss)
}

func (m *MemoryMetrics) IncDuplicateEvent() {
	atomic.AddUint64(&m.duplicateEvent, 1)
}

func (m *MemoryMetrics) GetDuplicateEvent() uint64 {
	return atomic.LoadUint64(&m.duplicateEvent)
}

func (m *MemoryMetrics) IncKafkaPublishError() {
	atomic.AddUint64(&m.kafkaPublishError, 1)
}

func (m *MemoryMetrics) GetKafkaPublishError() uint64 {
	return atomic.LoadUint64(&m.kafkaPublishError)
}

func (m *MemoryMetrics) IncKafkaConsumeError() {
	atomic.AddUint64(&m.kafkaConsumeError, 1)
}

func (m *MemoryMetrics) GetKafkaConsumeError() uint64 {
	return atomic.LoadUint64(&m.kafkaConsumeError)
}
