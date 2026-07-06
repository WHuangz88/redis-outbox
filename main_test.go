package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	httpAdapter "kafka-demo/internal/adapters/http"
	kafkaAdapter "kafka-demo/internal/adapters/kafka"
	repoAdapter "kafka-demo/internal/adapters/repository"
	"kafka-demo/internal/domain"
	"kafka-demo/internal/logger"
	"kafka-demo/internal/metrics"
	"kafka-demo/internal/service"
	"kafka-demo/internal/worker"
)

// ---------------------------------------------------------
// Test Doubles (Mocks and Fakes)
// ---------------------------------------------------------

// FakeCache implements ports.Cache using a local map.
type FakeCache struct {
	mu       sync.Mutex
	store    map[string]string
	failNext bool
}

func NewFakeCache() *FakeCache {
	return &FakeCache{store: make(map[string]string)}
}

func (c *FakeCache) Get(ctx context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext {
		c.failNext = false
		return "", errors.New("simulated cache failure")
	}
	val, ok := c.store[key]
	if !ok {
		return "", domain.ErrCacheMiss
	}
	return val, nil
}

func (c *FakeCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext {
		c.failNext = false
		return errors.New("simulated cache failure")
	}
	c.store[key] = value
	return nil
}

func (c *FakeCache) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext {
		c.failNext = false
		return errors.New("simulated cache failure")
	}
	delete(c.store, key)
	return nil
}

func (c *FakeCache) SimulateFailure(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failNext = fail
}

// FakePublisher implements ports.EventPublisher & DLQPublisher.
type PublishedMsg struct {
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

type FakePublisher struct {
	mu           sync.Mutex
	published    []PublishedMsg
	dlqPublished []PublishedMsg
	failNext     bool
	pubChan      chan PublishedMsg // Allows tests to await publishing without time.Sleep()
}

func NewFakePublisher() *FakePublisher {
	return &FakePublisher{
		pubChan: make(chan PublishedMsg, 100),
	}
}

func (p *FakePublisher) Publish(ctx context.Context, topic string, key string, value []byte, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failNext {
		p.failNext = false
		return domain.ErrPublishFailed
	}
	msg := PublishedMsg{Topic: topic, Key: key, Value: value, Headers: headers}
	p.published = append(p.published, msg)
	select {
	case p.pubChan <- msg:
	default:
	}
	return nil
}

func (p *FakePublisher) PublishDLQ(ctx context.Context, topic string, key string, value []byte, err error, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	dlqHeaders := make(map[string]string)
	for k, v := range headers {
		dlqHeaders[k] = v
	}
	if err != nil {
		dlqHeaders["x-error-message"] = err.Error()
	}
	msg := PublishedMsg{Topic: topic, Key: key, Value: value, Headers: dlqHeaders}
	p.dlqPublished = append(p.dlqPublished, msg)
	return nil
}

func (p *FakePublisher) SimulateFailure(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext = fail
}

func (p *FakePublisher) GetPublished() []PublishedMsg {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := make([]PublishedMsg, len(p.published))
	copy(copied, p.published)
	return copied
}

func (p *FakePublisher) GetDLQPublished() []PublishedMsg {
	p.mu.Lock()
	defer p.mu.Unlock()
	copied := make([]PublishedMsg, len(p.dlqPublished))
	copy(copied, p.dlqPublished)
	return copied
}

// MockKafkaReader implements kafkaReader to feed messages into the consumer.
type MockKafkaReader struct {
	mu        sync.Mutex
	messages  chan kafka.Message
	committed []kafka.Message
	closed    bool
}

func NewMockKafkaReader() *MockKafkaReader {
	return &MockKafkaReader{
		messages: make(chan kafka.Message, 100),
	}
}

func (m *MockKafkaReader) Push(msg kafka.Message) {
	m.messages <- msg
}

func (m *MockKafkaReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case msg, ok := <-m.messages:
		if !ok {
			return kafka.Message{}, io.EOF
		}
		return msg, nil
	}
}

func (m *MockKafkaReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.committed = append(m.committed, msgs...)
	return nil
}

func (m *MockKafkaReader) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.messages)
	}
	return nil
}

func (m *MockKafkaReader) GetCommitted() []kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]kafka.Message, len(m.committed))
	copy(copied, m.committed)
	return copied
}

// ---------------------------------------------------------
// Unit & Integration Test Suite
// ---------------------------------------------------------

func TestOrderLifecycle_HappyPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	publisher := NewFakePublisher()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()

	orderService := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)
	outboxWorker := worker.NewOutboxWorker(repo, publisher, logAdapter, metricAdapter, "orders", 10*time.Millisecond, 2, 10*time.Millisecond)
	
	consumer := kafkaAdapter.NewKafkaConsumer([]string{"test"}, "orders", "group", logAdapter, metricAdapter, publisher, "orders-dlq", 2, 2, 10*time.Millisecond)
	mockReader := NewMockKafkaReader()
	consumer.SetReader(mockReader)

	// 1. Start outbox polling and consumer worker pool
	outboxWorker.Start(ctx)
	
	go func() {
		_ = consumer.Consume(ctx, func(workerCtx context.Context, key, value []byte, headers map[string]string) error {
			var event domain.OrderCreatedEvent
			_ = json.Unmarshal(value, &event)
			return orderService.ProcessOrderCreated(workerCtx, event)
		})
	}()

	// 2. Simulate POST /order call
	reqBody := `{"order_id":"order-1", "product":"iphone", "qty":2}`
	req := httptest.NewRequest(http.MethodPost, "/order", strings.NewReader(reqBody))
	req.Header.Set("X-Request-ID", "trace-id-123")
	rr := httptest.NewRecorder()

	handler := httpAdapter.NewHTTPHandler(orderService)
	httpAdapter.CorrelationIDMiddleware(http.HandlerFunc(handler.OrderHandler)).ServeHTTP(rr, req)

	if rr.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %v", rr.Code)
	}

	// 3. Wait for Outbox Worker to publish to FakePublisher
	var published PublishedMsg
	select {
	case published = <-publisher.pubChan:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for outbox worker to publish event")
	}

	// Validate Correlation ID propagation
	if published.Headers["correlation_id"] != "trace-id-123" {
		t.Errorf("expected correlation ID 'trace-id-123', got '%v'", published.Headers["correlation_id"])
	}

	// 4. Push message to Mock Kafka Reader to trigger consumption
	var kafkaHeaders []kafka.Header
	for k, v := range published.Headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
	}
	mockReader.Push(kafka.Message{
		Topic:   published.Topic,
		Key:     []byte(published.Key),
		Value:   published.Value,
		Headers: kafkaHeaders,
	})

	// Wait for processing to complete in consumer goroutine
	time.Sleep(100 * time.Millisecond) // simple channel sync wait

	// 5. Verify database stock and order state
	prod, err := repo.GetInventory(ctx, "iphone")
	if err != nil {
		t.Fatalf("failed to query inventory: %v", err)
	}
	if prod.Stock != 3 {
		t.Errorf("expected stock to be 3, got %v", prod.Stock)
	}

	// Verify Inbox entry exists
	processed, _ := repo.IsInboxProcessed(ctx, published.Headers["event_id"])
	if !processed {
		t.Error("expected event to be marked processed in Inbox")
	}
}

func TestCacheAsidePattern(t *testing.T) {
	ctx := context.Background()
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()

	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)

	// Test Cache Miss: Cache is empty
	prod, err := svc.GetProduct(ctx, "iphone")
	if err != nil {
		t.Fatal(err)
	}
	if prod.Stock != 5 {
		t.Errorf("expected stock 5, got %d", prod.Stock)
	}
	if metricAdapter.GetCacheMiss() != 1 {
		t.Errorf("expected cache miss, metrics: %v", metricAdapter.GetCacheMiss())
	}
	if metricAdapter.GetCacheHit() != 0 {
		t.Errorf("expected no cache hit, metrics: %v", metricAdapter.GetCacheHit())
	}

	// Cache should now be populated
	cachedVal, err := cache.Get(ctx, "product:iphone")
	if err != nil {
		t.Fatal("expected cached item, but cache missed")
	}
	if !strings.Contains(cachedVal, `"stock":5`) {
		t.Errorf("unexpected cached payload: %v", cachedVal)
	}

	// Test Cache Hit
	prod, err = svc.GetProduct(ctx, "iphone")
	if err != nil {
		t.Fatal(err)
	}
	if metricAdapter.GetCacheHit() != 1 {
		t.Errorf("expected cache hit, metrics: %v", metricAdapter.GetCacheHit())
	}

	// Test Cache Invalidation
	event := domain.OrderCreatedEvent{
		EventID: uuid.NewString(),
		OrderID: uuid.NewString(),
		Product: "iphone",
		Qty:     2,
	}
	err = svc.ProcessOrderCreated(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	// Cache entry should be deleted
	_, err = cache.Get(ctx, "product:iphone")
	if err != domain.ErrCacheMiss {
		t.Errorf("expected CacheMiss after invalidation, got: %v", err)
	}
}

func TestInboxPattern_Idempotency(t *testing.T) {
	ctx := context.Background()
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()

	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)

	event := domain.OrderCreatedEvent{
		EventID: "same-event-id",
		OrderID: "order-123",
		Product: "iphone",
		Qty:     1,
	}

	// First execution
	err := svc.ProcessOrderCreated(ctx, event)
	if err != nil {
		t.Fatal(err)
	}

	// Second execution (duplicate)
	err = svc.ProcessOrderCreated(ctx, event)
	if err != domain.ErrDuplicateEvent {
		t.Errorf("expected ErrDuplicateEvent, got %v", err)
	}

	// Confirm inventory deducted only once (stock: 5 -> 4)
	prod, _ := repo.GetInventory(ctx, "iphone")
	if prod.Stock != 4 {
		t.Errorf("expected stock 4, got %v", prod.Stock)
	}
}

func TestHTTPHandler_Validation(t *testing.T) {
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()
	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)
	handler := httpAdapter.NewHTTPHandler(svc)

	// Test Invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/order", strings.NewReader(`{invalid json`))
	rr := httptest.NewRecorder()
	handler.OrderHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %v", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Invalid JSON payload") {
		t.Errorf("unexpected error body: %v", rr.Body.String())
	}
}

func TestInventoryOutOfStock(t *testing.T) {
	ctx := context.Background()
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()
	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)

	event := domain.OrderCreatedEvent{
		EventID: uuid.NewString(),
		OrderID: uuid.NewString(),
		Product: "iphone",
		Qty:     10, // Stock is only 5
	}

	err := svc.ProcessOrderCreated(ctx, event)
	if err != domain.ErrOutOfStock {
		t.Errorf("expected ErrOutOfStock, got %v", err)
	}
}

func TestConcurrentOrders_RaceSafety(t *testing.T) {
	ctx := context.Background()
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()
	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)

	// Seed 10 iphone stock
	_ = repo.ReserveInventory(ctx, "iphone", -5) // adds stock back up to 10

	var wg sync.WaitGroup
	workers := 10

	// Deduct 1 stock concurrently 10 times
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			event := domain.OrderCreatedEvent{
				EventID: uuid.NewString(),
				OrderID: uuid.NewString(),
				Product: "iphone",
				Qty:     1,
			}
			_ = svc.ProcessOrderCreated(ctx, event)
		}(i)
	}

	wg.Wait()

	// Verify final stock is exactly 0
	prod, _ := repo.GetInventory(ctx, "iphone")
	if prod.Stock != 0 {
		t.Errorf("expected stock 0, got %v", prod.Stock)
	}
}

func TestConsumer_WorkerPool_GracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()
	publisher := NewFakePublisher()
	
	consumer := kafkaAdapter.NewKafkaConsumer([]string{"test"}, "orders", "group", logAdapter, metricAdapter, publisher, "orders-dlq", 3, 0, 1*time.Millisecond)
	mockReader := NewMockKafkaReader()
	consumer.SetReader(mockReader)

	// Push 3 messages to reader
	for i := 0; i < 3; i++ {
		mockReader.Push(kafka.Message{
			Key:   []byte(uuid.NewString()),
			Value: []byte(`{"event_id":"abc"}`),
		})
	}

	var processedCount int64
	var mu sync.Mutex

	// Start consumer in background
	go func() {
		_ = consumer.Consume(ctx, func(workerCtx context.Context, key, value []byte, headers map[string]string) error {
			mu.Lock()
			processedCount++
			mu.Unlock()
			return nil
		})
	}()

	// Wait briefly for worker pool to consume
	time.Sleep(50 * time.Millisecond)

	// Cancel context to trigger graceful shutdown
	cancel()
	time.Sleep(50 * time.Millisecond)

	// Verify offsets committed matches processed messages
	mu.Lock()
	if processedCount != 3 {
		t.Errorf("expected 3 processed events, got %d", processedCount)
	}
	mu.Unlock()
}

func TestConsumer_Retry_And_DLQ(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()
	publisher := NewFakePublisher()
	
	// Max retries = 2
	consumer := kafkaAdapter.NewKafkaConsumer([]string{"test"}, "orders", "group", logAdapter, metricAdapter, publisher, "orders-dlq", 1, 2, 5*time.Millisecond)
	mockReader := NewMockKafkaReader()
	consumer.SetReader(mockReader)

	mockReader.Push(kafka.Message{
		Key:   []byte("test-key"),
		Value: []byte("test-payload"),
	})

	var attemptCount int
	var mu sync.Mutex

	go func() {
		_ = consumer.Consume(ctx, func(workerCtx context.Context, key, value []byte, headers map[string]string) error {
			mu.Lock()
			attemptCount++
			mu.Unlock()
			return errors.New("transient database connection error")
		})
	}()

	// Wait for consumer loop to retry and route to DLQ
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	// Initial try + 2 retries = 3 attempts
	if attemptCount != 3 {
		t.Errorf("expected 3 execution attempts, got %d", attemptCount)
	}
	mu.Unlock()

	// Check if message lands in DLQ
	dlqMsgs := publisher.GetDLQPublished()
	if len(dlqMsgs) != 1 {
		t.Fatalf("expected 1 message in DLQ, got %d", len(dlqMsgs))
	}

	if dlqMsgs[0].Headers["x-error-message"] != "transient database connection error" {
		t.Errorf("unexpected DLQ error header: %v", dlqMsgs[0].Headers["x-error-message"])
	}
}

func TestInfrastructureResilience_Failures(t *testing.T) {
	ctx := context.Background()
	repo := repoAdapter.NewInMemoryRepository()
	cache := NewFakeCache()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()

	svc := service.NewOrderService(repo, cache, logAdapter, metricAdapter, 1*time.Minute)

	// 1. Simulate Repository Failure
	repo.SimulateFailure(true)
	err := svc.CreateOrder(ctx, "o-123", "iphone", 1)
	if err != domain.ErrRepositoryFailure {
		t.Errorf("expected ErrRepositoryFailure, got %v", err)
	}

	// 2. Simulate Redis/Cache Failure during product fetch (Cache aside should failopen to repository)
	cache.SimulateFailure(true)
	prod, err := svc.GetProduct(ctx, "iphone")
	if err != nil {
		t.Fatalf("expected GetProduct to succeed by falling back to DB repo during Redis failure, got err: %v", err)
	}
	if prod.Stock != 5 {
		t.Errorf("expected stock 5, got %v", prod.Stock)
	}
}
