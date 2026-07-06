package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	cacheAdapter "kafka-demo/internal/adapters/cache"
	consulAdapter "kafka-demo/internal/adapters/consul"
	httpAdapter "kafka-demo/internal/adapters/http"
	kafkaAdapter "kafka-demo/internal/adapters/kafka"
	repoAdapter "kafka-demo/internal/adapters/repository"
	"kafka-demo/internal/config"
	"kafka-demo/internal/domain"
	"kafka-demo/internal/logger"
	"kafka-demo/internal/metrics"
	"kafka-demo/internal/ports"
	"kafka-demo/internal/service"
	"kafka-demo/internal/worker"

	"github.com/google/uuid"
)

func main() {
	// 1. Initialize core system utilities (using fmt.Println for bootstrap logging)
	fmt.Println("Starting service bootstrap...")
	cfg := config.LoadConfig()
	logAdapter := logger.NewSlogLogger()
	metricAdapter := metrics.NewMemoryMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Initialize infrastructure adapters
	repo := repoAdapter.NewInMemoryRepository()
	cache := cacheAdapter.NewRedisCache(cfg.RedisAddress)
	publisher := kafkaAdapter.NewKafkaPublisher(cfg.KafkaBrokers)

	// 2.5 Initialize Service Discovery Registry (Consul or NoOp)
	var registry ports.ServiceRegistry
	if cfg.ConsulEnabled {
		var err error
		registry, err = consulAdapter.NewConsulRegistry(cfg.ConsulAddress)
		if err != nil {
			logAdapter.Error(ctx, "Failed to initialize Consul registry. Falling back to NoOp.", err)
			registry = consulAdapter.NewNoOpRegistry()
		}
	} else {
		registry = consulAdapter.NewNoOpRegistry()
	}

	// 3. Initialize business logic service
	orderService := service.NewOrderService(repo, cache, logAdapter, metricAdapter, cfg.CacheTTL)

	// 4. Initialize background workers & consumers
	outboxWorker := worker.NewOutboxWorker(
		repo,
		publisher,
		logAdapter,
		metricAdapter,
		cfg.KafkaTopic,
		cfg.OutboxPollInterval,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)

	consumer := kafkaAdapter.NewKafkaConsumer(
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.ConsumerGroup,
		logAdapter,
		metricAdapter,
		publisher, // DLQ Publisher
		cfg.DLQTopic,
		cfg.WorkerCount,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)

	// 5. Initialize HTTP router and handlers
	httpHandler := httpAdapter.NewHTTPHandler(orderService)
	mux := http.NewServeMux()
	mux.HandleFunc("/product", httpHandler.ProductHandler)
	mux.HandleFunc("/order", httpHandler.OrderHandler)
	mux.HandleFunc("/health", httpHandler.HealthHandler)

	server := &http.Server{
		Addr:    cfg.HTTPPort,
		Handler: httpAdapter.CorrelationIDMiddleware(mux),
	}

	// 6. Start Outbox background worker polling
	outboxWorker.Start(ctx)

	// 7. Start Kafka Consumer group inside worker pool context
	go func() {
		err := consumer.Consume(ctx, func(workerCtx context.Context, key, value []byte, headers map[string]string) error {
			var event domain.OrderCreatedEvent
			if err := json.Unmarshal(value, &event); err != nil {
				logAdapter.Error(workerCtx, "Failed to deserialize event payload", err, "operation", "ConsumerProcess")
				return domain.ErrInvalidPayload
			}
			return orderService.ProcessOrderCreated(workerCtx, event)
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logAdapter.Error(context.Background(), "Consumer loop exited unexpectedly", err)
		}
	}()

	// 7.5 Register with Service Discovery (Consul)
	serviceID := "order-service-" + uuid.NewString()
	port := 8080
	if hostPortParts := strings.Split(cfg.HTTPPort, ":"); len(hostPortParts) == 2 {
		if p, err := strconv.Atoi(hostPortParts[1]); err == nil {
			port = p
		}
	}

	if err := registry.Register(ctx, serviceID, "order-service", "127.0.0.1", port); err != nil {
		logAdapter.Error(ctx, "Failed to register service with Consul. Continuing in fail-open mode.", err)
	} else {
		logAdapter.Info(ctx, "Service successfully registered with Consul", "service_id", serviceID)
	}

	// 8. Start HTTP Server
	go func() {
		fmt.Printf("HTTP Server listening on port %s\n", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("HTTP Server failed: %v\n", err)
			os.Exit(1)
		}
	}()

	// 9. Coordinate Graceful Shutdown via signal interception
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	fmt.Printf("Interception of signal %v. Initializing graceful shutdown...\n", sig)

	// A. Deregister from Consul first to stop incoming traffic routing
	deregCtx, deregCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := registry.Deregister(deregCtx, serviceID); err != nil {
		fmt.Printf("Consul deregistration failed: %v\n", err)
	} else {
		fmt.Println("Consul deregistration complete.")
	}
	deregCancel()

	// B. Trigger context cancellation for all background processes (Workers/Consumer)
	cancel()

	// C. Shutdown HTTP Server with a tight timeout bounds
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP Server shutdown error: %v\n", err)
	} else {
		fmt.Println("HTTP Server stopped gracefully.")
	}

	// D. Wait for Outbox worker loop to completely drain and exit
	outboxWorker.Wait()
	fmt.Println("Outbox worker stopped gracefully.")

	// E. Safely close physical broker and caching connections
	_ = consumer.Close()
	fmt.Println("Kafka Reader connection closed.")
	
	_ = publisher.Close()
	fmt.Println("Kafka Writer connection closed.")
	
	_ = cache.Close()
	fmt.Println("Redis client connection closed.")

	fmt.Println("Microservice shutdown complete.")
}