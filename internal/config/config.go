package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all config properties of the microservice.
type Config struct {
	KafkaBrokers       []string
	KafkaTopic         string
	ConsumerGroup      string
	RedisAddress       string
	HTTPPort           string
	MaxRetries         int
	RetryDelay         time.Duration
	WorkerCount        int
	CacheTTL           time.Duration
	OutboxPollInterval time.Duration
	DLQTopic           string
}

// LoadConfig fetches configuration from environment variables with sensible defaults.
func LoadConfig() *Config {
	return &Config{
		KafkaBrokers:       getEnvAsSlice("KAFKA_BROKERS", []string{"localhost:9092"}),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "orders"),
		ConsumerGroup:      getEnv("CONSUMER_GROUP", "inventory"),
		RedisAddress:       getEnv("REDIS_ADDRESS", "localhost:6379"),
		HTTPPort:           getEnv("HTTP_PORT", ":8080"),
		MaxRetries:         getEnvAsInt("MAX_RETRIES", 3),
		RetryDelay:         getEnvAsDuration("RETRY_DELAY", 100*time.Millisecond),
		WorkerCount:        getEnvAsInt("WORKER_COUNT", 3),
		CacheTTL:           getEnvAsDuration("CACHE_TTL", 5*time.Minute),
		OutboxPollInterval: getEnvAsDuration("OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
		DLQTopic:           getEnv("DLQ_TOPIC", "orders-dlq"),
	}
}

func getEnv(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvAsSlice(key string, defaultVal []string) []string {
	if value, exists := os.LookupEnv(key); exists {
		return []string{value} // Simplistic slice split could be added, but single endpoint suffices for simple setup
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valueStr := getEnv(key, "")
	if value, err := strconv.Atoi(valueStr); err == nil {
		return value
	}
	return defaultVal
}

func getEnvAsDuration(key string, defaultVal time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if value, err := time.ParseDuration(valueStr); err == nil {
		return value
	}
	return defaultVal
}
