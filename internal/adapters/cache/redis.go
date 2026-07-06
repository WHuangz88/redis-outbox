package cache

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"kafka-demo/internal/domain"
)

// RedisCache wraps a Redis client to implement ports.Cache.
type RedisCache struct {
	client   *redis.Client
	failNext bool
	mu       sync.Mutex
}

// NewRedisCache creates a wrapper around the go-redis client.
func NewRedisCache(addr string) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

// SimulateFailure triggers a simulated error on the next cache invocation.
func (c *RedisCache) SimulateFailure(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failNext = fail
}

func (c *RedisCache) checkFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failNext {
		c.failNext = false
		return domain.ErrRepositoryFailure // Represents a connection failure
	}
	return nil
}

// Get reads from Redis, mapping redis.Nil to ErrCacheMiss.
func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	if err := c.checkFailure(); err != nil {
		return "", err
	}
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", domain.ErrCacheMiss
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set writes a key-value pair to Redis with a TTL.
func (c *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	if err := c.checkFailure(); err != nil {
		return err
	}
	return c.client.Set(ctx, key, value, ttl).Err()
}

// Delete invalidates a key in Redis.
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if err := c.checkFailure(); err != nil {
		return err
	}
	return c.client.Del(ctx, key).Err()
}

// Close releases the Redis client connection.
func (c *RedisCache) Close() error {
	return c.client.Close()
}
