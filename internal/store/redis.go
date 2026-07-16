// Package store holds the data-layer clients (currently just Redis) that
// the limiter algorithms use to persist counters/state.
package store

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient initializes and pings the Redis connection.
// addr: host:port of the Redis server (e.g. "redis:6379" inside Docker Compose,
// or "localhost:6379" when running outside a container).
// Returns a ready-to-use *redis.Client, or an error if the server couldn't be reached.
func NewRedisClient(addr string) (*redis.Client, error) {
	// NewClient only builds the client struct in memory; it does not
	// actually open a network connection yet (redis-go connects lazily
	// on first command), so we still need the Ping below to fail fast.
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Use a background context (no deadline/cancellation tied to a request,
	// since this only runs once at startup) to verify the connection is alive
	// before handing the client back to callers.
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return client, nil
}