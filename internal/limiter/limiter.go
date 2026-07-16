// Package limiter contains the rate-limiting algorithm implementations
// (fixed window, sliding window log/counter, token bucket).
package limiter

import "context"

// Limiter is the interface every rate-limiting algorithm implements.
type Limiter interface {
	// Allow reports whether the request for `key` is permitted, and how
	// many more requests it can make before being blocked.
	Allow(ctx context.Context, key string) (allowed bool, remaining int, err error)
}
