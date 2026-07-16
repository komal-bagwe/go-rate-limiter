// Command server wires together the HTTP server, the Redis-backed store,
// and a rate limiter, then exposes a demo "/test" endpoint protected by it.
package main

import (
	"fmt"
	"net/http"
	"time"

	"rate-limiter/internal/limiter"
	"rate-limiter/internal/store"
)

// rateLimitMiddleware wraps `next` with a rate-limit check via `l`.
func rateLimitMiddleware(l limiter.Limiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: hardcoded for the demo; derive per-request in a real app
		// (JWT claim, API key header, r.RemoteAddr, ...).
		userID := "user_123"

		allowed, remaining, err := l.Allow(r.Context(), userID)
		if err != nil || !allowed {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, "❌ Too many requests. Please slow down.")
			return
		}

		w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", remaining))
		next(w, r)
	}
}

func main() {
	// "redis" is the Docker Compose service hostname (see docker-compose.yaml).
	rdb, _ := store.NewRedisClient("redis:6379")

	// All limiters below satisfy limiter.Limiter — swap the active one by
	// commenting/uncommenting; nothing else in this file changes.

	// myLimiter := limiter.NewFixedWindow(rdb, 5, 10*time.Second)
	// myLimiter := limiter.NewSlidingWindowLog(rdb, 5, 10*time.Second)
	// myLimiter := limiter.NewSlidingWindowCounter(rdb, 5, 10*time.Second)
	myLimiter := limiter.NewTokenBucket(rdb, 50, 5, 10*time.Second)

	http.HandleFunc("/test", rateLimitMiddleware(myLimiter, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "✅ You reached the protected endpoint! Attempts left: %s", w.Header().Get("X-RateLimit-Remaining"))
	}))

	fmt.Println("🚀 Server running on :8080...")
	http.ListenAndServe(":8080", nil)
}