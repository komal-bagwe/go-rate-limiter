package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// FixedWindow allows up to `limit` requests per key in each fixed
// `window`-length slot (e.g. 5 per 10s), resetting on a clock boundary.
// Trade-off: traffic straddling a window boundary can briefly let through
// up to ~2x `limit`.
type FixedWindow struct {
	redisClient *redis.Client
	limit       int
	window      time.Duration
}

// NewFixedWindow constructs a FixedWindow limiter.
func NewFixedWindow(rdb *redis.Client, limit int, window time.Duration) *FixedWindow {
	return &FixedWindow{redisClient: rdb, limit: limit, window: window}
}

// fixedWindowScript atomically increments the counter, attaches a TTL on
// the first hit of a window, and reports allowed/remaining (or, if
// blocked, the TTL left as retry-after).
var fixedWindowScript = redis.NewScript(`
local key      = KEYS[1]
local limit    = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])

local count = redis.call("INCR", key)
if count == 1 then
    redis.call("PEXPIRE", key, windowMs)
end

if count <= limit then
    return {1, limit - count}
else
    local ttl = redis.call("PTTL", key)
    if ttl < 0 then ttl = 0 end
    return {0, 0, ttl}
end
`)

// Check runs the algorithm for `key` and returns the full Result.
func (fw *FixedWindow) Check(ctx context.Context, key string) (*Result, error) {
	windowMs := fw.window.Milliseconds()

	raw, err := fixedWindowScript.Run(ctx, fw.redisClient, []string{key}, fw.limit, windowMs).Result()
	if err != nil {
		return nil, fmt.Errorf("redis fixed window script error: %w", err)
	}

	vals, ok := raw.([]interface{})
	if !ok || len(vals) < 2 {
		return nil, fmt.Errorf("unexpected fixed window script result: %v", raw)
	}

	allowed := vals[0].(int64) == 1
	remaining := int(vals[1].(int64))

	var retryAfter time.Duration
	if len(vals) > 2 {
		retryAfter = time.Duration(vals[2].(int64)) * time.Millisecond
	}

	return &Result{Allowed: allowed, Remaining: remaining, Limit: fw.limit, RetryAfter: retryAfter}, nil
}

// Allow implements the Limiter interface.
func (fw *FixedWindow) Allow(ctx context.Context, key string) (bool, int, error) {
	result, err := fw.Check(ctx, key)
	if err != nil {
		return false, 0, err
	}
	return result.Allowed, result.Remaining, nil
}
