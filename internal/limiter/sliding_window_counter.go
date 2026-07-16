package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// SlidingWindowCounter approximates SlidingWindowLog cheaply: instead of a
// per-request log, it keeps two integer counters per key (current and
// previous fixed window) and blends them by how far into the current
// window `now` is. O(1) storage per key, at the cost of being an estimate
// rather than an exact count.
type SlidingWindowCounter struct {
	redisClient *redis.Client
	limit       int
	window      time.Duration
}

// NewSlidingWindowCounter constructs a SlidingWindowCounter limiter.
func NewSlidingWindowCounter(rdb *redis.Client, limit int, window time.Duration) *SlidingWindowCounter {
	return &SlidingWindowCounter{redisClient: rdb, limit: limit, window: window}
}

// slidingWindowCounterScript atomically reads the current/previous window
// counters, blends them into an estimate, and either increments the
// current counter (if under `limit`) or reports retry-after.
var slidingWindowCounterScript = redis.NewScript(`
local key      = KEYS[1]
local now      = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
local limit    = tonumber(ARGV[3])

local currentWindowStart  = now - (now % windowMs)
local previousWindowStart = currentWindowStart - windowMs

local currentKey  = key .. ":" .. currentWindowStart
local previousKey = key .. ":" .. previousWindowStart

local currentCount  = tonumber(redis.call("GET", currentKey) or "0")
local previousCount = tonumber(redis.call("GET", previousKey) or "0")

local elapsedFraction  = (now - currentWindowStart) / windowMs
local previousWeight   = 1 - elapsedFraction

local estimate = currentCount + (previousCount * previousWeight)

if estimate < limit then
    redis.call("INCR", currentKey)
    redis.call("PEXPIRE", currentKey, windowMs * 2)

    local remaining = limit - estimate - 1
    if remaining < 0 then remaining = 0 end
    return {1, math.floor(remaining)}
else
    local retryAfterMs = currentWindowStart + windowMs - now
    return {0, 0, retryAfterMs}
end
`)

// Check runs the algorithm for `key` and returns the full Result.
func (swc *SlidingWindowCounter) Check(ctx context.Context, key string) (*Result, error) {
	now := time.Now().UnixMilli()
	windowMs := swc.window.Milliseconds()

	raw, err := slidingWindowCounterScript.Run(ctx, swc.redisClient, []string{key}, now, windowMs, swc.limit).Result()
	if err != nil {
		return nil, fmt.Errorf("redis sliding window counter script error: %w", err)
	}

	vals, ok := raw.([]interface{})
	if !ok || len(vals) < 2 {
		return nil, fmt.Errorf("unexpected sliding window counter script result: %v", raw)
	}

	allowed := vals[0].(int64) == 1
	remaining := int(vals[1].(int64))

	var retryAfter time.Duration
	if len(vals) > 2 {
		retryAfter = time.Duration(vals[2].(int64)) * time.Millisecond
	}

	return &Result{Allowed: allowed, Remaining: remaining, Limit: swc.limit, RetryAfter: retryAfter}, nil
}

// Allow implements the Limiter interface.
func (swc *SlidingWindowCounter) Allow(ctx context.Context, key string) (bool, int, error) {
	result, err := swc.Check(ctx, key)
	if err != nil {
		return false, 0, err
	}
	return result.Allowed, result.Remaining, nil
}
