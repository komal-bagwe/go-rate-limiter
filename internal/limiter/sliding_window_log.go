package limiter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// SlidingWindowLog allows up to `limit` requests per key inside any
// rolling `window`-length span, tracked exactly via a per-request log
// (a Redis sorted set). More precise than FixedWindow, at the cost of
// storing one entry per request instead of a single counter.
type SlidingWindowLog struct {
	redisClient *redis.Client
	limit       int
	window      time.Duration
}

// NewSlidingWindowLog constructs a SlidingWindowLog limiter.
func NewSlidingWindowLog(rdb *redis.Client, limit int, window time.Duration) *SlidingWindowLog {
	return &SlidingWindowLog{redisClient: rdb, limit: limit, window: window}
}

// requestSeq disambiguates log entries that land on the same millisecond.
var requestSeq uint64

// slidingWindowLogScript atomically evicts entries older than the window,
// counts what's left, and either logs the new request (if under `limit`)
// or reports how long until the oldest entry ages out.
var slidingWindowLogScript = redis.NewScript(`
local key        = KEYS[1]
local now        = tonumber(ARGV[1])
local windowMs    = tonumber(ARGV[2])
local limit       = tonumber(ARGV[3])
local member      = ARGV[4]

local windowStart = now - windowMs

redis.call("ZREMRANGEBYSCORE", key, "-inf", windowStart)

local count = redis.call("ZCARD", key)

if count < limit then
    redis.call("ZADD", key, now, member)
    redis.call("PEXPIRE", key, windowMs)
    return {1, limit - count - 1}
else
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local retryAfterMs = 0
    if oldest[2] ~= nil then
        retryAfterMs = (tonumber(oldest[2]) + windowMs) - now
    end
    return {0, 0, retryAfterMs}
end
`)

// Result carries the full outcome of a rate-limit check: allowed/blocked,
// remaining requests, and (if blocked) retry-after.
type Result struct {
	Allowed    bool
	Remaining  int
	Limit      int
	RetryAfter time.Duration
}

// Check runs the algorithm for `key` and returns the full Result.
func (sw *SlidingWindowLog) Check(ctx context.Context, key string) (*Result, error) {
	now := time.Now().UnixMilli()
	windowMs := sw.window.Milliseconds()

	// Unique member per request: timestamp + monotonic counter, so
	// concurrent requests never collide in the sorted set.
	seq := atomic.AddUint64(&requestSeq, 1)
	member := fmt.Sprintf("%d-%d", now, seq)

	raw, err := slidingWindowLogScript.Run(ctx, sw.redisClient, []string{key}, now, windowMs, sw.limit, member).Result()
	if err != nil {
		return nil, fmt.Errorf("redis sliding window script error: %w", err)
	}

	vals, ok := raw.([]interface{})
	if !ok || len(vals) < 2 {
		return nil, fmt.Errorf("unexpected sliding window script result: %v", raw)
	}

	allowed := vals[0].(int64) == 1
	remaining := int(vals[1].(int64))

	var retryAfter time.Duration
	if len(vals) > 2 {
		retryAfter = time.Duration(vals[2].(int64)) * time.Millisecond
	}

	return &Result{Allowed: allowed, Remaining: remaining, Limit: sw.limit, RetryAfter: retryAfter}, nil
}

// Allow implements the Limiter interface.
func (sw *SlidingWindowLog) Allow(ctx context.Context, key string) (bool, int, error) {
	result, err := sw.Check(ctx, key)
	if err != nil {
		return false, 0, err
	}
	return result.Allowed, result.Remaining, nil
}
