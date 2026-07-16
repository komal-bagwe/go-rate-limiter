package limiter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenBucket allows a burst of up to `capacity` tokens per key, refilling
// continuously at `refillTokens` per `refillWindow`. Each request consumes
// one token; unlike the window algorithms, there's no reset point — refill
// is based on elapsed time since the bucket was last touched.
type TokenBucket struct {
	redisClient  *redis.Client
	capacity     int
	refillTokens int
	refillWindow time.Duration
}

// NewTokenBucket constructs a TokenBucket limiter.
func NewTokenBucket(rdb *redis.Client, capacity, refillTokens int, refillWindow time.Duration) *TokenBucket {
	return &TokenBucket{redisClient: rdb, capacity: capacity, refillTokens: refillTokens, refillWindow: refillWindow}
}

// tokenBucketScript atomically loads the bucket's token count + timestamp
// (initializing a full bucket if unseen), refills based on elapsed time,
// and either consumes a token or reports retry-after.
var tokenBucketScript = redis.NewScript(`
local key            = KEYS[1]
local capacity       = tonumber(ARGV[1])
local refillTokens   = tonumber(ARGV[2])
local refillWindowMs = tonumber(ARGV[3])
local now            = tonumber(ARGV[4])

local bucket     = redis.call("HMGET", key, "tokens", "timestamp")
local tokens     = tonumber(bucket[1])
local lastRefill = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    lastRefill = now
end

local refillRatePerMs = refillTokens / refillWindowMs

local elapsedMs = now - lastRefill
if elapsedMs < 0 then elapsedMs = 0 end

tokens = math.min(capacity, tokens + (elapsedMs * refillRatePerMs))

local allowed      = 0
local retryAfterMs = 0

if tokens >= 1 then
    tokens  = tokens - 1
    allowed = 1
else
    -- No fixed reset point to fall back on, so retry-after is derived
    -- from the refill rate. Guard against a zero rate so we don't divide
    -- by zero; report -1 ("unknown") instead of a bogus wait time.
    if refillRatePerMs > 0 then
        local tokensNeeded = 1 - tokens
        retryAfterMs = math.ceil(tokensNeeded / refillRatePerMs)
    else
        retryAfterMs = -1
    end
end

redis.call("HMSET", key, "tokens", tokens, "timestamp", now)

local ttlMs = capacity / math.max(refillRatePerMs, 0.001)
redis.call("PEXPIRE", key, math.ceil(ttlMs))

if allowed == 1 then
    return {1, math.floor(tokens)}
else
    return {0, 0, retryAfterMs}
end
`)

// Check runs the algorithm for `key` and returns the full Result.
func (tb *TokenBucket) Check(ctx context.Context, key string) (*Result, error) {
	now := time.Now().UnixMilli()
	refillWindowMs := tb.refillWindow.Milliseconds()

	raw, err := tokenBucketScript.Run(ctx, tb.redisClient, []string{key}, tb.capacity, tb.refillTokens, refillWindowMs, now).Result()
	if err != nil {
		return nil, fmt.Errorf("redis token bucket script error: %w", err)
	}

	vals, ok := raw.([]interface{})
	if !ok || len(vals) < 2 {
		return nil, fmt.Errorf("unexpected token bucket script result: %v", raw)
	}

	allowed := vals[0].(int64) == 1
	remaining := int(vals[1].(int64))

	var retryAfter time.Duration
	if len(vals) > 2 {
		// -1 means no standard wait time could be computed; leave retryAfter at zero.
		if retryAfterMs := vals[2].(int64); retryAfterMs >= 0 {
			retryAfter = time.Duration(retryAfterMs) * time.Millisecond
		}
	}

	return &Result{Allowed: allowed, Remaining: remaining, Limit: tb.capacity, RetryAfter: retryAfter}, nil
}

// Allow implements the Limiter interface.
func (tb *TokenBucket) Allow(ctx context.Context, key string) (bool, int, error) {
	result, err := tb.Check(ctx, key)
	if err != nil {
		return false, 0, err
	}
	return result.Allowed, result.Remaining, nil
}
