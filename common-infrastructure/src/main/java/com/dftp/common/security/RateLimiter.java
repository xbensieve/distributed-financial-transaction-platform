package com.dftp.common.security;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe Token Bucket Rate Limiter supporting multiple endpoint categories and identities.
 */
public class RateLimiter {

    private final ConcurrentHashMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public static class TokenBucket {
        private final double capacity;
        private final double refillRatePerSecond;
        private double tokens;
        private final AtomicLong lastRefillTimestampNanos;

        public TokenBucket(double capacity, double refillRatePerSecond) {
            this.capacity = capacity;
            this.refillRatePerSecond = refillRatePerSecond;
            this.tokens = capacity;
            this.lastRefillTimestampNanos = new AtomicLong(System.nanoTime());
        }

        public synchronized boolean tryConsume() {
            refill();
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            return false;
        }

        public synchronized double getAvailableTokens() {
            refill();
            return tokens;
        }

        private void refill() {
            long now = System.nanoTime();
            long last = lastRefillTimestampNanos.get();
            long elapsedNanos = now - last;
            if (elapsedNanos > 0) {
                double addedTokens = (elapsedNanos / 1_000_000_000.0) * refillRatePerSecond;
                tokens = Math.min(capacity, tokens + addedTokens);
                lastRefillTimestampNanos.set(now);
            }
        }
    }

    public boolean tryAcquire(String key, double capacity, double refillRatePerSecond) {
        TokenBucket bucket = buckets.computeIfAbsent(key, k -> new TokenBucket(capacity, refillRatePerSecond));
        return bucket.tryConsume();
    }

    public void clear() {
        buckets.clear();
    }
}
