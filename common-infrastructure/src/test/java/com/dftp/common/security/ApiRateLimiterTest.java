package com.dftp.common.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ApiRateLimiterTest {

    private RateLimiter rateLimiter;
    private RateLimitingFilter rateLimitingFilter;

    @BeforeEach
    void setUp() {
        rateLimiter = new RateLimiter();
        rateLimitingFilter = new RateLimitingFilter();
        // Configure low capacities for deterministic unit testing
        ReflectionTestUtils.setField(rateLimitingFilter, "adminCapacity", 3.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "adminRefill", 1.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "txMutationCapacity", 5.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "txMutationRefill", 2.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "accMutationCapacity", 5.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "accMutationRefill", 2.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "readCapacity", 10.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "readRefill", 5.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "defaultCapacity", 10.0);
        ReflectionTestUtils.setField(rateLimitingFilter, "defaultRefill", 5.0);
    }

    @Test
    @DisplayName("RATE-01: RateLimiter permits requests up to capacity and rejects when exhausted")
    void testRateLimiterPermitsUpToCapacity() {
        String key = "test-key-1";
        double capacity = 3.0;
        double refillRate = 1.0;

        assertThat(rateLimiter.tryAcquire(key, capacity, refillRate)).isTrue();
        assertThat(rateLimiter.tryAcquire(key, capacity, refillRate)).isTrue();
        assertThat(rateLimiter.tryAcquire(key, capacity, refillRate)).isTrue();

        // 4th request must be rejected
        assertThat(rateLimiter.tryAcquire(key, capacity, refillRate)).isFalse();
    }

    @Test
    @DisplayName("RATE-02: Concurrent flood test — strictly enforces token capacity across competing threads")
    void testConcurrentFlooding() throws Exception {
        int threads = 20;
        double capacity = 5.0;
        double refill = 0.0; // no refill during instant test
        String key = "flood-key";

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch readyLatch = new CountDownLatch(threads);
        CountDownLatch startLatch = new CountDownLatch(1);

        AtomicInteger permitted = new AtomicInteger(0);
        AtomicInteger rejected = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    if (rateLimiter.tryAcquire(key, capacity, refill)) {
                        permitted.incrementAndGet();
                    } else {
                        rejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }

        readyLatch.await(3, TimeUnit.SECONDS);
        startLatch.countDown();
        executor.shutdown();
        assertThat(executor.awaitTermination(3, TimeUnit.SECONDS)).isTrue();

        assertThat(permitted.get()).isEqualTo(5);
        assertThat(rejected.get()).isEqualTo(15);
    }

    @Test
    @DisplayName("RATE-03: RateLimitingFilter blocks POST /transactions flood with HTTP 429 and does NOT invoke FilterChain")
    void testFilterRejectsTxMutationFlood() throws ServletException, IOException {
        FilterChain filterChain = mock(FilterChain.class);

        // First 5 requests must pass through
        for (int i = 0; i < 5; i++) {
            MockHttpServletRequest req = new MockHttpServletRequest("POST", "/transactions");
            req.setRemoteAddr("192.168.1.100");
            MockHttpServletResponse res = new MockHttpServletResponse();

            rateLimitingFilter.doFilter(req, res, filterChain);
            assertThat(res.getStatus()).isEqualTo(200);
        }

        verify(filterChain, times(5)).doFilter(any(HttpServletRequest.class), any(HttpServletResponse.class));

        // 6th request must be rejected with 429 Too Many Requests
        MockHttpServletRequest floodReq = new MockHttpServletRequest("POST", "/transactions");
        floodReq.setRemoteAddr("192.168.1.100");
        MockHttpServletResponse floodRes = new MockHttpServletResponse();

        rateLimitingFilter.doFilter(floodReq, floodRes, filterChain);

        assertThat(floodRes.getStatus()).isEqualTo(429);
        assertThat(floodRes.getHeader("Retry-After")).isEqualTo("1");
        assertThat(floodRes.getHeader("X-RateLimit-Limit")).isEqualTo("5");
        assertThat(floodRes.getHeader("X-RateLimit-Remaining")).isEqualTo("0");
        assertThat(floodRes.getContentAsString()).contains("API rate limit exceeded");

        // Filter chain must STILL have been invoked only 5 times — no 6th invocation!
        verify(filterChain, times(5)).doFilter(any(HttpServletRequest.class), any(HttpServletResponse.class));
    }

    @Test
    @DisplayName("RATE-04: Actuator health endpoints are excluded from rate limiting")
    void testActuatorEndpointsExempt() throws ServletException, IOException {
        FilterChain filterChain = mock(FilterChain.class);

        for (int i = 0; i < 20; i++) {
            MockHttpServletRequest req = new MockHttpServletRequest("GET", "/actuator/health");
            req.setRemoteAddr("10.0.0.1");
            MockHttpServletResponse res = new MockHttpServletResponse();

            rateLimitingFilter.doFilter(req, res, filterChain);
            assertThat(res.getStatus()).isEqualTo(200);
        }

        verify(filterChain, times(20)).doFilter(any(HttpServletRequest.class), any(HttpServletResponse.class));
    }

    @Test
    @DisplayName("RATE-05: Distinct clients have isolated rate limit buckets")
    void testDistinctClientsIsolated() throws ServletException, IOException {
        FilterChain filterChain = mock(FilterChain.class);

        // Exhaust client A
        for (int i = 0; i < 5; i++) {
            MockHttpServletRequest req = new MockHttpServletRequest("POST", "/transactions");
            req.setRemoteAddr("192.168.1.10");
            MockHttpServletResponse res = new MockHttpServletResponse();
            rateLimitingFilter.doFilter(req, res, filterChain);
        }

        // Client A's next request is 429
        MockHttpServletRequest reqA = new MockHttpServletRequest("POST", "/transactions");
        reqA.setRemoteAddr("192.168.1.10");
        MockHttpServletResponse resA = new MockHttpServletResponse();
        rateLimitingFilter.doFilter(reqA, resA, filterChain);
        assertThat(resA.getStatus()).isEqualTo(429);

        // Client B request must succeed
        MockHttpServletRequest reqB = new MockHttpServletRequest("POST", "/transactions");
        reqB.setRemoteAddr("192.168.1.20");
        MockHttpServletResponse resB = new MockHttpServletResponse();
        rateLimitingFilter.doFilter(reqB, resB, filterChain);
        assertThat(resB.getStatus()).isEqualTo(200);
    }
}
