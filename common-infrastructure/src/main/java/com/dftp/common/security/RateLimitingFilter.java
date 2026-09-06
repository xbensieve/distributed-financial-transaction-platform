package com.dftp.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/**
 * Global Edge API Rate Limiting Filter.
 * Enforces token-bucket rate limits per client identity and endpoint sensitivity.
 * Rejects floods at the HTTP edge (HTTP 429) before entering transactional / Saga boundaries,
 * guaranteeing zero financial state modification on rejected requests.
 */
@Slf4j
@Component
@ConditionalOnWebApplication
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class RateLimitingFilter extends OncePerRequestFilter {

    private final RateLimiter rateLimiter = new RateLimiter();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${dftp.rate-limiting.enabled:true}")
    private boolean enabled = true;

    @Value("${dftp.rate-limiting.trusted-proxy.enabled:false}")
    private boolean trustedProxyEnabled = false;

    @org.springframework.beans.factory.annotation.Autowired(required = false)
    private com.dftp.common.observability.DftpMetrics dftpMetrics;

    // Configurable capacities & refill rates
    @Value("${dftp.rate-limiting.admin.capacity:10}")
    private double adminCapacity = 10;
    @Value("${dftp.rate-limiting.admin.refill-rate:5}")
    private double adminRefill = 5;

    @Value("${dftp.rate-limiting.tx-mutation.capacity:20}")
    private double txMutationCapacity = 20;
    @Value("${dftp.rate-limiting.tx-mutation.refill-rate:10}")
    private double txMutationRefill = 10;

    @Value("${dftp.rate-limiting.acc-mutation.capacity:20}")
    private double accMutationCapacity = 20;
    @Value("${dftp.rate-limiting.acc-mutation.refill-rate:10}")
    private double accMutationRefill = 10;

    @Value("${dftp.rate-limiting.sensitive-read.capacity:50}")
    private double readCapacity = 50;
    @Value("${dftp.rate-limiting.sensitive-read.refill-rate:25}")
    private double readRefill = 25;

    @Value("${dftp.rate-limiting.default.capacity:100}")
    private double defaultCapacity = 100;
    @Value("${dftp.rate-limiting.default.refill-rate:50}")
    private double defaultRefill = 50;

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        // Actuator health & metrics endpoints are never rate limited
        return !enabled || path.startsWith("/actuator");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();
        String method = request.getMethod();
        String clientIdentity = resolveClientIdentity(request);

        EndpointTier tier = classifyEndpoint(method, path);
        String rateLimitKey = tier.name() + ":" + clientIdentity;

        boolean allowed = rateLimiter.tryAcquire(rateLimitKey, tier.capacity, tier.refillRate);

        if (!allowed) {
            log.warn("RATE_LIMIT_EXCEEDED: Rejected {} {} from '{}' (Tier: {}, Capacity: {})",
                    method, path, clientIdentity, tier.name(), tier.capacity);

            if (dftpMetrics != null) {
                dftpMetrics.recordRateLimitExceeded(tier.name());
            }

            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
            response.setHeader("Retry-After", "1");
            response.setHeader("X-RateLimit-Limit", String.valueOf((int) tier.capacity));
            response.setHeader("X-RateLimit-Remaining", "0");
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);

            Map<String, Object> errorBody = Map.of(
                    "timestamp", Instant.now().toString(),
                    "status", HttpStatus.TOO_MANY_REQUESTS.value(),
                    "error", "Too Many Requests",
                    "message", "API rate limit exceeded for endpoint category: " + tier.name(),
                    "path", path
            );

            response.getWriter().write(objectMapper.writeValueAsString(errorBody));
            return; // Terminate filter chain early — zero database/Saga involvement!
        }

        filterChain.doFilter(request, response);
    }

    private String resolveClientIdentity(HttpServletRequest request) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ") && authHeader.length() > 15) {
            // Use sub-hash of Bearer token as identity to segregate different authenticated principals
            return "token:" + Integer.toHexString(authHeader.hashCode());
        }

        if (trustedProxyEnabled) {
            String xForwardedFor = request.getHeader("X-Forwarded-For");
            if (xForwardedFor != null && !xForwardedFor.isBlank()) {
                return "ip:" + xForwardedFor.split(",")[0].trim();
            }
        }
        return "ip:" + request.getRemoteAddr();
    }

    private EndpointTier classifyEndpoint(String method, String path) {
        if (path.startsWith("/admin")) {
            return new EndpointTier("ADMIN", adminCapacity, adminRefill);
        } else if ("POST".equalsIgnoreCase(method) && path.startsWith("/transactions")) {
            return new EndpointTier("TX_MUTATION", txMutationCapacity, txMutationRefill);
        } else if ("POST".equalsIgnoreCase(method) && path.startsWith("/accounts")) {
            return new EndpointTier("ACC_MUTATION", accMutationCapacity, accMutationRefill);
        } else if ("GET".equalsIgnoreCase(method) && (path.startsWith("/transactions") || path.startsWith("/accounts") || path.startsWith("/ledger"))) {
            return new EndpointTier("SENSITIVE_READ", readCapacity, readRefill);
        } else {
            return new EndpointTier("DEFAULT", defaultCapacity, defaultRefill);
        }
    }

    private record EndpointTier(String name, double capacity, double refillRate) {}

    public void resetRateLimiter() {
        rateLimiter.clear();
    }
}
