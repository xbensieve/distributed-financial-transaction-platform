package com.dftp.common.observability;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enterprise W3C TraceContext & Distributed Tracing Propagator (P14).
 * Enforces W3C Recommendation (https://www.w3.org/TR/trace-context/):
 * - traceparent: version(2)-trace_id(32)-parent_id(16)-trace_flags(2)
 * - tracestate: list of key-value pairs
 *
 * Bridges seamlessly across:
 * - Incoming/Outgoing HTTP requests & responses
 * - Outbox table records
 * - Kafka ProducerRecord headers
 * - Kafka ConsumerRecord headers & MDC
 */
@Slf4j
public final class TraceContextPropagator {

    public static final String TRACEPARENT_HEADER = "traceparent";
    public static final String TRACESTATE_HEADER = "tracestate";
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    // MDC Keys
    public static final String MDC_TRACE_ID_CAMEL = "traceId";
    public static final String MDC_TRACE_ID_SNAKE = "trace_id";
    public static final String MDC_SPAN_ID_CAMEL = "spanId";
    public static final String MDC_SPAN_ID_SNAKE = "span_id";
    public static final String MDC_PARENT_ID = "parentId";
    public static final String MDC_CORRELATION_ID = "correlationId";
    public static final String MDC_TRACEPARENT = "traceparent";
    public static final String MDC_TRACESTATE = "tracestate";

    private static final Pattern W3C_TRACEPARENT_PATTERN =
            Pattern.compile("^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$");

    private static final SecureRandom RANDOM = new SecureRandom();

    private TraceContextPropagator() {
    }

    public record TraceMetadata(
            String traceparent,
            String traceId,
            String spanId,
            String tracestate,
            String correlationId
    ) {
        public static TraceMetadata createNew() {
            String traceId = generateHex(32);
            String spanId = generateHex(16);
            String traceparent = "00-" + traceId + "-" + spanId + "-01";
            String correlationId = UUID.randomUUID().toString();
            return new TraceMetadata(traceparent, traceId, spanId, null, correlationId);
        }

        public static TraceMetadata createNew(String correlationId) {
            String traceId = generateHex(32);
            String spanId = generateHex(16);
            String traceparent = "00-" + traceId + "-" + spanId + "-01";
            String corr = (correlationId != null && !correlationId.isBlank()) 
                    ? correlationId 
                    : UUID.randomUUID().toString();
            return new TraceMetadata(traceparent, traceId, spanId, null, corr);
        }

        public TraceMetadata createChildSpan() {
            String newSpanId = generateHex(16);
            String newTraceparent = "00-" + this.traceId + "-" + newSpanId + "-01";
            return new TraceMetadata(newTraceparent, this.traceId, newSpanId, this.tracestate, this.correlationId);
        }
    }

    /**
     * Parses a W3C traceparent string into TraceMetadata.
     * Returns empty if invalid format.
     */
    public static Optional<TraceMetadata> parseTraceparent(String rawTraceparent, String tracestate, String correlationId) {
        if (rawTraceparent == null || rawTraceparent.isBlank()) {
            return Optional.empty();
        }
        String trimmed = rawTraceparent.trim().toLowerCase();
        Matcher matcher = W3C_TRACEPARENT_PATTERN.matcher(trimmed);
        if (!matcher.matches()) {
            log.debug("Invalid W3C traceparent format: {}", rawTraceparent);
            return Optional.empty();
        }

        String version = matcher.group(1);
        String traceId = matcher.group(2);
        String spanId = matcher.group(3);
        String flags = matcher.group(4);

        // Version 00 cannot have all-zeros traceId or spanId
        if ("00000000000000000000000000000000".equals(traceId) || "0000000000000000".equals(spanId)) {
            log.debug("Invalid W3C traceparent: traceId or spanId is all zeros: {}", rawTraceparent);
            return Optional.empty();
        }

        String corr = (correlationId != null && !correlationId.isBlank()) 
                ? correlationId 
                : UUID.randomUUID().toString();

        return Optional.of(new TraceMetadata(trimmed, traceId, spanId, tracestate, corr));
    }

    /**
     * Extracts TraceMetadata from an incoming HttpServletRequest.
     * Generates a new valid W3C TraceContext if none exists or if invalid.
     */
    public static TraceMetadata extractOrGenerate(HttpServletRequest request) {
        String rawTraceparent = request.getHeader(TRACEPARENT_HEADER);
        String tracestate = request.getHeader(TRACESTATE_HEADER);
        String correlationId = request.getHeader(CORRELATION_ID_HEADER);

        return parseTraceparent(rawTraceparent, tracestate, correlationId)
                .orElseGet(() -> TraceMetadata.createNew(correlationId));
    }

    /**
     * Injects TraceMetadata headers into an HttpServletResponse.
     */
    public static void inject(HttpServletResponse response, TraceMetadata metadata) {
        if (metadata == null || response == null) {
            return;
        }
        response.setHeader(TRACEPARENT_HEADER, metadata.traceparent());
        if (metadata.tracestate() != null && !metadata.tracestate().isBlank()) {
            response.setHeader(TRACESTATE_HEADER, metadata.tracestate());
        }
        if (metadata.correlationId() != null) {
            response.setHeader(CORRELATION_ID_HEADER, metadata.correlationId());
        }
    }

    /**
     * Extracts TraceMetadata from Kafka Record Headers.
     * Returns empty if traceparent header is absent or invalid.
     */
    public static Optional<TraceMetadata> extractFromKafkaHeaders(Headers headers) {
        if (headers == null) {
            return Optional.empty();
        }
        String traceparent = getHeaderValue(headers, TRACEPARENT_HEADER);
        String tracestate = getHeaderValue(headers, TRACESTATE_HEADER);
        String correlationId = getHeaderValue(headers, CORRELATION_ID_HEADER);

        return parseTraceparent(traceparent, tracestate, correlationId);
    }

    /**
     * Injects TraceMetadata into Kafka Record Headers.
     */
    public static void injectIntoKafkaHeaders(Headers headers, TraceMetadata metadata) {
        if (headers == null || metadata == null) {
            return;
        }
        if (metadata.traceparent() != null) {
            headers.remove(TRACEPARENT_HEADER);
            headers.add(new RecordHeader(TRACEPARENT_HEADER, metadata.traceparent().getBytes(StandardCharsets.UTF_8)));
        }
        if (metadata.tracestate() != null) {
            headers.remove(TRACESTATE_HEADER);
            headers.add(new RecordHeader(TRACESTATE_HEADER, metadata.tracestate().getBytes(StandardCharsets.UTF_8)));
        }
        if (metadata.correlationId() != null) {
            headers.remove(CORRELATION_ID_HEADER);
            headers.add(new RecordHeader(CORRELATION_ID_HEADER, metadata.correlationId().getBytes(StandardCharsets.UTF_8)));
        }
    }

    /**
     * Populates SLF4J MDC with standard trace identifiers.
     */
    public static void populateMdc(TraceMetadata metadata) {
        if (metadata == null) {
            return;
        }
        if (metadata.traceId() != null) {
            MDC.put(MDC_TRACE_ID_CAMEL, metadata.traceId());
            MDC.put(MDC_TRACE_ID_SNAKE, metadata.traceId());
        }
        if (metadata.spanId() != null) {
            MDC.put(MDC_SPAN_ID_CAMEL, metadata.spanId());
            MDC.put(MDC_SPAN_ID_SNAKE, metadata.spanId());
        }
        if (metadata.traceparent() != null) {
            MDC.put(MDC_TRACEPARENT, metadata.traceparent());
        }
        if (metadata.tracestate() != null) {
            MDC.put(MDC_TRACESTATE, metadata.tracestate());
        }
        if (metadata.correlationId() != null) {
            MDC.put(MDC_CORRELATION_ID, metadata.correlationId());
        }
    }

    /**
     * Clears tracing information from SLF4J MDC.
     */
    public static void clearMdc() {
        MDC.remove(MDC_TRACE_ID_CAMEL);
        MDC.remove(MDC_TRACE_ID_SNAKE);
        MDC.remove(MDC_SPAN_ID_CAMEL);
        MDC.remove(MDC_SPAN_ID_SNAKE);
        MDC.remove(MDC_PARENT_ID);
        MDC.remove(MDC_TRACEPARENT);
        MDC.remove(MDC_TRACESTATE);
        MDC.remove(MDC_CORRELATION_ID);
    }

    /**
     * Captures current active TraceMetadata from MDC if present.
     */
    public static Optional<TraceMetadata> currentTraceMetadata() {
        String traceparent = MDC.get(MDC_TRACEPARENT);
        String tracestate = MDC.get(MDC_TRACESTATE);
        String correlationId = MDC.get(MDC_CORRELATION_ID);
        if (traceparent != null) {
            return parseTraceparent(traceparent, tracestate, correlationId);
        }
        String traceId = MDC.get(MDC_TRACE_ID_CAMEL);
        String spanId = MDC.get(MDC_SPAN_ID_CAMEL);
        if (traceId != null && spanId != null) {
            String synthesized = "00-" + traceId + "-" + spanId + "-01";
            return parseTraceparent(synthesized, tracestate, correlationId);
        }
        return Optional.empty();
    }

    private static String getHeaderValue(Headers headers, String headerKey) {
        Header header = headers.lastHeader(headerKey);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }

    private static String generateHex(int length) {
        byte[] bytes = new byte[length / 2];
        RANDOM.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(length);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
