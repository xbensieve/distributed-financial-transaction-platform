package com.dftp.common.observability;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TraceContextPropagatorTest {

    @AfterEach
    void tearDown() {
        TraceContextPropagator.clearMdc();
    }

    @Test
    @DisplayName("Should generate valid W3C traceparent and metadata")
    void testCreateNewTraceMetadata() {
        TraceContextPropagator.TraceMetadata metadata = TraceContextPropagator.TraceMetadata.createNew();

        assertThat(metadata.traceId()).hasSize(32);
        assertThat(metadata.spanId()).hasSize(16);
        assertThat(metadata.traceparent())
                .isEqualTo("00-" + metadata.traceId() + "-" + metadata.spanId() + "-01");
        assertThat(metadata.correlationId()).isNotBlank();
    }

    @Test
    @DisplayName("Should parse valid W3C traceparent correctly")
    void testParseValidTraceparent() {
        String validTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        Optional<TraceContextPropagator.TraceMetadata> parsed =
                TraceContextPropagator.parseTraceparent(validTraceparent, "congo=t61rcWkgMzE", "test-corr-id");

        assertThat(parsed).isPresent();
        TraceContextPropagator.TraceMetadata meta = parsed.get();
        assertThat(meta.traceparent()).isEqualTo(validTraceparent);
        assertThat(meta.traceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(meta.spanId()).isEqualTo("00f067aa0ba902b7");
        assertThat(meta.tracestate()).isEqualTo("congo=t61rcWkgMzE");
        assertThat(meta.correlationId()).isEqualTo("test-corr-id");
    }

    @Test
    @DisplayName("Should reject invalid or malformed W3C traceparent")
    void testRejectInvalidTraceparent() {
        // Invalid length
        assertThat(TraceContextPropagator.parseTraceparent("00-abc-def-01", null, null)).isEmpty();
        // All zeros traceId (forbidden in W3C)
        assertThat(TraceContextPropagator.parseTraceparent(
                "00-00000000000000000000000000000000-00f067aa0ba902b7-01", null, null)).isEmpty();
        // All zeros spanId (forbidden in W3C)
        assertThat(TraceContextPropagator.parseTraceparent(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", null, null)).isEmpty();
        // Null or blank
        assertThat(TraceContextPropagator.parseTraceparent(null, null, null)).isEmpty();
        assertThat(TraceContextPropagator.parseTraceparent("   ", null, null)).isEmpty();
    }

    @Test
    @DisplayName("Should create child span with matching traceId and new spanId")
    void testCreateChildSpan() {
        TraceContextPropagator.TraceMetadata parent = TraceContextPropagator.TraceMetadata.createNew();
        TraceContextPropagator.TraceMetadata child = parent.createChildSpan();

        assertThat(child.traceId()).isEqualTo(parent.traceId());
        assertThat(child.spanId()).isNotEqualTo(parent.spanId());
        assertThat(child.spanId()).hasSize(16);
        assertThat(child.correlationId()).isEqualTo(parent.correlationId());
        assertThat(child.traceparent()).isEqualTo("00-" + child.traceId() + "-" + child.spanId() + "-01");
    }

    @Test
    @DisplayName("Should inject and extract trace context into/from Kafka Record Headers")
    void testKafkaHeadersPropagation() {
        TraceContextPropagator.TraceMetadata original = TraceContextPropagator.TraceMetadata.createNew("corr-kafka-123");
        Headers headers = new RecordHeaders();

        TraceContextPropagator.injectIntoKafkaHeaders(headers, original);

        Optional<TraceContextPropagator.TraceMetadata> extracted =
                TraceContextPropagator.extractFromKafkaHeaders(headers);

        assertThat(extracted).isPresent();
        assertThat(extracted.get().traceparent()).isEqualTo(original.traceparent());
        assertThat(extracted.get().traceId()).isEqualTo(original.traceId());
        assertThat(extracted.get().spanId()).isEqualTo(original.spanId());
        assertThat(extracted.get().correlationId()).isEqualTo("corr-kafka-123");
    }

    @Test
    @DisplayName("Should populate and clear SLF4J MDC correctly")
    void testMdcPopulationAndClear() {
        TraceContextPropagator.TraceMetadata meta = TraceContextPropagator.TraceMetadata.createNew("corr-mdc-456");

        TraceContextPropagator.populateMdc(meta);

        assertThat(MDC.get(TraceContextPropagator.MDC_TRACE_ID_CAMEL)).isEqualTo(meta.traceId());
        assertThat(MDC.get(TraceContextPropagator.MDC_TRACE_ID_SNAKE)).isEqualTo(meta.traceId());
        assertThat(MDC.get(TraceContextPropagator.MDC_SPAN_ID_CAMEL)).isEqualTo(meta.spanId());
        assertThat(MDC.get(TraceContextPropagator.MDC_SPAN_ID_SNAKE)).isEqualTo(meta.spanId());
        assertThat(MDC.get(TraceContextPropagator.MDC_TRACEPARENT)).isEqualTo(meta.traceparent());
        assertThat(MDC.get(TraceContextPropagator.MDC_CORRELATION_ID)).isEqualTo("corr-mdc-456");

        TraceContextPropagator.clearMdc();

        assertThat(MDC.get(TraceContextPropagator.MDC_TRACE_ID_CAMEL)).isNull();
        assertThat(MDC.get(TraceContextPropagator.MDC_SPAN_ID_CAMEL)).isNull();
        assertThat(MDC.get(TraceContextPropagator.MDC_TRACEPARENT)).isNull();
        assertThat(MDC.get(TraceContextPropagator.MDC_CORRELATION_ID)).isNull();
    }
}
