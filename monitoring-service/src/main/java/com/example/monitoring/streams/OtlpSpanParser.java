package com.example.monitoring.streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Parses OTLP JSON trace batches (as produced by OTel Collector kafkaexporter)
 * into flat SpanRecord lists. Accepts SERVER (kind=2) and CONSUMER (kind=5) spans,
 * which cover both HTTP request latency and Kafka message processing latency.
 */
@Component
public class OtlpSpanParser {

    private static final Logger log = LoggerFactory.getLogger(OtlpSpanParser.class);
    private static final int SPAN_KIND_SERVER   = 2;
    private static final int SPAN_KIND_CONSUMER = 5;
    private final ObjectMapper mapper = new ObjectMapper();

    public List<SpanRecord> parse(String json) {
        if (json == null || json.isBlank()) return Collections.emptyList();
        try {
            JsonNode root = mapper.readTree(json);
            List<SpanRecord> records = new ArrayList<>();

            for (JsonNode rs : root.path("resourceSpans")) {
                String svc = extractAttr(rs.path("resource").path("attributes"), "service.name");
                if (svc.isEmpty()) continue;

                for (JsonNode ss : rs.path("scopeSpans")) {
                    for (JsonNode span : ss.path("spans")) {
                        int kind = span.path("kind").asInt();
                        if (kind != SPAN_KIND_SERVER && kind != SPAN_KIND_CONSUMER) continue;

                        String name = span.path("name").asText("");
                        if (name.contains("/actuator") || name.contains("health")) continue;

                        long startNs = parseLong(span.path("startTimeUnixNano").asText("0"));
                        long endNs   = parseLong(span.path("endTimeUnixNano").asText("0"));
                        if (startNs == 0 || endNs <= startNs) continue;

                        double durationMs = (endNs - startNs) / 1_000_000.0;
                        String traceId = span.path("traceId").asText("");
                        records.add(new SpanRecord(svc, durationMs, traceId));
                    }
                }
            }
            return records;
        } catch (Exception e) {
            log.warn("Failed to parse OTLP span batch: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    private String extractAttr(JsonNode attrs, String key) {
        for (JsonNode attr : attrs) {
            if (key.equals(attr.path("key").asText())) {
                return attr.path("value").path("stringValue").asText("");
            }
        }
        return "";
    }

    private long parseLong(String s) {
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return 0L; }
    }
}
