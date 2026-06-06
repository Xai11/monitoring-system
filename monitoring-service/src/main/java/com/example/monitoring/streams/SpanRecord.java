package com.example.monitoring.streams;

public record SpanRecord(String serviceName, double durationMs, String traceId) {
    // Used as Kafka Streams intermediate value — serialized to JSON by JsonSerde
}
