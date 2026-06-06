package com.example.monitoring.streams;

import com.example.monitoring.alerting.AlertEvent;
import com.example.monitoring.alerting.AlertmanagerClient;
import com.example.monitoring.config.MonitoringProperties;
import com.example.monitoring.rule.RuleHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * THRESHOLD rule — Kafka Streams topology.
 *
 * otel.spans (OTLP JSON)
 *   → parse → filter SERVER spans
 *   → groupBy(serviceName)
 *   → hopping window (windowSize / slideSeconds)
 *   → aggregate T-Digest
 *   → if p99 > threshold && count >= minSample → AlertEvent
 *   → otel.alerts.pending
 */
@Component
public class LatencyTopology implements RuleHandler {

    private static final Logger log = LoggerFactory.getLogger(LatencyTopology.class);

    public static final String SPANS_TOPIC  = "otel.spans";
    public static final String ALERTS_TOPIC = "otel.alerts.pending";

    @Autowired private MonitoringProperties props;
    @Autowired private OtlpSpanParser parser;
    @Autowired private AlertmanagerClient alertmanager;

    private final ObjectMapper om = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    @Override
    public String getRuleType() { return "THRESHOLD"; }

    @Bean
    public KStream<String, String> latencyStream(StreamsBuilder builder) {
        var lat = props.getLatency();

        KStream<String, String> raw = builder.stream(
            SPANS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));

        raw
            .flatMapValues(value -> parser.parse(value))
            .filter((k, span) -> span != null && span.durationMs() > 0)
            .selectKey((k, span) -> span.serviceName())
            .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(SpanRecord.class)))
            .windowedBy(
                TimeWindows.ofSizeAndGrace(
                    Duration.ofSeconds(lat.getWindowSizeSeconds()),
                    Duration.ofSeconds(lat.getSlideSeconds())
                ).advanceBy(Duration.ofSeconds(lat.getSlideSeconds()))
            )
            .aggregate(
                TDigestAgg::new,
                (svc, span, agg) -> agg.add(span.durationMs(), span.traceId()),
                Materialized.<String, TDigestAgg, WindowStore<Bytes, byte[]>>as("latency-tdigest")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(new TDigestAggSerde())
            )
            .toStream()
            .filter((wk, agg) ->
                agg != null
                && agg.size() >= lat.getMinSampleCount()
                && agg.p99() > lat.getP99ThresholdMs()
            )
            .map((wk, agg) -> {
                AlertEvent event = new AlertEvent(
                    wk.key(),
                    getRuleType(),
                    round2(agg.p99()),
                    lat.getP99ThresholdMs(),
                    "warning",
                    String.format("p99=%.0fms > threshold=%.0fms (n=%d)",
                        agg.p99(), lat.getP99ThresholdMs(), agg.size()),
                    agg.getLastTraceId(),
                    Instant.now()
                );
                log.warn("[ALERT] {} service={} p99={}ms",
                    getRuleType(), wk.key(), (int) agg.p99());
                alertmanager.fire(event);
                try {
                    return KeyValue.pair(wk.key(), om.writeValueAsString(event));
                } catch (Exception e) {
                    return KeyValue.pair(wk.key(), "{}");
                }
            })
            .to(ALERTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return raw;
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
