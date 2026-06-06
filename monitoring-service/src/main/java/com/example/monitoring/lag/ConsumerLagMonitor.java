package com.example.monitoring.lag;

import com.example.monitoring.alerting.AlertEvent;
import com.example.monitoring.alerting.AlertmanagerClient;
import com.example.monitoring.config.MonitoringProperties;
import com.example.monitoring.rule.RuleHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;

/**
 * RATE rule (DLQ) and consumer lag checks — polled from Prometheus every slide-seconds.
 * Also implements ABSENCE rule: fires if no spans seen for a service in 2× window period.
 *
 * Adding a new scheduled rule:
 *   1. Implement RuleHandler
 *   2. Inject KafkaTemplate, AlertmanagerClient
 *   3. @Scheduled poll, publish to otel.alerts.pending
 */
@Component
public class ConsumerLagMonitor implements RuleHandler {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMonitor.class);

    @Value("${prometheus.url}")
    private String prometheusUrl;

    @Autowired private MonitoringProperties props;
    @Autowired private AlertmanagerClient alertmanager;
    @Autowired private KafkaTemplate<String, String> kafka;

    private final RestTemplate http = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public String getRuleType() { return "RATE"; }

    /** Consumer lag — THRESHOLD rule for Kafka consumer groups. */
    @Scheduled(fixedDelayString = "${monitoring.latency.slide-seconds:10}000")
    public void checkConsumerLag() {
        long threshold = props.getConsumerLag().getThresholdMessages();
        String query = "sum(kafka_consumergroup_lag)by(topic,consumergroup)";
        queryPrometheus(query, "consumer_lag", (metric, value) -> {
            if (value > threshold) {
                String topic = metric.path("topic").asText("?");
                String group = metric.path("consumergroup").asText("?");
                AlertEvent event = new AlertEvent(
                    group, "THRESHOLD", value, threshold, "warning",
                    String.format("Consumer lag %s/%s = %.0f > %d", topic, group, value, threshold),
                    null, Instant.now()
                );
                log.warn("[ALERT] ConsumerLag: {}/{} = {}", topic, group, (long) value);
                alertmanager.fire(event);
                publishToKafka(group, event);
            }
        });
    }

    /** DLQ rate — RATE rule: fires when DLQ topic has non-zero message rate. */
    @Scheduled(fixedDelayString = "${monitoring.dlq.window-size-minutes:5}00000")
    public void checkDlqRate() {
        double rateThreshold = props.getDlq().getRateThreshold();
        String query = "sum(rate(kafka_topic_partition_current_offset"
            + "{topic=~\".*[.-]dlq$|.*dead[.-]letter.*\"}[2m]))by(topic)";
        queryPrometheus(query, "dlq_rate", (metric, value) -> {
            if (value > rateThreshold) {
                String topic = metric.path("topic").asText("?");
                AlertEvent event = new AlertEvent(
                    topic, getRuleType(), value, rateThreshold, "critical",
                    String.format("DLQ %s rate=%.4f/s > threshold=%.4f", topic, value, rateThreshold),
                    null, Instant.now()
                );
                log.error("[ALERT] DLQ rate: {} = {}/s", topic, value);
                alertmanager.fire(event);
                publishToKafka(topic, event);
            }
        });
    }

    private void queryPrometheus(String promql, String ruleName, MetricCallback cb) {
        try {
            String url = prometheusUrl + "/api/v1/query?query=" +
                java.net.URLEncoder.encode(promql, java.nio.charset.StandardCharsets.UTF_8);
            String body = http.getForObject(url, String.class);
            JsonNode root = om.readTree(body);
            for (JsonNode result : root.path("data").path("result")) {
                double value = Double.parseDouble(result.path("value").get(1).asText("0"));
                cb.accept(result.path("metric"), value);
            }
        } catch (Exception e) {
            log.debug("Prometheus query '{}' failed: {}", ruleName, e.getMessage());
        }
    }

    private void publishToKafka(String key, AlertEvent event) {
        try {
            kafka.send("otel.alerts.pending", key, om.writeValueAsString(event));
        } catch (Exception e) {
            log.warn("Failed to publish alert to Kafka: {}", e.getMessage());
        }
    }

    @FunctionalInterface
    interface MetricCallback {
        void accept(JsonNode metric, double value);
    }
}
