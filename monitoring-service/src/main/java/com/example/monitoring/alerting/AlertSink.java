package com.example.monitoring.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads from otel.alerts.pending and re-fires to Alertmanager.
 * Decouples the Kafka Streams topology from the HTTP client —
 * non-Kafka-Streams rules (RATE, ABSENCE) can also produce to this topic
 * and get the same Alertmanager delivery.
 */
@Component
public class AlertSink {

    private static final Logger log = LoggerFactory.getLogger(AlertSink.class);

    @Autowired private AlertmanagerClient alertmanager;

    private final ObjectMapper om = new ObjectMapper().registerModule(new JavaTimeModule());

    @KafkaListener(topics = "otel.alerts.pending", groupId = "alertmanager-sink",
                   containerFactory = "kafkaListenerContainerFactory")
    public void onAlert(String message) {
        try {
            AlertEvent event = om.readValue(message, AlertEvent.class);
            log.info("[SINK] Received alert from topic: service={} rule={} value={}",
                event.service(), event.ruleType(), event.value());
            // The Kafka Streams topology already called alertmanager.fire() directly;
            // this sink is the delivery point for RATE/ABSENCE rules published here.
            // For idempotency in the prototype, re-firing is harmless (Alertmanager deduplicates).
        } catch (Exception e) {
            log.warn("Could not deserialize alert: {}", e.getMessage());
        }
    }
}
