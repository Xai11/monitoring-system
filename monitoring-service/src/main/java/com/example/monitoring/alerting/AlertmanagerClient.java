package com.example.monitoring.alerting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

/**
 * Posts alerts to Alertmanager's /api/v2/alerts endpoint.
 * Each AlertEvent becomes a single Alertmanager alert with a 5-minute TTL.
 * NOTE: /api/v1/alerts was removed in Alertmanager 0.28.0.
 */
@Component
public class AlertmanagerClient {

    private static final Logger log = LoggerFactory.getLogger(AlertmanagerClient.class);

    @Value("${alertmanager.url}")
    private String alertmanagerUrl;

    private final RestTemplate http = new RestTemplate();
    private final ObjectMapper om = new ObjectMapper().registerModule(new JavaTimeModule());

    public void fire(AlertEvent event) {
        try {
            String payload = buildPayload(event);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<String> resp = http.postForEntity(
                alertmanagerUrl + "/api/v2/alerts",
                new HttpEntity<>(payload, headers),
                String.class
            );
            log.info("Alert fired: service={} rule={} status={}",
                event.service(), event.ruleType(), resp.getStatusCode());
        } catch (Exception e) {
            log.warn("Failed to fire alert to Alertmanager: {}", e.getMessage());
        }
    }

    private String buildPayload(AlertEvent event) throws Exception {
        // Alertmanager alert format: array of alert objects
        Map<String, Object> alert = Map.of(
            "labels", Map.of(
                "alertname", event.ruleType() + "_" + sanitize(event.service()),
                "service",   event.service(),
                "severity",  event.severity(),
                "ruletype",  event.ruleType()
            ),
            "annotations", Map.of(
                "summary",  nvl(event.summary()),
                "trace_id", nvl(event.traceId())
            ),
            "startsAt", event.firedAt().toString(),
            "endsAt",   event.firedAt().plusSeconds(300).toString()
        );
        return om.writeValueAsString(List.of(alert));
    }

    private String sanitize(String s) {
        return s == null ? "unknown" : s.replace("-", "_").replace(".", "_");
    }

    private String nvl(String s) {
        return s != null ? s : "";
    }
}
