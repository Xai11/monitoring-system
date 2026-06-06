package com.example.monitoring.alerting;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AlertEvent(
    String service,
    String ruleType,   // THRESHOLD | RATE | ABSENCE
    double value,
    double threshold,
    String severity,   // warning | critical
    String summary,
    String traceId,    // exemplar trace for correlation
    Instant firedAt
) {}
