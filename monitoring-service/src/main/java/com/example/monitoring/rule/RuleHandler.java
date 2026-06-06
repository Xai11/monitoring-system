package com.example.monitoring.rule;

/**
 * Plugin interface for anomaly detection rules.
 * Add a new rule type by implementing this interface — no changes to the detector core.
 * Rule types: THRESHOLD (p99 latency), RATE (DLQ frequency), ABSENCE (heartbeat).
 */
public interface RuleHandler {
    /** Identifies this rule in AlertEvent.ruleType and Alertmanager labels. */
    String getRuleType();
}
