package com.example.monitoring.streams;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

/**
 * Mutable aggregate for a Kafka Streams windowed computation.
 * Wraps a T-Digest (compression=100) for accurate p99 over a sliding window.
 */
public class TDigestAgg {

    private static final double COMPRESSION = 100.0;

    private final TDigest digest;
    private String lastTraceId = "";

    public TDigestAgg() {
        this.digest = TDigest.createMergingDigest(COMPRESSION);
    }

    TDigestAgg(TDigest digest, String lastTraceId) {
        this.digest = digest;
        this.lastTraceId = lastTraceId;
    }

    public TDigestAgg add(double durationMs, String traceId) {
        digest.add(durationMs);
        if (traceId != null && !traceId.isEmpty()) {
            lastTraceId = traceId;
        }
        return this;
    }

    public double p99() {
        return digest.size() > 0 ? digest.quantile(0.99) : 0.0;
    }

    public long size() {
        return (long) digest.size();
    }

    public TDigest getDigest() { return digest; }
    public String getLastTraceId() { return lastTraceId; }
}
