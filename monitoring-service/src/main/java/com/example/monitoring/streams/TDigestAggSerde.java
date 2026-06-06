package com.example.monitoring.streams;

import com.tdunning.math.stats.MergingDigest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Kafka Serde for TDigestAgg state store.
 * Wire format: [4B: tdigest length][N bytes: tdigest bytes][4B: traceId length][M bytes: traceId UTF-8]
 */
public class TDigestAggSerde implements Serde<TDigestAgg> {

    @Override
    public Serializer<TDigestAgg> serializer() {
        return (topic, agg) -> {
            if (agg == null) return null;
            com.tdunning.math.stats.TDigest digest = agg.getDigest();
            ByteBuffer tBuf = ByteBuffer.allocate(digest.smallByteSize());
            digest.asSmallBytes(tBuf);
            tBuf.flip();
            byte[] traceBytes = agg.getLastTraceId().getBytes(StandardCharsets.UTF_8);
            ByteBuffer out = ByteBuffer.allocate(4 + tBuf.limit() + 4 + traceBytes.length);
            out.putInt(tBuf.limit());
            out.put(tBuf);
            out.putInt(traceBytes.length);
            out.put(traceBytes);
            return out.array();
        };
    }

    @Override
    public Deserializer<TDigestAgg> deserializer() {
        return (topic, bytes) -> {
            if (bytes == null || bytes.length == 0) return new TDigestAgg();
            try {
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                int tLen = buf.getInt();
                byte[] tBytes = new byte[tLen];
                buf.get(tBytes);
                int traceLen = buf.getInt();
                byte[] traceBytes = new byte[traceLen];
                buf.get(traceBytes);
                return new TDigestAgg(
                    MergingDigest.fromBytes(ByteBuffer.wrap(tBytes)),
                    new String(traceBytes, StandardCharsets.UTF_8)
                );
            } catch (Exception e) {
                return new TDigestAgg();
            }
        };
    }

    @Override public void configure(Map<String, ?> configs, boolean isKey) {}
    @Override public void close() {}
}
