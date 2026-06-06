package com.example.monitoring.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "monitoring")
public class MonitoringProperties {

    private Latency latency = new Latency();
    private Dlq dlq = new Dlq();
    private ConsumerLag consumerLag = new ConsumerLag();

    public Latency getLatency() { return latency; }
    public void setLatency(Latency latency) { this.latency = latency; }

    public Dlq getDlq() { return dlq; }
    public void setDlq(Dlq dlq) { this.dlq = dlq; }

    public ConsumerLag getConsumerLag() { return consumerLag; }
    public void setConsumerLag(ConsumerLag consumerLag) { this.consumerLag = consumerLag; }

    public static class Latency {
        private int windowSizeSeconds = 60;
        private int slideSeconds = 10;
        private double p99ThresholdMs = 200;
        private int minSampleCount = 10;

        public int getWindowSizeSeconds() { return windowSizeSeconds; }
        public void setWindowSizeSeconds(int v) { this.windowSizeSeconds = v; }

        public int getSlideSeconds() { return slideSeconds; }
        public void setSlideSeconds(int v) { this.slideSeconds = v; }

        public double getP99ThresholdMs() { return p99ThresholdMs; }
        public void setP99ThresholdMs(double v) { this.p99ThresholdMs = v; }

        public int getMinSampleCount() { return minSampleCount; }
        public void setMinSampleCount(int v) { this.minSampleCount = v; }
    }

    public static class Dlq {
        private int windowSizeMinutes = 5;
        private double rateThreshold = 0.01;

        public int getWindowSizeMinutes() { return windowSizeMinutes; }
        public void setWindowSizeMinutes(int v) { this.windowSizeMinutes = v; }

        public double getRateThreshold() { return rateThreshold; }
        public void setRateThreshold(double v) { this.rateThreshold = v; }
    }

    public static class ConsumerLag {
        private long thresholdMessages = 1000;

        public long getThresholdMessages() { return thresholdMessages; }
        public void setThresholdMessages(long v) { this.thresholdMessages = v; }
    }
}
