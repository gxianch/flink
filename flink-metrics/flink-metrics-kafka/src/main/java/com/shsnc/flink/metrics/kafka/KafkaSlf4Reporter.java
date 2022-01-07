package com.shsnc.flink.metrics.kafka;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Properties;

/** {@link MetricReporter} that exports {@link Metric Metrics} via KAFKA {@link Logger}. */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaSlf4Reporter extends AbstractReporter implements Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSlf4Reporter.class);
    private static final String lineSeparator = System.lineSeparator();

    // the initial size roughly fits ~150 metrics with default scope settings
    private int previousSize = 16384;

    @VisibleForTesting
    Map<Gauge<?>, String> getGauges() {
        return gauges;
    }

    @VisibleForTesting
    Map<Counter, String> getCounters() {
        return counters;
    }

    @VisibleForTesting
    Map<Histogram, String> getHistograms() {
        return histograms;
    }

    @VisibleForTesting
    Map<Meter, String> getMeters() {
        return meters;
    }

    private KafkaProducer kafkaProducer;
    private String topic;

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("metricConfig：" + metricConfig);
        topic = metricConfig.getString("topic", "guo_flinksql");
        Properties properties = new Properties();
        properties.put(
                "bootstrap.servers",
                metricConfig.getString(
                        "bootstrap.servers", "192.168.199.102:9098,192.168.199.104:9098"));
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void close() {}

    @Override
    public void report() {
        try {
            tryReport();
        } catch (ConcurrentModificationException ignored) {
            // at tryReport() we don't synchronize while iterating over the various maps which might
            // cause a
            // ConcurrentModificationException to be thrown, if concurrently a metric is being added
            // or removed.
        }
    }

    private void tryReport() {
        // initialize with previous size to avoid repeated resizing of backing array
        // pad the size to allow deviations in the final string, for example due to different double
        // value representations
        StringBuilder builder = new StringBuilder((int) (previousSize * 1.1));

        builder.append(lineSeparator)
                .append(
                        "=========================== Starting metrics report ===========================")
                .append(lineSeparator);

        builder.append(lineSeparator)
                .append(
                        "-- Counters -------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Counter, String> metric : counters.entrySet()) {
            builder.append(metric.getValue())
                    .append(": ")
                    .append(metric.getKey().getCount())
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "-- Gauges ---------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
            builder.append(metric.getValue())
                    .append(": ")
                    .append(metric.getKey().getValue())
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "-- Meters ---------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Meter, String> metric : meters.entrySet()) {
            builder.append(metric.getValue())
                    .append(": ")
                    .append(metric.getKey().getRate())
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "-- Histograms -----------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
            HistogramStatistics stats = metric.getKey().getStatistics();
            builder.append(metric.getValue())
                    .append(": count=")
                    .append(stats.size())
                    .append(", min=")
                    .append(stats.getMin())
                    .append(", max=")
                    .append(stats.getMax())
                    .append(", mean=")
                    .append(stats.getMean())
                    .append(", stddev=")
                    .append(stats.getStdDev())
                    .append(", p50=")
                    .append(stats.getQuantile(0.50))
                    .append(", p75=")
                    .append(stats.getQuantile(0.75))
                    .append(", p95=")
                    .append(stats.getQuantile(0.95))
                    .append(", p98=")
                    .append(stats.getQuantile(0.98))
                    .append(", p99=")
                    .append(stats.getQuantile(0.99))
                    .append(", p999=")
                    .append(stats.getQuantile(0.999))
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "=========================== Finished metrics report ===========================")
                .append(lineSeparator);
        //        LOG.info(builder.toString());
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, null, builder.toString());
        kafkaProducer.send(record);
        previousSize = builder.length();
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
