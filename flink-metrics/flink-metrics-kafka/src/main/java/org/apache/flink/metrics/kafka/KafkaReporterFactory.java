package org.apache.flink.metrics.kafka;

import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

/** {@link MetricReporterFactory} for {@link KafkaReporter}. */
@InterceptInstantiationViaReflection(
        reporterClassName = "org.apache.flink.metrics.kafka.KafkaReporter")
public class KafkaReporterFactory implements MetricReporterFactory {

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        return new KafkaReporter();
    }
}
