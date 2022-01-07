/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.shsnc.flink.metrics.kafka;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.shsnc.flink.metrics.kafka.influxdb.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/** 改造自InfluxdbReporter */

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
@InstantiateViaFactory(factoryClassName = "org.apache.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporterBak extends AbstractReporter<MeasurementInfo> implements Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReporterBak.class);

    public KafkaReporterBak() {
        super(new MeasurementInfoProvider());
    }

    private KafkaProducer kafkaProducer;
    private String topic;
    private List<String> endsWithMetricList;

    @Override
    public void open(MetricConfig metricConfig) {
        LOG.info("metricConfig：" + metricConfig);
        topic = metricConfig.getString("topic", "");
        if (StringUtils.isBlank(topic)) {
            LOG.error("metrics.reporter.kafka_reporter.topic is null");
        }
        String endsWithMetric = metricConfig.getString("endswith.metricname", "").trim();
        endsWithMetricList = Arrays.asList(endsWithMetric.split(","));
        String bootstrapservers = metricConfig.getString("bootstrap.servers", "");
        if (StringUtils.isBlank(bootstrapservers)) {
            LOG.error("metrics.reporter.kafka_reporter.bootstrap.servers is null");
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapservers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    @Override
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    @Override
    public void report() {
        tryReport();
    }

    private final ObjectMapper mapper = new ObjectMapper();

    private void tryReport() {
        Instant timestamp = Instant.now();
        try {

            List<Map> list = new ArrayList<>();
            for (Map.Entry<Gauge<?>, MeasurementInfo> entry : gauges.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .endsWith(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey())));
                }
            }
            for (Map.Entry<Counter, MeasurementInfo> entry : counters.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .endsWith(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey())));
                }
            }
            for (Map.Entry<Histogram, MeasurementInfo> entry : histograms.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .endsWith(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey())));
                }
            }
            for (Map.Entry<Meter, MeasurementInfo> entry : meters.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .endsWith(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey())));
                }
            }
            if (list.size() > 0) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                topic, null, mapper.writeValueAsString(list));
                kafkaProducer.send(record);
            }

        } catch (ConcurrentModificationException
                | NoSuchElementException
                | JsonProcessingException e) {
            // ignore - may happen when metrics are concurrently added or removed
            // report next time
            LOG.error(e.getMessage());
            return;
        }
    }

    private Map<String, Object> getPointMap(MyPoint p) {
        Map<String, Object> pointmap = new HashMap<>();
        pointmap.put("name", p.getMeasurement());
        pointmap.put("tags", p.getTags());
        pointmap.put("time", p.getTime());
        pointmap.put("fields", p.getFields());
        return pointmap;
    }
}
