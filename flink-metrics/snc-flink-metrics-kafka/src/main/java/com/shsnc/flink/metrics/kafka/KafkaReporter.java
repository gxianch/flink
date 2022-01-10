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
@InstantiateViaFactory(factoryClassName = "com.shsnc.flink.metrics.kafka.KafkaReporterFactory")
public class KafkaReporter extends AbstractReporter<MeasurementInfo> implements Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaReporter.class);

    public KafkaReporter() {
        super(new MeasurementInfoProvider());
    }

    private KafkaProducer kafkaProducer;
    private String topic;
    private List<String> endsWithMetricList;
    private static final String JOB_ID = "job_id";
    private static final String JOB_NAME = "job_name";

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
            // 从带job的指标中获取job_id和job_name,赋值给不带的指标,配置的指标不带_job_也可以
            String job_id = "";
            String job_name = "";
            List<MeasurementInfo> metriclist = new ArrayList<>();
            metriclist.addAll(gauges.values());
            metriclist.addAll(counters.values());
            metriclist.addAll(histograms.values());
            metriclist.addAll(meters.values());
            for (MeasurementInfo info : metriclist) {
                if (info.getName().startsWith("jobmanager_job_")
                        || info.getName().startsWith("taskmanager_job_")) {
                    job_id = info.getTags().getOrDefault(JOB_ID, "");
                    job_name = info.getTags().getOrDefault(JOB_NAME, "");
                    //                    LOG.info("job_id:{},job_name:{}", job_id, job_name);
                    if (StringUtils.isBlank(job_id) || StringUtils.isBlank(job_name)) {
                        LOG.error("do not get job_id or job name:{}", info);
                        return; // TODO 获取不到job_id，不report到kafka？
                    }
                    break;
                }
            }
            List<Map> list = new ArrayList<>();

            /*         可以过滤以后再匹配，但是如果过滤指标不含job_id，则没有job_id
            Map<Gauge<?>, MeasurementInfo> gaugesfilter =gauges.entrySet().stream()
                .filter(map -> endsWithMetricList.stream()
                        .anyMatch(
                                endWith ->
                                        map.getValue()
                                                .getName()
                                                .endsWith(endWith.trim())))
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));*/
            for (Map.Entry<Gauge<?>, MeasurementInfo> entry : gauges.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .contains(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey()),
                                    job_id,
                                    job_name));
                }
            }
            for (Map.Entry<Counter, MeasurementInfo> entry : counters.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .contains(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey()),
                                    job_id,
                                    job_name));
                }
            }
            for (Map.Entry<Histogram, MeasurementInfo> entry : histograms.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .contains(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey()),
                                    job_id,
                                    job_name));
                }
            }
            for (Map.Entry<Meter, MeasurementInfo> entry : meters.entrySet()) {
                boolean flag =
                        endsWithMetricList.stream()
                                .anyMatch(
                                        endWith ->
                                                entry.getValue()
                                                        .getName()
                                                        .contains(endWith.trim()));
                if (flag) {
                    list.add(
                            getPointMap(
                                    MetricMapper.map(entry.getValue(), timestamp, entry.getKey()),
                                    job_id,
                                    job_name));
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

    private Map<String, Object> getPointMap(MyPoint p, String job_id, String job_name) {
        Map<String, Object> pointmap = new HashMap<>();
        pointmap.put("name", p.getMeasurement());
        if (!p.getTags().containsKey(JOB_ID)) {
            p.getTags().put(JOB_ID, job_id);
        }
        if (!p.getTags().containsKey(JOB_NAME)) {
            p.getTags().put(JOB_NAME, job_name);
        }
        pointmap.put("tags", p.getTags());
        pointmap.put("time", p.getTime());
        pointmap.put("fields", p.getFields());
        return pointmap;
    }
}
