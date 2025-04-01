package com.gs.iceberg.ingestion.service;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for collecting and exposing metrics.
 * Collects metrics from Kafka, Iceberg, and the application.
 */
@Service
@Slf4j
public class MetricsService {

    private final MeterRegistry meterRegistry;
    private final IcebergService icebergService;
    private final ConsumerFactory<String, String> consumerFactory;
    
    @Value("${kafka.topic}")
    private String kafkaTopic;
    
    private final AtomicLong kafkaLag = new AtomicLong(0);
    private final AtomicLong ingestionRate = new AtomicLong(0);
    private final AtomicLong lastProcessedCount = new AtomicLong(0);
    
    @Autowired
    public MetricsService(MeterRegistry meterRegistry, 
                         IcebergService icebergService,
                         ConsumerFactory<String, String> consumerFactory) {
        this.meterRegistry = meterRegistry;
        this.icebergService = icebergService;
        this.consumerFactory = consumerFactory;
    }
    
    /**
     * Initializes the metrics service by registering metrics.
     */
    @PostConstruct
    public void init() {
        Gauge.builder("kafka.lag", kafkaLag, AtomicLong::get)
            .description("Lag at Kafka partition level")
            .register(meterRegistry);
        
        Gauge.builder("kafka.ingestion.rate", ingestionRate, AtomicLong::get)
            .description("Rate of messages ingested (messages/minute)")
            .register(meterRegistry);
        
        Gauge.builder("iceberg.records.stored.total", icebergService, IcebergService::getTotalStoredRecords)
            .description("Total number of records stored in Iceberg")
            .register(meterRegistry);
    }
    
    /**
     * Updates metrics every minute.
     * Collects metrics from Kafka and the application.
     */
    @Scheduled(fixedRate = 60000) // Every minute
    public void updateMetrics() {
        try {
            updateKafkaLag();
            
            long currentCount = icebergService.getTotalStoredRecords();
            long previousCount = lastProcessedCount.getAndSet(currentCount);
            ingestionRate.set(currentCount - previousCount);
            
            log.debug("Updated metrics: lag={}, ingestionRate={}, totalRecords={}", 
                     kafkaLag.get(), ingestionRate.get(), currentCount);
        } catch (Exception e) {
            log.error("Error updating metrics", e);
        }
    }
    
    /**
     * Updates the Kafka lag metric.
     * Calculates the lag at partition level.
     */
    private void updateKafkaLag() {
        try (Consumer<String, String> consumer = createConsumer()) {
            Set<TopicPartition> partitions = consumer.partitionsFor(kafkaTopic)
                .stream()
                .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                .collect(java.util.stream.Collectors.toSet());
            
            consumer.assign(partitions);
            
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            
            Map<TopicPartition, Long> currentOffsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                long position = consumer.position(partition);
                currentOffsets.put(partition, position);
            }
            
            long totalLag = 0;
            for (TopicPartition partition : partitions) {
                long endOffset = endOffsets.get(partition);
                long currentOffset = currentOffsets.get(partition);
                long partitionLag = endOffset - currentOffset;
                totalLag += partitionLag;
                
                Gauge.builder("kafka.lag.partition", () -> partitionLag)
                    .tag("partition", String.valueOf(partition.partition()))
                    .description("Lag for Kafka partition")
                    .register(meterRegistry);
            }
            
            kafkaLag.set(totalLag);
        } catch (Exception e) {
            log.error("Error calculating Kafka lag", e);
        }
    }
    
    /**
     * Creates a Kafka consumer for metrics collection.
     * 
     * @return A Kafka consumer
     */
    private Consumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                 consumerFactory.getConfigurationProperties().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "metrics-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new KafkaConsumer<>(props);
    }
    
    /**
     * Gets the current Kafka lag.
     * 
     * @return The current Kafka lag
     */
    public long getKafkaLag() {
        return kafkaLag.get();
    }
    
    /**
     * Gets the current ingestion rate.
     * 
     * @return The current ingestion rate (messages/minute)
     */
    public long getIngestionRate() {
        return ingestionRate.get();
    }
}
