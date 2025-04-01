package com.gs.iceberg.ingestion.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.iceberg.ingestion.model.MessageSchema;
import com.gs.iceberg.ingestion.service.IcebergService;
import com.gs.iceberg.ingestion.service.SchemaService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer for processing JSON messages.
 * Validates messages against the schema and stores them in HDFS/S3 using Iceberg.
 * Uses Kerberos keytab file based authentication for Kafka.
 */
@Component
@Slf4j
public class KafkaConsumer {

    private final ObjectMapper objectMapper;
    private final SchemaService schemaService;
    private final IcebergService icebergService;
    private final ErrorProducer errorProducer;
    private final MeterRegistry meterRegistry;
    
    private final Counter messagesReadCounter;
    private final Counter validMessagesCounter;
    private final Counter invalidMessagesCounter;
    private final Timer processingTimer;
    
    @Value("${kafka.consumer-rate:100}")
    private int consumerRate;
    
    @Autowired
    public KafkaConsumer(ObjectMapper objectMapper, 
                         SchemaService schemaService,
                         IcebergService icebergService,
                         ErrorProducer errorProducer,
                         MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.schemaService = schemaService;
        this.icebergService = icebergService;
        this.errorProducer = errorProducer;
        this.meterRegistry = meterRegistry;
        
        this.messagesReadCounter = Counter.builder("kafka.messages.read")
                .description("Number of messages read from Kafka")
                .register(meterRegistry);
        this.validMessagesCounter = Counter.builder("kafka.messages.valid")
                .description("Number of valid messages")
                .register(meterRegistry);
        this.invalidMessagesCounter = Counter.builder("kafka.messages.invalid")
                .description("Number of invalid messages")
                .register(meterRegistry);
        this.processingTimer = Timer.builder("kafka.messages.processing.time")
                .description("Time taken to process messages")
                .register(meterRegistry);
        
        log.info("Kafka consumer initialized with rate: {} messages/minute", consumerRate);
    }
    
    /**
     * Processes messages from Kafka.
     * Validates messages against the schema and stores them in HDFS/S3 using Iceberg.
     * Invalid messages are pushed to an error queue.
     * 
     * @param message The JSON message from Kafka
     */
    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.consumer-group}", 
                  properties = {"max.poll.records=${kafka.consumer-rate}"})
    public void processMessage(String message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        messagesReadCounter.increment();
        
        try {
            log.debug("Received message: {}", message);
            
            Map<String, Object> jsonMessage = objectMapper.readValue(message, Map.class);
            
            MessageSchema schema = schemaService.getCurrentSchema();
            
            if (schema != null && schema.validateMessage(jsonMessage)) {
                validMessagesCounter.increment();
                icebergService.writeToIceberg(jsonMessage);
                log.debug("Message processed successfully");
            } else {
                handleInvalidMessage(message, "Schema validation failed");
            }
        } catch (JsonProcessingException e) {
            handleInvalidMessage(message, "Invalid JSON format: " + e.getMessage());
        } catch (Exception e) {
            handleInvalidMessage(message, "Error processing message: " + e.getMessage());
            log.error("Error processing message", e);
        } finally {
            sample.stop(processingTimer);
        }
    }
    
    /**
     * Handles invalid messages by pushing them to an error queue.
     * 
     * @param message The original message
     * @param errorReason The reason for the error
     */
    private void handleInvalidMessage(String message, String errorReason) {
        invalidMessagesCounter.increment();
        log.warn("Invalid message: {}", errorReason);
        
        boolean sent = errorProducer.sendErrorMessage(message, errorReason);
        if (!sent) {
            log.error("Failed to send message to error queue: {}", errorReason);
        }
    }
}
