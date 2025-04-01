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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumer for processing JSON messages.
 * Validates messages against the schema and stores them in HDFS/S3 using Iceberg.
 */
@Component
@Slf4j
public class KafkaConsumer {

    private final ObjectMapper objectMapper;
    private final SchemaService schemaService;
    private final IcebergService icebergService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    
    private final Counter messagesReadCounter;
    private final Counter validMessagesCounter;
    private final Counter invalidMessagesCounter;
    private final Timer processingTimer;
    
    @Autowired
    public KafkaConsumer(ObjectMapper objectMapper, 
                         SchemaService schemaService,
                         IcebergService icebergService,
                         KafkaTemplate<String, String> kafkaTemplate,
                         MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.schemaService = schemaService;
        this.icebergService = icebergService;
        this.kafkaTemplate = kafkaTemplate;
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
    }
    
    /**
     * Processes messages from Kafka.
     * Validates messages against the schema and stores them in HDFS/S3 using Iceberg.
     * Invalid messages are pushed to an error queue.
     * 
     * @param message The JSON message from Kafka
     */
    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.consumer-group}")
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
        
        try {
            ErrorMessage errorMessage = new ErrorMessage(message, errorReason);
            String errorJson = objectMapper.writeValueAsString(errorMessage);
            
            kafkaTemplate.send("error-queue", errorJson);
            log.debug("Message pushed to error queue");
        } catch (JsonProcessingException e) {
            log.error("Error creating error message", e);
        }
    }
    
    /**
     * Represents an error message for the error queue.
     */
    private static class ErrorMessage {
        private final String originalMessage;
        private final String errorReason;
        private final long timestamp;
        
        public ErrorMessage(String originalMessage, String errorReason) {
            this.originalMessage = originalMessage;
            this.errorReason = errorReason;
            this.timestamp = System.currentTimeMillis();
        }
        
        public String getOriginalMessage() {
            return originalMessage;
        }
        
        public String getErrorReason() {
            return errorReason;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
    }
}
