package com.gs.iceberg.ingestion.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka producer for sending error messages to the error queue.
 */
@Component
@Slf4j
public class ErrorProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    @Value("${kafka.error-topic:json-messages-error}")
    private String errorTopic;

    @Autowired
    public ErrorProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Sends an error message to the error queue.
     *
     * @param originalMessage The original message that caused the error
     * @param errorReason The reason for the error
     * @return true if the message was sent successfully, false otherwise
     */
    public boolean sendErrorMessage(String originalMessage, String errorReason) {
        try {
            ErrorMessage errorMessage = new ErrorMessage(originalMessage, errorReason);
            String errorJson = objectMapper.writeValueAsString(errorMessage);
            
            kafkaTemplate.send(errorTopic, errorJson);
            log.debug("Message pushed to error queue: {}", errorReason);
            return true;
        } catch (JsonProcessingException e) {
            log.error("Error creating error message", e);
            return false;
        } catch (Exception e) {
            log.error("Error sending message to error queue", e);
            return false;
        }
    }

    /**
     * Represents an error message for the error queue.
     */
    public static class ErrorMessage {
        private final String originalMessage;
        private final String errorReason;
        private final long timestamp;
        private final String status;
        private final long expiryTime;
        
        public ErrorMessage(String originalMessage, String errorReason) {
            this.originalMessage = originalMessage;
            this.errorReason = errorReason;
            this.timestamp = System.currentTimeMillis();
            this.status = "NEW";
            this.expiryTime = System.currentTimeMillis() + 3600000; // 1 hour expiry
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
        
        public String getStatus() {
            return status;
        }
        
        public long getExpiryTime() {
            return expiryTime;
        }
    }
}
