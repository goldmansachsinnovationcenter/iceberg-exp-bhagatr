package com.gs.iceberg.ingestion.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Producer for sending system alerts to a dedicated Kafka topic.
 */
@Component
@Slf4j
public class AlertProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    
    @Value("${kafka.alert.topic:system-alerts}")
    private String alertTopic;
    
    @Autowired
    public AlertProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    /**
     * Sends a system alert to the alert topic.
     *
     * @param alertType The type of alert
     * @param message The alert message
     * @param severity The severity of the alert (INFO, WARNING, ERROR, CRITICAL)
     */
    public void sendAlert(String alertType, String message, String severity) {
        try {
            Map<String, Object> alert = new HashMap<>();
            alert.put("type", alertType);
            alert.put("message", message);
            alert.put("severity", severity);
            alert.put("timestamp", LocalDateTime.now().toString());
            
            String alertJson = convertToJson(alert);
            
            log.info("Sending alert to topic {}: {}", alertTopic, alertJson);
            kafkaTemplate.send(alertTopic, alertJson);
        } catch (Exception e) {
            log.error("Error sending alert to Kafka", e);
        }
    }
    
    /**
     * Sends an INFO level system alert.
     *
     * @param alertType The type of alert
     * @param message The alert message
     */
    public void sendInfoAlert(String alertType, String message) {
        sendAlert(alertType, message, "INFO");
    }
    
    /**
     * Sends a WARNING level system alert.
     *
     * @param alertType The type of alert
     * @param message The alert message
     */
    public void sendWarningAlert(String alertType, String message) {
        sendAlert(alertType, message, "WARNING");
    }
    
    /**
     * Sends an ERROR level system alert.
     *
     * @param alertType The type of alert
     * @param message The alert message
     */
    public void sendErrorAlert(String alertType, String message) {
        sendAlert(alertType, message, "ERROR");
    }
    
    /**
     * Sends a CRITICAL level system alert.
     *
     * @param alertType The type of alert
     * @param message The alert message
     */
    public void sendCriticalAlert(String alertType, String message) {
        sendAlert(alertType, message, "CRITICAL");
    }
    
    /**
     * Converts a map to a JSON string.
     *
     * @param map The map to convert
     * @return The JSON string
     */
    private String convertToJson(Map<String, Object> map) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) {
                json.append(",");
            }
            
            json.append("\"").append(entry.getKey()).append("\":");
            
            if (entry.getValue() instanceof String) {
                json.append("\"").append(entry.getValue()).append("\"");
            } else {
                json.append(entry.getValue());
            }
            
            first = false;
        }
        
        json.append("}");
        
        return json.toString();
    }
}
