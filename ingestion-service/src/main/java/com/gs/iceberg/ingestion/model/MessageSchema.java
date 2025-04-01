package com.gs.iceberg.ingestion.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * Represents a JSON message schema.
 * Used for validating incoming messages from Kafka.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessageSchema {

    private String name;
    private String description;
    private List<FieldDefinition> fields;
    
    /**
     * Validates a JSON message against this schema.
     * 
     * @param message The JSON message to validate
     * @return true if the message is valid, false otherwise
     */
    public boolean validateMessage(Map<String, Object> message) {
        if (message == null || fields == null) {
            return false;
        }
        
        for (FieldDefinition field : fields) {
            if (field.isRequired() && !message.containsKey(field.getName())) {
                return false;
            }
            
            if (message.containsKey(field.getName())) {
                Object value = message.get(field.getName());
                if (!validateFieldType(value, field.getType())) {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Validates a field value against the expected type.
     * 
     * @param value The field value
     * @param type The expected field type
     * @return true if the value matches the type, false otherwise
     */
    private boolean validateFieldType(Object value, String type) {
        if (value == null) {
            return true; // Null values are allowed for non-required fields
        }
        
        switch (type.toLowerCase()) {
            case "string":
                return value instanceof String;
            case "number":
            case "integer":
                return value instanceof Number;
            case "boolean":
                return value instanceof Boolean;
            case "object":
                return value instanceof Map;
            case "array":
                return value instanceof List;
            default:
                return true; // Unknown types are allowed
        }
    }
    
    /**
     * Represents a field definition in a JSON message schema.
     */
    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldDefinition {
        private String name;
        private String type;
        private boolean required;
        private String description;
    }
}
