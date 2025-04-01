package com.gs.iceberg.ingestion.service;

import com.gs.iceberg.ingestion.model.MessageSchema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SchemaService.
 */
public class SchemaServiceTest {

    @InjectMocks
    private SchemaService schemaService;

    private MessageSchema testSchema;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        testSchema = new MessageSchema();
        testSchema.setName("TestSchema");
        testSchema.setDescription("Test schema for unit tests");
        
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "string");
        fields.put("name", "string");
        fields.put("age", "integer");
        fields.put("active", "boolean");
        fields.put("timestamp", "timestamp");
        
        testSchema.setFields(fields);
    }

    @Test
    public void testValidateMessage_ValidMessage() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
        
        boolean isValid = schemaService.validateMessage(message, testSchema);
        
        assertTrue(isValid, "Valid message should be validated successfully");
    }

    @Test
    public void testValidateMessage_InvalidMessage_MissingRequiredField() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
        
        boolean isValid = schemaService.validateMessage(message, testSchema);
        
        assertFalse(isValid, "Message missing required field should be invalid");
    }

    @Test
    public void testValidateMessage_InvalidMessage_WrongType() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":\"thirty\",\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
        
        boolean isValid = schemaService.validateMessage(message, testSchema);
        
        assertFalse(isValid, "Message with wrong type should be invalid");
    }

    @Test
    public void testValidateMessage_InvalidJson() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"";
        
        boolean isValid = schemaService.validateMessage(message, testSchema);
        
        assertFalse(isValid, "Invalid JSON should be invalid");
    }
}
