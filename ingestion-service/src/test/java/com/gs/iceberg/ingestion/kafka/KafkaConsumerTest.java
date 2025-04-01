package com.gs.iceberg.ingestion.kafka;

import com.gs.iceberg.ingestion.model.MessageSchema;
import com.gs.iceberg.ingestion.service.IcebergService;
import com.gs.iceberg.ingestion.service.MetricsService;
import com.gs.iceberg.ingestion.service.SchemaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

/**
 * Unit tests for KafkaConsumer.
 */
public class KafkaConsumerTest {

    @Mock
    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> kafkaConsumer;
    
    @Mock
    private SchemaService schemaService;
    
    @Mock
    private IcebergService icebergService;
    
    @Mock
    private MetricsService metricsService;
    
    @Mock
    private ErrorProducer errorProducer;
    
    @InjectMocks
    private KafkaConsumer consumer;
    
    private MessageSchema testSchema;
    private String testTopic = "test-topic";
    private String testMessage = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        testSchema = new MessageSchema();
        testSchema.setName("TestSchema");
        
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "string");
        fields.put("name", "string");
        fields.put("age", "integer");
        fields.put("active", "boolean");
        fields.put("timestamp", "timestamp");
        
        testSchema.setFields(fields);
        
        when(schemaService.getCurrentSchema()).thenReturn(testSchema);
    }

    @Test
    public void testProcessMessage_ValidMessage() {
        when(testSchema.validateMessage(any())).thenReturn(true);
        
        consumer.processMessage(testMessage);
        
        verify(icebergService).writeToIceberg(any());
        verify(errorProducer, never()).sendErrorMessage(anyString(), anyString());
    }

    @Test
    public void testProcessMessage_InvalidMessage() {
        when(testSchema.validateMessage(any())).thenReturn(false);
        
        consumer.processMessage(testMessage);
        
        verify(icebergService, never()).writeToIceberg(any());
        verify(errorProducer).sendErrorMessage(eq(testMessage), contains("Schema validation failed"));
    }

    @Test
    public void testProcessMessage_WriteFailure() {
        when(testSchema.validateMessage(any())).thenReturn(true);
        
        doThrow(new RuntimeException("Failed to write message")).when(icebergService).writeToIceberg(any());
        
        consumer.processMessage(testMessage);
        
        verify(icebergService).writeToIceberg(any());
        verify(errorProducer).sendErrorMessage(eq(testMessage), contains("Error processing message"));
    }

    @Test
    public void testProcessMessage_Exception() {
        when(testSchema.validateMessage(any())).thenThrow(new RuntimeException("Test exception"));
        
        consumer.processMessage(testMessage);
        
        verify(icebergService, never()).writeToIceberg(any());
        verify(errorProducer).sendErrorMessage(eq(testMessage), contains("Error processing message"));
    }
}
