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
        
        consumer.setTopic(testTopic);
        consumer.setTableName("messages");
        consumer.setSchema(testSchema);
    }

    @Test
    public void testConsumeMessages_ValidMessage() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(testTopic, 0, 0, "key", testMessage);
        
        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(new TopicPartition(testTopic, 0), Collections.singletonList(record));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
        
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
        
        when(schemaService.validateMessage(testMessage, testSchema)).thenReturn(true);
        
        when(icebergService.writeMessage(testMessage, "messages")).thenReturn(true);
        
        consumer.consumeMessages();
        
        verify(kafkaConsumer).poll(any(Duration.class));
        verify(schemaService).validateMessage(testMessage, testSchema);
        verify(icebergService).writeMessage(testMessage, "messages");
        verify(metricsService).incrementMessagesProcessed();
        verify(errorProducer, never()).sendToErrorQueue(anyString(), anyString());
    }

    @Test
    public void testConsumeMessages_InvalidMessage() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(testTopic, 0, 0, "key", testMessage);
        
        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(new TopicPartition(testTopic, 0), Collections.singletonList(record));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
        
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
        
        when(schemaService.validateMessage(testMessage, testSchema)).thenReturn(false);
        
        consumer.consumeMessages();
        
        verify(kafkaConsumer).poll(any(Duration.class));
        verify(schemaService).validateMessage(testMessage, testSchema);
        verify(icebergService, never()).writeMessage(anyString(), anyString());
        verify(metricsService).incrementMessagesRejected();
        verify(errorProducer).sendToErrorQueue(eq(testMessage), contains("Schema validation failed"));
    }

    @Test
    public void testConsumeMessages_WriteFailure() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(testTopic, 0, 0, "key", testMessage);
        
        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(new TopicPartition(testTopic, 0), Collections.singletonList(record));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
        
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
        
        when(schemaService.validateMessage(testMessage, testSchema)).thenReturn(true);
        
        when(icebergService.writeMessage(testMessage, "messages")).thenReturn(false);
        
        consumer.consumeMessages();
        
        verify(kafkaConsumer).poll(any(Duration.class));
        verify(schemaService).validateMessage(testMessage, testSchema);
        verify(icebergService).writeMessage(testMessage, "messages");
        verify(metricsService).incrementMessagesRejected();
        verify(errorProducer).sendToErrorQueue(eq(testMessage), contains("Failed to write message"));
    }

    @Test
    public void testConsumeMessages_Exception() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(testTopic, 0, 0, "key", testMessage);
        
        Map<TopicPartition, java.util.List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
        recordsMap.put(new TopicPartition(testTopic, 0), Collections.singletonList(record));
        ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
        
        when(kafkaConsumer.poll(any(Duration.class))).thenReturn(records);
        
        when(schemaService.validateMessage(testMessage, testSchema)).thenThrow(new RuntimeException("Test exception"));
        
        consumer.consumeMessages();
        
        verify(kafkaConsumer).poll(any(Duration.class));
        verify(schemaService).validateMessage(testMessage, testSchema);
        verify(icebergService, never()).writeMessage(anyString(), anyString());
        verify(metricsService).incrementMessagesRejected();
        verify(errorProducer).sendToErrorQueue(eq(testMessage), contains("Error processing message"));
    }
}
