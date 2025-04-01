package com.gs.iceberg.ingestion.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ErrorProducer.
 */
public class ErrorProducerTest {

    @Mock
    private KafkaProducer<String, String> kafkaProducer;
    
    @InjectMocks
    private ErrorProducer errorProducer;
    
    private String errorTopic = "error-topic";
    private String testMessage = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
    private String errorReason = "Schema validation failed";

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        try {
            java.lang.reflect.Field field = ErrorProducer.class.getDeclaredField("errorTopic");
            field.setAccessible(true);
            field.set(errorProducer, errorTopic);
        } catch (Exception e) {
            fail("Failed to set errorTopic field: " + e.getMessage());
        }
    }

    @Test
    public void testSendErrorMessage_Success() {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(errorTopic, 0),
                0,
                0,
                System.currentTimeMillis(),
                0L,
                0,
                0
        );
        
        Future<RecordMetadata> future = CompletableFuture.completedFuture(recordMetadata);
        when(kafkaProducer.send(any(ProducerRecord.class))).thenReturn(future);
        
        boolean result = errorProducer.sendErrorMessage(testMessage, errorReason);
        
        assertTrue(result, "Message should be sent to error queue successfully");
        
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendErrorMessage_Exception() {
        when(kafkaProducer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException("Test exception"));
        
        boolean result = errorProducer.sendErrorMessage(testMessage, errorReason);
        
        assertFalse(result, "Message should not be sent to error queue when an exception occurs");
        
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendErrorMessage_NullMessage() {
        boolean result = errorProducer.sendErrorMessage(null, errorReason);
        
        assertFalse(result, "Null message should not be sent to error queue");
        
        verify(kafkaProducer, never()).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendErrorMessage_EmptyMessage() {
        boolean result = errorProducer.sendErrorMessage("", errorReason);
        
        assertFalse(result, "Empty message should not be sent to error queue");
        
        verify(kafkaProducer, never()).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendErrorMessage_NullReason() {
        boolean result = errorProducer.sendErrorMessage(testMessage, null);
        
        assertTrue(result, "Message with null reason should still be sent to error queue");
        
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }

    @Test
    public void testSendErrorMessage_EmptyReason() {
        boolean result = errorProducer.sendErrorMessage(testMessage, "");
        
        assertTrue(result, "Message with empty reason should still be sent to error queue");
        
        verify(kafkaProducer).send(any(ProducerRecord.class));
    }
}
