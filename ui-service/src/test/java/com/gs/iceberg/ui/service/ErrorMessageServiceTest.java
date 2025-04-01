package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.ErrorMessage;
import com.gs.iceberg.ui.repository.ErrorMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ErrorMessageService.
 */
public class ErrorMessageServiceTest {

    @Mock
    private ErrorMessageRepository errorMessageRepository;
    
    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @InjectMocks
    private ErrorMessageService errorMessageService;
    
    private ErrorMessage testErrorMessage;
    private List<ErrorMessage> testErrorMessages;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        testErrorMessage = new ErrorMessage();
        testErrorMessage.setId(1L);
        testErrorMessage.setMessage("{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}");
        testErrorMessage.setErrorReason("Schema validation failed");
        testErrorMessage.setCreatedAt(LocalDateTime.now());
        testErrorMessage.setUpdatedAt(LocalDateTime.now());
        
        testErrorMessages = Arrays.asList(testErrorMessage);
        
        errorMessageService.setReprocessingTopic("reprocessing-topic");
    }

    @Test
    public void testGetAllErrorMessages() {
        when(errorMessageRepository.findAll()).thenReturn(testErrorMessages);
        
        List<ErrorMessage> result = errorMessageService.getAllErrorMessages();
        
        assertEquals(1, result.size(), "Should return one error message");
        assertEquals("Schema validation failed", result.get(0).getErrorReason(), "Error reason should match");
        
        verify(errorMessageRepository).findAll();
    }

    @Test
    public void testGetErrorMessageById_Found() {
        when(errorMessageRepository.findById(1L)).thenReturn(Optional.of(testErrorMessage));
        
        Optional<ErrorMessage> result = errorMessageService.getErrorMessageById(1L);
        
        assertTrue(result.isPresent(), "Error message should be found");
        assertEquals("Schema validation failed", result.get().getErrorReason(), "Error reason should match");
        
        verify(errorMessageRepository).findById(1L);
    }

    @Test
    public void testGetErrorMessageById_NotFound() {
        when(errorMessageRepository.findById(2L)).thenReturn(Optional.empty());
        
        Optional<ErrorMessage> result = errorMessageService.getErrorMessageById(2L);
        
        assertFalse(result.isPresent(), "Error message should not be found");
        
        verify(errorMessageRepository).findById(2L);
    }

    @Test
    public void testSaveErrorMessage() {
        when(errorMessageRepository.save(any(ErrorMessage.class))).thenReturn(testErrorMessage);
        
        ErrorMessage result = errorMessageService.saveErrorMessage(testErrorMessage);
        
        assertNotNull(result, "Saved error message should not be null");
        assertEquals("Schema validation failed", result.getErrorReason(), "Error reason should match");
        
        verify(errorMessageRepository).save(any(ErrorMessage.class));
    }

    @Test
    public void testUpdateErrorMessage_Found() {
        ErrorMessage updatedErrorMessage = new ErrorMessage();
        updatedErrorMessage.setMessage("{\"id\":\"123\",\"name\":\"Jane Doe\",\"age\":25,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}");
        updatedErrorMessage.setErrorReason("Updated error reason");
        
        when(errorMessageRepository.findById(1L)).thenReturn(Optional.of(testErrorMessage));
        when(errorMessageRepository.save(any(ErrorMessage.class))).thenAnswer(invocation -> invocation.getArgument(0));
        
        Optional<ErrorMessage> result = errorMessageService.updateErrorMessage(1L, updatedErrorMessage);
        
        assertTrue(result.isPresent(), "Updated error message should be present");
        assertEquals("Updated error reason", result.get().getErrorReason(), "Error reason should be updated");
        assertTrue(result.get().getMessage().contains("Jane Doe"), "Message should be updated");
        
        verify(errorMessageRepository).findById(1L);
        verify(errorMessageRepository).save(any(ErrorMessage.class));
    }

    @Test
    public void testUpdateErrorMessage_NotFound() {
        ErrorMessage updatedErrorMessage = new ErrorMessage();
        updatedErrorMessage.setMessage("{\"id\":\"123\",\"name\":\"Jane Doe\",\"age\":25,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}");
        updatedErrorMessage.setErrorReason("Updated error reason");
        
        when(errorMessageRepository.findById(2L)).thenReturn(Optional.empty());
        
        Optional<ErrorMessage> result = errorMessageService.updateErrorMessage(2L, updatedErrorMessage);
        
        assertFalse(result.isPresent(), "Updated error message should not be present");
        
        verify(errorMessageRepository).findById(2L);
        verify(errorMessageRepository, never()).save(any(ErrorMessage.class));
    }

    @Test
    public void testDeleteErrorMessage() {
        errorMessageService.deleteErrorMessage(1L);
        
        verify(errorMessageRepository).deleteById(1L);
    }

    @Test
    public void testReprocessErrorMessage_Success() {
        when(errorMessageRepository.findById(1L)).thenReturn(Optional.of(testErrorMessage));
        
        boolean result = errorMessageService.reprocessErrorMessage(1L);
        
        assertTrue(result, "Error message should be reprocessed successfully");
        
        verify(errorMessageRepository).findById(1L);
        verify(kafkaTemplate).send(eq("reprocessing-topic"), anyString());
        verify(errorMessageRepository).deleteById(1L);
    }

    @Test
    public void testReprocessErrorMessage_NotFound() {
        when(errorMessageRepository.findById(2L)).thenReturn(Optional.empty());
        
        boolean result = errorMessageService.reprocessErrorMessage(2L);
        
        assertFalse(result, "Error message should not be reprocessed");
        
        verify(errorMessageRepository).findById(2L);
        verify(kafkaTemplate, never()).send(anyString(), anyString());
        verify(errorMessageRepository, never()).deleteById(anyLong());
    }

    @Test
    public void testReprocessErrorMessage_Exception() {
        when(errorMessageRepository.findById(1L)).thenReturn(Optional.of(testErrorMessage));
        when(kafkaTemplate.send(anyString(), anyString())).thenThrow(new RuntimeException("Test exception"));
        
        boolean result = errorMessageService.reprocessErrorMessage(1L);
        
        assertFalse(result, "Error message should not be reprocessed when an exception occurs");
        
        verify(errorMessageRepository).findById(1L);
        verify(kafkaTemplate).send(eq("reprocessing-topic"), anyString());
        verify(errorMessageRepository, never()).deleteById(anyLong());
    }

    @Test
    public void testPurgeExpiredErrorMessages() {
        when(errorMessageRepository.deleteByCreatedAtBefore(any(LocalDateTime.class))).thenReturn(5);
        
        int result = errorMessageService.purgeExpiredErrorMessages(1);
        
        assertEquals(5, result, "Should purge 5 error messages");
        
        verify(errorMessageRepository).deleteByCreatedAtBefore(any(LocalDateTime.class));
    }
}
