package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.FieldDefinition;
import com.gs.iceberg.ui.model.MessageSchema;
import com.gs.iceberg.ui.repository.MessageSchemaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MessageSchemaService.
 */
public class MessageSchemaServiceTest {

    @Mock
    private MessageSchemaRepository messageSchemaRepository;

    @InjectMocks
    private MessageSchemaService messageSchemaService;

    private MessageSchema testSchema;
    private List<MessageSchema> testSchemas;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        testSchema = new MessageSchema();
        testSchema.setId(1L);
        testSchema.setName("TestSchema");
        testSchema.setDescription("Test schema for unit tests");
        testSchema.setCreatedAt(LocalDateTime.now());
        testSchema.setUpdatedAt(LocalDateTime.now());
        
        List<FieldDefinition> fields = new ArrayList<>();
        fields.add(new FieldDefinition("id", "string", true));
        fields.add(new FieldDefinition("name", "string", true));
        fields.add(new FieldDefinition("age", "integer", true));
        fields.add(new FieldDefinition("active", "boolean", true));
        fields.add(new FieldDefinition("timestamp", "timestamp", true));
        
        testSchema.setFields(fields);
        
        testSchemas = Arrays.asList(testSchema);
    }

    @Test
    public void testGetAllSchemas() {
        when(messageSchemaRepository.findAll()).thenReturn(testSchemas);
        
        List<MessageSchema> result = messageSchemaService.getAllSchemas();
        
        assertEquals(1, result.size(), "Should return one schema");
        assertEquals("TestSchema", result.get(0).getName(), "Schema name should match");
        
        verify(messageSchemaRepository).findAll();
    }

    @Test
    public void testGetSchemaById_Found() {
        when(messageSchemaRepository.findById(1L)).thenReturn(Optional.of(testSchema));
        
        Optional<MessageSchema> result = messageSchemaService.getSchemaById(1L);
        
        assertTrue(result.isPresent(), "Schema should be found");
        assertEquals("TestSchema", result.get().getName(), "Schema name should match");
        
        verify(messageSchemaRepository).findById(1L);
    }

    @Test
    public void testGetSchemaById_NotFound() {
        when(messageSchemaRepository.findById(2L)).thenReturn(Optional.empty());
        
        Optional<MessageSchema> result = messageSchemaService.getSchemaById(2L);
        
        assertFalse(result.isPresent(), "Schema should not be found");
        
        verify(messageSchemaRepository).findById(2L);
    }

    @Test
    public void testCreateSchema() {
        when(messageSchemaRepository.save(any(MessageSchema.class))).thenReturn(testSchema);
        
        MessageSchema result = messageSchemaService.createSchema(testSchema);
        
        assertNotNull(result, "Created schema should not be null");
        assertEquals("TestSchema", result.getName(), "Schema name should match");
        
        verify(messageSchemaRepository).save(any(MessageSchema.class));
    }

    @Test
    public void testUpdateSchema_Found() {
        MessageSchema updatedSchema = new MessageSchema();
        updatedSchema.setName("UpdatedSchema");
        updatedSchema.setDescription("Updated test schema");
        
        List<FieldDefinition> updatedFields = new ArrayList<>();
        updatedFields.add(new FieldDefinition("id", "string", true));
        updatedFields.add(new FieldDefinition("name", "string", true));
        updatedFields.add(new FieldDefinition("age", "integer", true));
        
        updatedSchema.setFields(updatedFields);
        
        when(messageSchemaRepository.findById(1L)).thenReturn(Optional.of(testSchema));
        when(messageSchemaRepository.save(any(MessageSchema.class))).thenAnswer(invocation -> invocation.getArgument(0));
        
        Optional<MessageSchema> result = messageSchemaService.updateSchema(1L, updatedSchema);
        
        assertTrue(result.isPresent(), "Updated schema should be present");
        assertEquals("UpdatedSchema", result.get().getName(), "Schema name should be updated");
        assertEquals("Updated test schema", result.get().getDescription(), "Schema description should be updated");
        assertEquals(3, result.get().getFields().size(), "Schema should have 3 fields");
        
        verify(messageSchemaRepository).findById(1L);
        verify(messageSchemaRepository).save(any(MessageSchema.class));
    }

    @Test
    public void testUpdateSchema_NotFound() {
        MessageSchema updatedSchema = new MessageSchema();
        updatedSchema.setName("UpdatedSchema");
        
        when(messageSchemaRepository.findById(2L)).thenReturn(Optional.empty());
        
        Optional<MessageSchema> result = messageSchemaService.updateSchema(2L, updatedSchema);
        
        assertFalse(result.isPresent(), "Updated schema should not be present");
        
        verify(messageSchemaRepository).findById(2L);
        verify(messageSchemaRepository, never()).save(any(MessageSchema.class));
    }

    @Test
    public void testDeleteSchema() {
        messageSchemaService.deleteSchema(1L);
        
        verify(messageSchemaRepository).deleteById(1L);
    }
}
