package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.MessageSchema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for QueryService.
 */
public class QueryServiceTest {

    @Mock
    private SparkSession sparkSession;
    
    @Mock
    private Dataset<Row> dataset;
    
    @Mock
    private MessageSchemaService messageSchemaService;
    
    @Mock
    private StorageConfigurationService storageConfigurationService;
    
    @InjectMocks
    private QueryService queryService;
    
    private MessageSchema testSchema;
    private Map<String, Object> testFilters;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        testSchema = new MessageSchema();
        testSchema.setId(1L);
        testSchema.setName("TestSchema");
        
        testFilters = new HashMap<>();
        testFilters.put("name", "John Doe");
        testFilters.put("age", 30);
        testFilters.put("active", true);
        
        when(sparkSession.sql(anyString())).thenReturn(dataset);
        when(dataset.limit(anyInt())).thenReturn(dataset);
    }

    @Test
    public void testExecuteQuery_Success() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        when(dataset.collectAsList()).thenReturn(Collections.emptyList());
        
        List<Map<String, Object>> result = queryService.executeQuery(1L, testFilters, 100);
        
        assertNotNull(result, "Result should not be null");
        assertEquals(0, result.size(), "Result should be empty");
        
        verify(messageSchemaService).getSchemaById(1L);
        verify(sparkSession).sql(contains("SELECT * FROM"));
        verify(dataset).limit(100);
        verify(dataset).collectAsList();
    }

    @Test
    public void testExecuteQuery_SchemaNotFound() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.empty());
        
        List<Map<String, Object>> result = queryService.executeQuery(1L, testFilters, 100);
        
        assertNotNull(result, "Result should not be null");
        assertEquals(0, result.size(), "Result should be empty");
        
        verify(messageSchemaService).getSchemaById(1L);
        verify(sparkSession, never()).sql(anyString());
        verify(dataset, never()).limit(anyInt());
        verify(dataset, never()).collectAsList();
    }

    @Test
    public void testExecuteQuery_Exception() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        when(sparkSession.sql(anyString())).thenThrow(new RuntimeException("Test exception"));
        
        List<Map<String, Object>> result = queryService.executeQuery(1L, testFilters, 100);
        
        assertNotNull(result, "Result should not be null");
        assertEquals(0, result.size(), "Result should be empty");
        
        verify(messageSchemaService).getSchemaById(1L);
        verify(sparkSession).sql(contains("SELECT * FROM"));
        verify(dataset, never()).limit(anyInt());
        verify(dataset, never()).collectAsList();
    }

    @Test
    public void testBuildQueryString_WithFilters() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        
        String query = queryService.buildQueryString(1L, testFilters);
        
        assertNotNull(query, "Query should not be null");
        assertTrue(query.startsWith("SELECT * FROM"), "Query should start with SELECT");
        assertTrue(query.contains("WHERE"), "Query should contain WHERE clause");
        assertTrue(query.contains("name = 'John Doe'"), "Query should contain string filter");
        assertTrue(query.contains("age = 30"), "Query should contain numeric filter");
        assertTrue(query.contains("active = true"), "Query should contain boolean filter");
        
        verify(messageSchemaService).getSchemaById(1L);
    }

    @Test
    public void testBuildQueryString_NoFilters() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        
        String query = queryService.buildQueryString(1L, Collections.emptyMap());
        
        assertNotNull(query, "Query should not be null");
        assertTrue(query.startsWith("SELECT * FROM"), "Query should start with SELECT");
        assertFalse(query.contains("WHERE"), "Query should not contain WHERE clause");
        
        verify(messageSchemaService).getSchemaById(1L);
    }

    @Test
    public void testBuildQueryString_SchemaNotFound() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.empty());
        
        String query = queryService.buildQueryString(1L, testFilters);
        
        assertNull(query, "Query should be null");
        
        verify(messageSchemaService).getSchemaById(1L);
    }

    @Test
    public void testExecuteBulkQuery_Success() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        when(dataset.collectAsList()).thenReturn(Collections.emptyList());
        
        List<Map<String, Object>> result = queryService.executeBulkQuery(1L, testFilters);
        
        assertNotNull(result, "Result should not be null");
        assertEquals(0, result.size(), "Result should be empty");
        
        verify(messageSchemaService).getSchemaById(1L);
        verify(sparkSession).sql(contains("SELECT * FROM"));
        verify(dataset, never()).limit(anyInt());
        verify(dataset).collectAsList();
    }

    @Test
    public void testGetTableColumns_Success() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.of(testSchema));
        
        List<String> result = queryService.getTableColumns(1L);
        
        assertNotNull(result, "Result should not be null");
        
        verify(messageSchemaService).getSchemaById(1L);
    }

    @Test
    public void testGetTableColumns_SchemaNotFound() {
        when(messageSchemaService.getSchemaById(1L)).thenReturn(Optional.empty());
        
        List<String> result = queryService.getTableColumns(1L);
        
        assertNotNull(result, "Result should not be null");
        assertEquals(0, result.size(), "Result should be empty");
        
        verify(messageSchemaService).getSchemaById(1L);
    }
}
