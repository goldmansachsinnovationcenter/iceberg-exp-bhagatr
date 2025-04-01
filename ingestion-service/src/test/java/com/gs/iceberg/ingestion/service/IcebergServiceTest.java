package com.gs.iceberg.ingestion.service;

import com.gs.iceberg.ingestion.config.SparkConfig;
import com.gs.iceberg.ingestion.config.StorageConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
 * Unit tests for IcebergService.
 */
public class IcebergServiceTest {

    @Mock
    private SparkConfig sparkConfig;
    
    @Mock
    private StorageConfig storageConfig;
    
    @Mock
    private SparkSession sparkSession;
    
    @Mock
    private Dataset<Row> dataset;
    
    @InjectMocks
    private IcebergService icebergService;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        
        when(sparkConfig.getSparkSession()).thenReturn(sparkSession);
        
        when(storageConfig.getStorageType()).thenReturn("HDFS");
        when(storageConfig.getStoragePath()).thenReturn("hdfs://localhost:9000/iceberg");
        when(storageConfig.getRetentionDays()).thenReturn(10);
    }

    @Test
    public void testWriteMessage_Success() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
        String tableName = "messages";
        
        when(sparkSession.read()).thenReturn(mock(org.apache.spark.sql.DataFrameReader.class));
        when(sparkSession.read().json(anyString())).thenReturn(dataset);
        when(dataset.write()).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString()).mode(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        
        boolean result = icebergService.writeMessage(message, tableName);
        
        assertTrue(result, "Message should be written successfully");
        
        verify(sparkSession).read();
        verify(sparkSession.read()).json(anyString());
        verify(dataset).write();
        verify(dataset.write()).format("iceberg");
        verify(dataset.write().format("iceberg")).mode("append");
    }

    @Test
    public void testWriteMessage_Exception() {
        String message = "{\"id\":\"123\",\"name\":\"John Doe\",\"age\":30,\"active\":true,\"timestamp\":\"2025-01-01T12:00:00Z\"}";
        String tableName = "messages";
        
        when(sparkSession.read()).thenThrow(new RuntimeException("Test exception"));
        
        boolean result = icebergService.writeMessage(message, tableName);
        
        assertFalse(result, "Message should not be written when an exception occurs");
        
        verify(sparkSession).read();
    }

    @Test
    public void testCreateTable_Success() {
        Map<String, String> schema = new HashMap<>();
        schema.put("id", "string");
        schema.put("name", "string");
        schema.put("age", "integer");
        schema.put("active", "boolean");
        schema.put("timestamp", "timestamp");
        
        String tableName = "messages";
        
        when(sparkSession.sql(anyString())).thenReturn(dataset);
        
        boolean result = icebergService.createTable(schema, tableName);
        
        assertTrue(result, "Table should be created successfully");
        
        verify(sparkSession).sql(contains("CREATE TABLE"));
    }

    @Test
    public void testCreateTable_Exception() {
        Map<String, String> schema = new HashMap<>();
        schema.put("id", "string");
        schema.put("name", "string");
        schema.put("age", "integer");
        schema.put("active", "boolean");
        schema.put("timestamp", "timestamp");
        
        String tableName = "messages";
        
        when(sparkSession.sql(anyString())).thenThrow(new RuntimeException("Test exception"));
        
        boolean result = icebergService.createTable(schema, tableName);
        
        assertFalse(result, "Table should not be created when an exception occurs");
        
        verify(sparkSession).sql(contains("CREATE TABLE"));
    }
}
