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
        
        
        when(storageConfig.getType()).thenReturn("HDFS");
        when(storageConfig.getStoragePath()).thenReturn("hdfs://localhost:9000/iceberg");
    }

    @Test
    public void testWriteToIceberg_Success() {
        Map<String, Object> message = new HashMap<>();
        message.put("id", "123");
        message.put("name", "John Doe");
        message.put("age", 30);
        message.put("active", true);
        message.put("timestamp", "2025-01-01T12:00:00Z");
        
        when(sparkSession.createDataFrame(anyList(), any(org.apache.spark.sql.types.StructType.class))).thenReturn(dataset);
        when(dataset.write()).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString()).mode(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString()).mode(anyString()).option(anyString(), anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        
        icebergService.writeToIceberg(message);
        
        verify(dataset.write()).format("iceberg");
        verify(dataset.write().format("iceberg")).mode("Append");
    }

    @Test
    public void testWriteToIceberg_Exception() {
        Map<String, Object> message = new HashMap<>();
        message.put("id", "123");
        message.put("name", "John Doe");
        message.put("age", 30);
        message.put("active", true);
        message.put("timestamp", "2025-01-01T12:00:00Z");
        
        when(sparkSession.createDataFrame(anyList(), any(org.apache.spark.sql.types.StructType.class))).thenThrow(new RuntimeException("Test exception"));
        
        icebergService.writeToIceberg(message);
        
        verify(sparkSession).createDataFrame(anyList(), any(org.apache.spark.sql.types.StructType.class));
    }

    @Test
    public void testInitializeIcebergTable() {
        when(sparkSession.catalog()).thenReturn(mock(org.apache.spark.sql.catalog.Catalog.class));
        when(sparkSession.catalog().tableExists(anyString())).thenReturn(false);
        when(sparkSession.createDataFrame(anyList(), any(org.apache.spark.sql.types.StructType.class))).thenReturn(dataset);
        when(dataset.write()).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString()).mode(anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        when(dataset.write().format(anyString()).mode(anyString()).option(anyString(), anyString())).thenReturn(mock(org.apache.spark.sql.DataFrameWriter.class));
        
        icebergService.init();
        
        verify(sparkSession.catalog()).tableExists(anyString());
        verify(dataset.write()).format("iceberg");
    }
}
