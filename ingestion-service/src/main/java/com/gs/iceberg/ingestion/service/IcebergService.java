package com.gs.iceberg.ingestion.service;

import com.gs.iceberg.ingestion.config.StorageConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for writing data to Apache Iceberg.
 * Uses Spark to write data to HDFS/S3 using Iceberg table format.
 */
@Service
@Slf4j
public class IcebergService {

    private final SparkSession sparkSession;
    private final StorageConfig storageConfig;
    private final MeterRegistry meterRegistry;
    
    @Value("${iceberg.table-name:messages}")
    private String tableName;
    
    @Value("${iceberg.partition-by:}")
    private String partitionBy;
    
    private final Counter recordsStoredCounter;
    private final Timer writeTimer;
    private final AtomicLong totalStoredRecords = new AtomicLong(0);
    
    @Autowired
    public IcebergService(SparkSession sparkSession, 
                         StorageConfig storageConfig,
                         MeterRegistry meterRegistry) {
        this.sparkSession = sparkSession;
        this.storageConfig = storageConfig;
        this.meterRegistry = meterRegistry;
        
        this.recordsStoredCounter = Counter.builder("iceberg.records.stored")
                .description("Number of records stored in Iceberg")
                .register(meterRegistry);
        this.writeTimer = Timer.builder("iceberg.write.time")
                .description("Time taken to write to Iceberg")
                .register(meterRegistry);
    }
    
    /**
     * Initializes the Iceberg service by configuring storage authentication.
     */
    @PostConstruct
    public void init() {
        if ("HDFS".equalsIgnoreCase(storageConfig.getType())) {
            storageConfig.configureHdfsKerberos();
        } else if ("S3".equalsIgnoreCase(storageConfig.getType())) {
            storageConfig.configureS3Authentication();
        }
        
        initializeIcebergTable();
    }
    
    /**
     * Initializes the Iceberg table if it doesn't exist.
     */
    private void initializeIcebergTable() {
        try {
            String tablePath = storageConfig.getStoragePath() + "/" + tableName;
            log.info("Initializing Iceberg table at: {}", tablePath);
            
            boolean tableExists = sparkSession.catalog().tableExists("local." + tableName);
            
            if (!tableExists) {
                log.info("Creating Iceberg table: {}", tableName);
                
                StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("timestamp", DataTypes.TimestampType, false),
                    DataTypes.createStructField("data", DataTypes.StringType, true)
                });
                
                Dataset<Row> emptyDF = sparkSession.createDataFrame(new ArrayList<>(), schema);
                
                if (partitionBy != null && !partitionBy.isEmpty()) {
                    emptyDF.write()
                        .format("iceberg")
                        .mode(SaveMode.ErrorIfExists)
                        .option("path", tablePath)
                        .partitionBy(partitionBy.split(","))
                        .saveAsTable("local." + tableName);
                } else {
                    emptyDF.write()
                        .format("iceberg")
                        .mode(SaveMode.ErrorIfExists)
                        .option("path", tablePath)
                        .saveAsTable("local." + tableName);
                }
                
                log.info("Iceberg table created successfully");
            } else {
                log.info("Iceberg table already exists");
            }
        } catch (Exception e) {
            log.error("Error initializing Iceberg table", e);
        }
    }
    
    /**
     * Writes a JSON message to Iceberg.
     * 
     * @param message The JSON message to write
     */
    public void writeToIceberg(Map<String, Object> message) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            if (!message.containsKey("timestamp")) {
                message.put("timestamp", System.currentTimeMillis());
            }
            
            if (!message.containsKey("id")) {
                message.put("id", java.util.UUID.randomUUID().toString());
            }
            
            List<Map<String, Object>> data = new ArrayList<>();
            data.add(message);
            
            Dataset<Row> df = sparkSession.createDataFrame(data, createSchema(message));
            
            String tablePath = storageConfig.getStoragePath() + "/" + tableName;
            
            df.write()
                .format("iceberg")
                .mode(SaveMode.Append)
                .option("path", tablePath)
                .saveAsTable("local." + tableName);
            
            recordsStoredCounter.increment();
            totalStoredRecords.incrementAndGet();
            
            log.debug("Message written to Iceberg successfully");
        } catch (Exception e) {
            log.error("Error writing to Iceberg", e);
            throw new RuntimeException("Error writing to Iceberg", e);
        } finally {
            sample.stop(writeTimer);
        }
    }
    
    /**
     * Creates a Spark schema from a JSON message.
     * 
     * @param message The JSON message
     * @return The Spark schema
     */
    private StructType createSchema(Map<String, Object> message) {
        List<StructField> fields = new ArrayList<>();
        
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof String) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
            } else if (value instanceof Integer) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.IntegerType, true));
            } else if (value instanceof Long) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.LongType, true));
            } else if (value instanceof Double) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.DoubleType, true));
            } else if (value instanceof Boolean) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.BooleanType, true));
            } else if (value instanceof java.util.Date) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.DateType, true));
            } else if (value instanceof java.sql.Timestamp) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.TimestampType, true));
            } else if (value instanceof List) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.createArrayType(DataTypes.StringType), true));
            } else if (value instanceof Map) {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
            } else {
                fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
            }
        }
        
        return DataTypes.createStructType(fields);
    }
    
    /**
     * Gets the total number of stored records.
     * 
     * @return The total number of stored records
     */
    public long getTotalStoredRecords() {
        return totalStoredRecords.get();
    }
}
