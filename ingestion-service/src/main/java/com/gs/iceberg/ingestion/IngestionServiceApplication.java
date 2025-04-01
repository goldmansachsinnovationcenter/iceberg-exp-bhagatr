package com.gs.iceberg.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the Ingestion Service.
 * This service is responsible for consuming JSON messages from Kafka,
 * validating them against a schema, and storing them in HDFS/S3 using Iceberg.
 */
@SpringBootApplication
@EnableScheduling
public class IngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionServiceApplication.class, args);
    }
}
