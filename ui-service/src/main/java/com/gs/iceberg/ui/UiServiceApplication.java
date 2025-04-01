package com.gs.iceberg.ui;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the UI Service.
 * This service provides a web interface for configuration, querying, and monitoring
 * of the Kafka-Iceberg application.
 */
@SpringBootApplication
@EnableScheduling
public class UiServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(UiServiceApplication.class, args);
    }
}
