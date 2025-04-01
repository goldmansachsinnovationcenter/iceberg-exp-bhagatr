package com.gs.iceberg.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.iceberg.ingestion.model.MessageSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Service for managing JSON message schemas.
 * Loads the schema from a file and provides methods for validating messages.
 */
@Service
@Slf4j
public class SchemaService {

    private final ObjectMapper objectMapper;
    private final AtomicReference<MessageSchema> currentSchema = new AtomicReference<>();
    
    @Value("${schema.file:#{null}}")
    private String schemaFile;
    
    @Autowired
    public SchemaService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }
    
    /**
     * Initializes the schema service by loading the schema from a file.
     */
    @PostConstruct
    public void init() {
        if (schemaFile != null && !schemaFile.isEmpty()) {
            try {
                loadSchemaFromFile(schemaFile);
            } catch (IOException e) {
                log.error("Error loading schema from file: {}", schemaFile, e);
            }
        }
    }
    
    /**
     * Loads a schema from a file.
     * 
     * @param filePath The path to the schema file
     * @throws IOException If an error occurs while reading the file
     */
    public void loadSchemaFromFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            MessageSchema schema = objectMapper.readValue(file, MessageSchema.class);
            setCurrentSchema(schema);
            log.info("Schema loaded from file: {}", filePath);
        } else {
            log.warn("Schema file not found: {}", filePath);
        }
    }
    
    /**
     * Sets the current schema.
     * 
     * @param schema The new schema
     */
    public void setCurrentSchema(MessageSchema schema) {
        currentSchema.set(schema);
    }
    
    /**
     * Gets the current schema.
     * 
     * @return The current schema
     */
    public MessageSchema getCurrentSchema() {
        return currentSchema.get();
    }
    
    /**
     * Validates a message against the current schema.
     * 
     * @param message The message to validate
     * @return true if the message is valid, false otherwise
     */
    public boolean validateMessage(Object message) {
        MessageSchema schema = currentSchema.get();
        if (schema == null) {
            log.warn("No schema available for validation");
            return false;
        }
        
        if (message instanceof java.util.Map) {
            return schema.validateMessage((java.util.Map<String, Object>) message);
        }
        
        return false;
    }
}
