package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.FieldDefinition;
import com.gs.iceberg.ui.model.MessageSchema;
import com.gs.iceberg.ui.repository.MessageSchemaRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Service for managing message schemas.
 */
@Service
@Slf4j
public class MessageSchemaService {

    private final MessageSchemaRepository messageSchemaRepository;

    @Autowired
    public MessageSchemaService(MessageSchemaRepository messageSchemaRepository) {
        this.messageSchemaRepository = messageSchemaRepository;
    }

    /**
     * Gets all message schemas.
     *
     * @return The list of message schemas
     */
    public List<MessageSchema> getAllSchemas() {
        return messageSchemaRepository.findAll();
    }

    /**
     * Gets a message schema by ID.
     *
     * @param id The ID of the message schema
     * @return The message schema, if found
     */
    public Optional<MessageSchema> getSchemaById(Long id) {
        return messageSchemaRepository.findById(id);
    }

    /**
     * Gets a message schema by name.
     *
     * @param name The name of the message schema
     * @return The message schema, if found
     */
    public Optional<MessageSchema> getSchemaByName(String name) {
        return messageSchemaRepository.findByName(name);
    }

    /**
     * Creates a new message schema.
     *
     * @param schema The message schema to create
     * @return The created message schema
     */
    @Transactional
    public MessageSchema createSchema(MessageSchema schema) {
        log.info("Creating message schema: {}", schema.getName());
        return messageSchemaRepository.save(schema);
    }

    /**
     * Updates an existing message schema.
     *
     * @param id The ID of the message schema to update
     * @param schema The updated message schema
     * @return The updated message schema, if found
     */
    @Transactional
    public Optional<MessageSchema> updateSchema(Long id, MessageSchema schema) {
        log.info("Updating message schema with ID: {}", id);
        return messageSchemaRepository.findById(id)
                .map(existingSchema -> {
                    existingSchema.setName(schema.getName());
                    existingSchema.setDescription(schema.getDescription());
                    
                    existingSchema.getFields().clear();
                    for (FieldDefinition field : schema.getFields()) {
                        field.setSchema(existingSchema);
                        existingSchema.getFields().add(field);
                    }
                    
                    return messageSchemaRepository.save(existingSchema);
                });
    }

    /**
     * Deletes a message schema.
     *
     * @param id The ID of the message schema to delete
     */
    @Transactional
    public void deleteSchema(Long id) {
        log.info("Deleting message schema with ID: {}", id);
        messageSchemaRepository.deleteById(id);
    }
}
