package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.MessageSchema;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for MessageSchema entities.
 */
@Repository
public interface MessageSchemaRepository extends JpaRepository<MessageSchema, Long> {
    
    /**
     * Finds a message schema by name.
     *
     * @param name The name of the schema
     * @return The message schema, if found
     */
    Optional<MessageSchema> findByName(String name);
}
