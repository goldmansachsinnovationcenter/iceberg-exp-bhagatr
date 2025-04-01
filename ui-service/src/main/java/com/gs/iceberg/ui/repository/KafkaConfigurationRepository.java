package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.KafkaConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for KafkaConfiguration entities.
 */
@Repository
public interface KafkaConfigurationRepository extends JpaRepository<KafkaConfiguration, Long> {
    
    /**
     * Finds a Kafka configuration by name.
     *
     * @param name The name of the configuration
     * @return The Kafka configuration, if found
     */
    Optional<KafkaConfiguration> findByName(String name);
}
