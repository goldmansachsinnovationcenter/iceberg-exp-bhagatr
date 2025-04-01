package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.StorageConfiguration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Repository for StorageConfiguration entities.
 */
@Repository
public interface StorageConfigurationRepository extends JpaRepository<StorageConfiguration, Long> {
    
    /**
     * Finds a storage configuration by name.
     *
     * @param name The name of the configuration
     * @return The storage configuration, if found
     */
    Optional<StorageConfiguration> findByName(String name);
    
    /**
     * Finds the active storage configuration.
     *
     * @return The active storage configuration, if found
     */
    Optional<StorageConfiguration> findByActiveTrue();
}
