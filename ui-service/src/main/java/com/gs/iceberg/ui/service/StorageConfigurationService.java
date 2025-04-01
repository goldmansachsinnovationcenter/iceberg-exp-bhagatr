package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.StorageConfiguration;
import com.gs.iceberg.ui.repository.StorageConfigurationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Service for managing storage configurations.
 */
@Service
@Slf4j
public class StorageConfigurationService {

    private final StorageConfigurationRepository storageConfigurationRepository;

    @Autowired
    public StorageConfigurationService(StorageConfigurationRepository storageConfigurationRepository) {
        this.storageConfigurationRepository = storageConfigurationRepository;
    }

    /**
     * Gets all storage configurations.
     *
     * @return The list of storage configurations
     */
    public List<StorageConfiguration> getAllConfigurations() {
        return storageConfigurationRepository.findAll();
    }

    /**
     * Gets a storage configuration by ID.
     *
     * @param id The ID of the storage configuration
     * @return The storage configuration, if found
     */
    public Optional<StorageConfiguration> getConfigurationById(Long id) {
        return storageConfigurationRepository.findById(id);
    }

    /**
     * Gets a storage configuration by name.
     *
     * @param name The name of the storage configuration
     * @return The storage configuration, if found
     */
    public Optional<StorageConfiguration> getConfigurationByName(String name) {
        return storageConfigurationRepository.findByName(name);
    }

    /**
     * Creates a new storage configuration.
     *
     * @param configuration The storage configuration to create
     * @return The created storage configuration
     */
    @Transactional
    public StorageConfiguration createConfiguration(StorageConfiguration configuration) {
        log.info("Creating storage configuration: {}", configuration.getName());
        return storageConfigurationRepository.save(configuration);
    }

    /**
     * Updates an existing storage configuration.
     *
     * @param id The ID of the storage configuration to update
     * @param configuration The updated storage configuration
     * @return The updated storage configuration, if found
     */
    @Transactional
    public Optional<StorageConfiguration> updateConfiguration(Long id, StorageConfiguration configuration) {
        log.info("Updating storage configuration with ID: {}", id);
        return storageConfigurationRepository.findById(id)
                .map(existingConfiguration -> {
                    existingConfiguration.setName(configuration.getName());
                    existingConfiguration.setStorageType(configuration.getStorageType());
                    existingConfiguration.setBasePath(configuration.getBasePath());
                    existingConfiguration.setRetentionDays(configuration.getRetentionDays());
                    existingConfiguration.setKeytabPath(configuration.getKeytabPath());
                    existingConfiguration.setPrincipal(configuration.getPrincipal());
                    existingConfiguration.setAccessKey(configuration.getAccessKey());
                    existingConfiguration.setSecretKey(configuration.getSecretKey());
                    existingConfiguration.setRegion(configuration.getRegion());
                    
                    return storageConfigurationRepository.save(existingConfiguration);
                });
    }

    /**
     * Deletes a storage configuration.
     *
     * @param id The ID of the storage configuration to delete
     */
    @Transactional
    public void deleteConfiguration(Long id) {
        log.info("Deleting storage configuration with ID: {}", id);
        storageConfigurationRepository.deleteById(id);
    }
}
