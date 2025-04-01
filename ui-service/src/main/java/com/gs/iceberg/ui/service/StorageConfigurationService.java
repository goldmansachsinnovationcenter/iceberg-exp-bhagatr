package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.StorageConfiguration;
import com.gs.iceberg.ui.repository.StorageConfigurationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
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
    private final Environment environment;
    
    @Value("${storage.type:HDFS}")
    private String defaultStorageType;
    
    @Value("${storage.base-path:localhost:9000/iceberg}")
    private String defaultBasePath;
    
    @Value("${storage.retention-days:10}")
    private int defaultRetentionDays;

    @Autowired
    public StorageConfigurationService(StorageConfigurationRepository storageConfigurationRepository,
                                      Environment environment) {
        this.storageConfigurationRepository = storageConfigurationRepository;
        this.environment = environment;
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
     * Gets the active storage configuration.
     *
     * @return The active storage configuration, if found
     */
    public Optional<StorageConfiguration> getActiveConfiguration() {
        return storageConfigurationRepository.findByActiveTrue();
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
                    existingConfiguration.setActive(configuration.isActive());
                    
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
    
    /**
     * Deactivates all storage configurations.
     */
    @Transactional
    public void deactivateAllConfigurations() {
        log.info("Deactivating all storage configurations");
        List<StorageConfiguration> configurations = storageConfigurationRepository.findAll();
        for (StorageConfiguration configuration : configurations) {
            if (configuration.isActive()) {
                configuration.setActive(false);
                storageConfigurationRepository.save(configuration);
            }
        }
    }
    
    /**
     * Activates a storage configuration.
     *
     * @param id The ID of the storage configuration to activate
     * @return The activated storage configuration, if found
     */
    @Transactional
    public Optional<StorageConfiguration> activateConfiguration(Long id) {
        log.info("Activating storage configuration with ID: {}", id);
        
        deactivateAllConfigurations();
        
        return storageConfigurationRepository.findById(id)
                .map(configuration -> {
                    configuration.setActive(true);
                    return storageConfigurationRepository.save(configuration);
                });
    }
    
    /**
     * Applies the active storage configuration to the application.
     *
     * @return true if the configuration was applied, false otherwise
     */
    @Transactional
    public boolean applyActiveConfiguration() {
        log.info("Applying active storage configuration");
        
        Optional<StorageConfiguration> activeConfigOpt = getActiveConfiguration();
        if (activeConfigOpt.isPresent()) {
            StorageConfiguration activeConfig = activeConfigOpt.get();
            log.info("Applying storage configuration: {}", activeConfig.getName());
            
            System.setProperty("storage.type", activeConfig.getStorageType());
            System.setProperty("storage.base-path", activeConfig.getBasePath());
            System.setProperty("storage.retention-days", String.valueOf(activeConfig.getRetentionDays()));
            
            if ("HDFS".equalsIgnoreCase(activeConfig.getStorageType())) {
                if (activeConfig.getKeytabPath() != null && !activeConfig.getKeytabPath().isEmpty()) {
                    System.setProperty("storage.hdfs-keytab-path", activeConfig.getKeytabPath());
                }
                
                if (activeConfig.getPrincipal() != null && !activeConfig.getPrincipal().isEmpty()) {
                    System.setProperty("storage.hdfs-principal", activeConfig.getPrincipal());
                }
                
                if (activeConfig.getKeytabPath() != null && !activeConfig.getKeytabPath().isEmpty() && 
                    activeConfig.getPrincipal() != null && !activeConfig.getPrincipal().isEmpty()) {
                    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
                    System.setProperty("hadoop.security.authentication", "kerberos");
                    System.setProperty("hadoop.security.authorization", "true");
                }
            } else if ("S3".equalsIgnoreCase(activeConfig.getStorageType())) {
                if (activeConfig.getAccessKey() != null && !activeConfig.getAccessKey().isEmpty()) {
                    System.setProperty("storage.s3-access-key", activeConfig.getAccessKey());
                }
                
                if (activeConfig.getSecretKey() != null && !activeConfig.getSecretKey().isEmpty()) {
                    System.setProperty("storage.s3-secret-key", activeConfig.getSecretKey());
                }
                
                if (activeConfig.getRegion() != null && !activeConfig.getRegion().isEmpty()) {
                    System.setProperty("storage.s3-region", activeConfig.getRegion());
                }
                
                System.setProperty("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                System.setProperty("fs.s3a.aws.credentials.provider", 
                                  "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            }
            
            return true;
        } else {
            log.warn("No active storage configuration found");
            return false;
        }
    }
    
    /**
     * Initializes the default storage configuration if none exists.
     */
    @Transactional
    public void initializeDefaultConfiguration() {
        if (storageConfigurationRepository.count() == 0) {
            log.info("Initializing default storage configuration");
            
            StorageConfiguration defaultConfig = new StorageConfiguration();
            defaultConfig.setName("Default Configuration");
            defaultConfig.setStorageType(defaultStorageType);
            defaultConfig.setBasePath(defaultBasePath);
            defaultConfig.setRetentionDays(defaultRetentionDays);
            defaultConfig.setActive(true);
            
            storageConfigurationRepository.save(defaultConfig);
        }
    }
}
