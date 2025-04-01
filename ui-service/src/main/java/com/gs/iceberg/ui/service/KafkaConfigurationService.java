package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.KafkaConfiguration;
import com.gs.iceberg.ui.repository.KafkaConfigurationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Service for managing Kafka configurations.
 */
@Service
@Slf4j
public class KafkaConfigurationService {

    private final KafkaConfigurationRepository kafkaConfigurationRepository;

    @Autowired
    public KafkaConfigurationService(KafkaConfigurationRepository kafkaConfigurationRepository) {
        this.kafkaConfigurationRepository = kafkaConfigurationRepository;
    }

    /**
     * Gets all Kafka configurations.
     *
     * @return The list of Kafka configurations
     */
    public List<KafkaConfiguration> getAllConfigurations() {
        return kafkaConfigurationRepository.findAll();
    }

    /**
     * Gets a Kafka configuration by ID.
     *
     * @param id The ID of the Kafka configuration
     * @return The Kafka configuration, if found
     */
    public Optional<KafkaConfiguration> getConfigurationById(Long id) {
        return kafkaConfigurationRepository.findById(id);
    }

    /**
     * Gets a Kafka configuration by name.
     *
     * @param name The name of the Kafka configuration
     * @return The Kafka configuration, if found
     */
    public Optional<KafkaConfiguration> getConfigurationByName(String name) {
        return kafkaConfigurationRepository.findByName(name);
    }

    /**
     * Creates a new Kafka configuration.
     *
     * @param configuration The Kafka configuration to create
     * @return The created Kafka configuration
     */
    @Transactional
    public KafkaConfiguration createConfiguration(KafkaConfiguration configuration) {
        log.info("Creating Kafka configuration: {}", configuration.getName());
        return kafkaConfigurationRepository.save(configuration);
    }

    /**
     * Updates an existing Kafka configuration.
     *
     * @param id The ID of the Kafka configuration to update
     * @param configuration The updated Kafka configuration
     * @return The updated Kafka configuration, if found
     */
    @Transactional
    public Optional<KafkaConfiguration> updateConfiguration(Long id, KafkaConfiguration configuration) {
        log.info("Updating Kafka configuration with ID: {}", id);
        return kafkaConfigurationRepository.findById(id)
                .map(existingConfiguration -> {
                    existingConfiguration.setName(configuration.getName());
                    existingConfiguration.setBootstrapServers(configuration.getBootstrapServers());
                    existingConfiguration.setTopic(configuration.getTopic());
                    existingConfiguration.setConsumerGroup(configuration.getConsumerGroup());
                    existingConfiguration.setSecurityProtocol(configuration.getSecurityProtocol());
                    existingConfiguration.setSaslMechanism(configuration.getSaslMechanism());
                    existingConfiguration.setKeytabPath(configuration.getKeytabPath());
                    existingConfiguration.setPrincipal(configuration.getPrincipal());
                    existingConfiguration.setConsumptionRate(configuration.getConsumptionRate());
                    
                    return kafkaConfigurationRepository.save(existingConfiguration);
                });
    }

    /**
     * Deletes a Kafka configuration.
     *
     * @param id The ID of the Kafka configuration to delete
     */
    @Transactional
    public void deleteConfiguration(Long id) {
        log.info("Deleting Kafka configuration with ID: {}", id);
        kafkaConfigurationRepository.deleteById(id);
    }
}
