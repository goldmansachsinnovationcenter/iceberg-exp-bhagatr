package com.gs.iceberg.ui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity representing a Kafka configuration.
 */
@Entity
@Table(name = "kafka_configurations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaConfiguration {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String bootstrapServers;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String consumerGroup;

    @Column(nullable = false)
    private String securityProtocol;

    private String saslMechanism;

    private String keytabPath;

    private String principal;

    @Column(nullable = false)
    private int consumptionRate = 100; // Default: 100 messages/minute

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "kafkaConfiguration", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<ErrorMessage> errorMessages = new ArrayList<>();

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    /**
     * Adds an error message to the configuration.
     *
     * @param errorMessage The error message to add
     */
    public void addErrorMessage(ErrorMessage errorMessage) {
        errorMessages.add(errorMessage);
        errorMessage.setKafkaConfiguration(this);
    }

    /**
     * Removes an error message from the configuration.
     *
     * @param errorMessage The error message to remove
     */
    public void removeErrorMessage(ErrorMessage errorMessage) {
        errorMessages.remove(errorMessage);
        errorMessage.setKafkaConfiguration(null);
    }
}
