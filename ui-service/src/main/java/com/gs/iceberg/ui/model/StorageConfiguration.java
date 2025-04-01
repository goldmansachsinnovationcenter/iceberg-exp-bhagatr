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
 * Entity representing a storage configuration.
 */
@Entity
@Table(name = "storage_configurations")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StorageConfiguration {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String storageType; // "HDFS" or "S3"

    @Column(nullable = false)
    private String basePath;

    @Column(nullable = false)
    private int retentionDays = 10; // Default: 10 days

    private String keytabPath;

    private String principal;

    private String accessKey;

    private String secretKey;

    private String region;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @OneToMany(mappedBy = "storageConfiguration", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<Metric> metrics = new ArrayList<>();

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
     * Adds a metric to the configuration.
     *
     * @param metric The metric to add
     */
    public void addMetric(Metric metric) {
        metrics.add(metric);
        metric.setStorageConfiguration(this);
    }

    /**
     * Removes a metric from the configuration.
     *
     * @param metric The metric to remove
     */
    public void removeMetric(Metric metric) {
        metrics.remove(metric);
        metric.setStorageConfiguration(null);
    }
}
