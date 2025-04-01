package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.Metric;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for Metric entities.
 */
@Repository
public interface MetricRepository extends JpaRepository<Metric, Long> {
    
    /**
     * Finds metrics by name.
     *
     * @param name The name of the metric
     * @return The list of metrics
     */
    List<Metric> findByName(String name);
    
    /**
     * Finds metrics by storage configuration ID.
     *
     * @param storageConfigurationId The ID of the storage configuration
     * @return The list of metrics
     */
    List<Metric> findByStorageConfigurationId(Long storageConfigurationId);
    
    /**
     * Finds metrics by type.
     *
     * @param type The type of the metric
     * @return The list of metrics
     */
    List<Metric> findByType(String type);
    
    /**
     * Finds metrics by timestamp range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of metrics
     */
    List<Metric> findByTimestampBetween(LocalDateTime startTime, LocalDateTime endTime);
}
