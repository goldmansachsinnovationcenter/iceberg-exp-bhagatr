package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.SLO;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for SLO entities.
 */
@Repository
public interface SLORepository extends JpaRepository<SLO, Long> {
    
    /**
     * Finds SLOs by name.
     *
     * @param name The name of the SLO
     * @return The list of SLOs
     */
    List<SLO> findByName(String name);
    
    /**
     * Finds SLOs by metric ID.
     *
     * @param metricId The ID of the metric
     * @return The list of SLOs
     */
    List<SLO> findByMetricId(Long metricId);
    
    /**
     * Finds SLOs by status.
     *
     * @param status The status of the SLO
     * @return The list of SLOs
     */
    List<SLO> findByStatus(String status);
    
    /**
     * Finds SLOs by timestamp range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of SLOs
     */
    List<SLO> findByTimestampBetween(LocalDateTime startTime, LocalDateTime endTime);
}
