package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.Metric;
import com.gs.iceberg.ui.model.SLO;
import com.gs.iceberg.ui.repository.MetricRepository;
import com.gs.iceberg.ui.repository.SLORepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing metrics and SLOs.
 */
@Service
@Slf4j
public class MetricService {

    private final MetricRepository metricRepository;
    private final SLORepository sloRepository;

    @Autowired
    public MetricService(MetricRepository metricRepository, SLORepository sloRepository) {
        this.metricRepository = metricRepository;
        this.sloRepository = sloRepository;
    }

    /**
     * Gets all metrics.
     *
     * @return The list of metrics
     */
    public List<Metric> getAllMetrics() {
        return metricRepository.findAll();
    }

    /**
     * Gets metrics by name.
     *
     * @param name The name of the metric
     * @return The list of metrics
     */
    public List<Metric> getMetricsByName(String name) {
        return metricRepository.findByName(name);
    }

    /**
     * Gets metrics by type.
     *
     * @param type The type of the metric
     * @return The list of metrics
     */
    public List<Metric> getMetricsByType(String type) {
        return metricRepository.findByType(type);
    }

    /**
     * Gets metrics by timestamp range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of metrics
     */
    public List<Metric> getMetricsByTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        return metricRepository.findByTimestampBetween(startTime, endTime);
    }

    /**
     * Gets a metric by ID.
     *
     * @param id The ID of the metric
     * @return The metric, if found
     */
    public Optional<Metric> getMetricById(Long id) {
        return metricRepository.findById(id);
    }

    /**
     * Creates a new metric.
     *
     * @param metric The metric to create
     * @return The created metric
     */
    @Transactional
    public Metric createMetric(Metric metric) {
        log.info("Creating metric: {}", metric.getName());
        return metricRepository.save(metric);
    }

    /**
     * Updates an existing metric.
     *
     * @param id The ID of the metric to update
     * @param metric The updated metric
     * @return The updated metric, if found
     */
    @Transactional
    public Optional<Metric> updateMetric(Long id, Metric metric) {
        log.info("Updating metric with ID: {}", id);
        return metricRepository.findById(id)
                .map(existingMetric -> {
                    existingMetric.setName(metric.getName());
                    existingMetric.setValue(metric.getValue());
                    existingMetric.setType(metric.getType());
                    existingMetric.setTags(metric.getTags());
                    
                    return metricRepository.save(existingMetric);
                });
    }

    /**
     * Deletes a metric.
     *
     * @param id The ID of the metric to delete
     */
    @Transactional
    public void deleteMetric(Long id) {
        log.info("Deleting metric with ID: {}", id);
        metricRepository.deleteById(id);
    }

    /**
     * Gets all SLOs.
     *
     * @return The list of SLOs
     */
    public List<SLO> getAllSLOs() {
        return sloRepository.findAll();
    }

    /**
     * Gets SLOs by name.
     *
     * @param name The name of the SLO
     * @return The list of SLOs
     */
    public List<SLO> getSLOsByName(String name) {
        return sloRepository.findByName(name);
    }

    /**
     * Gets SLOs by status.
     *
     * @param status The status of the SLO
     * @return The list of SLOs
     */
    public List<SLO> getSLOsByStatus(String status) {
        return sloRepository.findByStatus(status);
    }

    /**
     * Gets SLOs by timestamp range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of SLOs
     */
    public List<SLO> getSLOsByTimeRange(LocalDateTime startTime, LocalDateTime endTime) {
        return sloRepository.findByTimestampBetween(startTime, endTime);
    }

    /**
     * Gets an SLO by ID.
     *
     * @param id The ID of the SLO
     * @return The SLO, if found
     */
    public Optional<SLO> getSLOById(Long id) {
        return sloRepository.findById(id);
    }

    /**
     * Creates a new SLO.
     *
     * @param slo The SLO to create
     * @return The created SLO
     */
    @Transactional
    public SLO createSLO(SLO slo) {
        log.info("Creating SLO: {}", slo.getName());
        return sloRepository.save(slo);
    }

    /**
     * Updates an existing SLO.
     *
     * @param id The ID of the SLO to update
     * @param slo The updated SLO
     * @return The updated SLO, if found
     */
    @Transactional
    public Optional<SLO> updateSLO(Long id, SLO slo) {
        log.info("Updating SLO with ID: {}", id);
        return sloRepository.findById(id)
                .map(existingSLO -> {
                    existingSLO.setName(slo.getName());
                    existingSLO.setDescription(slo.getDescription());
                    existingSLO.setTarget(slo.getTarget());
                    existingSLO.setActualValue(slo.getActualValue());
                    
                    return sloRepository.save(existingSLO);
                });
    }

    /**
     * Deletes an SLO.
     *
     * @param id The ID of the SLO to delete
     */
    @Transactional
    public void deleteSLO(Long id) {
        log.info("Deleting SLO with ID: {}", id);
        sloRepository.deleteById(id);
    }

    /**
     * Updates SLOs based on metrics.
     * Runs every 5 minutes to update SLOs based on the latest metrics.
     */
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    @Transactional
    public void updateSLOs() {
        log.info("Updating SLOs based on metrics");
        List<SLO> slos = sloRepository.findAll();
        
        for (SLO slo : slos) {
            try {
                Optional<Metric> metric = metricRepository.findById(slo.getMetric().getId());
                
                if (metric.isPresent()) {
                    slo.setActualValue(metric.get().getValue());
                    slo.setTimestamp(LocalDateTime.now());
                    
                    sloRepository.save(slo);
                }
            } catch (Exception e) {
                log.error("Error updating SLO: {}", slo.getName(), e);
            }
        }
    }
}
