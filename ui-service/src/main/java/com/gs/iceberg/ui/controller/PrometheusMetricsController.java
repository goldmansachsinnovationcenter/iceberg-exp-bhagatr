package com.gs.iceberg.ui.controller;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for exposing Prometheus metrics.
 */
@RestController
@RequestMapping("/actuator/prometheus")
@Slf4j
public class PrometheusMetricsController {

    private final PrometheusMeterRegistry meterRegistry;

    @Autowired
    public PrometheusMetricsController(MeterRegistry meterRegistry) {
        if (!(meterRegistry instanceof PrometheusMeterRegistry)) {
            throw new IllegalArgumentException("Expected PrometheusMeterRegistry but got " + meterRegistry.getClass().getName());
        }
        this.meterRegistry = (PrometheusMeterRegistry) meterRegistry;
    }

    /**
     * Exposes Prometheus metrics.
     *
     * @return The Prometheus metrics
     */
    @GetMapping(produces = "text/plain")
    public ResponseEntity<String> getMetrics() {
        log.debug("Prometheus metrics requested");
        return ResponseEntity.ok(meterRegistry.scrape());
    }
}
