package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.Metric;
import com.gs.iceberg.ui.model.SLO;
import com.gs.iceberg.ui.service.MetricService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Controller for managing metrics and SLOs.
 */
@Controller
@RequestMapping("/metrics")
@Slf4j
public class MetricsController {

    private final MetricService metricService;

    @Autowired
    public MetricsController(MetricService metricService) {
        this.metricService = metricService;
    }

    /**
     * Displays the metrics dashboard page.
     *
     * @param model The model
     * @return The metrics dashboard page
     */
    @GetMapping
    public String metricsDashboard(Model model) {
        List<Metric> metrics = metricService.getAllMetrics();
        model.addAttribute("metrics", metrics);
        return "metrics/dashboard";
    }

    /**
     * Displays the SLO dashboard page.
     *
     * @param model The model
     * @return The SLO dashboard page
     */
    @GetMapping("/slo")
    public String sloDashboard(Model model) {
        List<SLO> slos = metricService.getAllSLOs();
        model.addAttribute("slos", slos);
        return "metrics/slo";
    }

    /**
     * Displays the SLO creation page.
     *
     * @param model The model
     * @return The SLO creation page
     */
    @GetMapping("/slo/create")
    public String createSLOForm(Model model) {
        model.addAttribute("slo", new SLO());
        model.addAttribute("metrics", metricService.getAllMetrics());
        return "metrics/create-slo";
    }

    /**
     * Handles SLO creation.
     *
     * @param slo The SLO to create
     * @return Redirect to the SLO dashboard page
     */
    @PostMapping("/slo/create")
    public String createSLO(@ModelAttribute SLO slo) {
        metricService.createSLO(slo);
        return "redirect:/metrics/slo";
    }

    /**
     * Displays the SLO edit page.
     *
     * @param id The ID of the SLO to edit
     * @param model The model
     * @return The SLO edit page
     */
    @GetMapping("/slo/edit/{id}")
    public String editSLOForm(@PathVariable Long id, Model model) {
        metricService.getSLOById(id).ifPresent(slo -> {
            model.addAttribute("slo", slo);
            model.addAttribute("metrics", metricService.getAllMetrics());
        });
        return "metrics/edit-slo";
    }

    /**
     * Handles SLO update.
     *
     * @param id The ID of the SLO to update
     * @param slo The updated SLO
     * @return Redirect to the SLO dashboard page
     */
    @PostMapping("/slo/edit/{id}")
    public String updateSLO(@PathVariable Long id, @ModelAttribute SLO slo) {
        metricService.updateSLO(id, slo);
        return "redirect:/metrics/slo";
    }

    /**
     * Handles SLO deletion.
     *
     * @param id The ID of the SLO to delete
     * @return Redirect to the SLO dashboard page
     */
    @GetMapping("/slo/delete/{id}")
    public String deleteSLO(@PathVariable Long id) {
        metricService.deleteSLO(id);
        return "redirect:/metrics/slo";
    }

    /**
     * REST API for getting all metrics.
     *
     * @return The list of metrics
     */
    @GetMapping("/api")
    @ResponseBody
    public ResponseEntity<List<Metric>> getMetrics() {
        List<Metric> metrics = metricService.getAllMetrics();
        return new ResponseEntity<>(metrics, HttpStatus.OK);
    }

    /**
     * REST API for getting metrics by name.
     *
     * @param name The name of the metrics
     * @return The list of metrics
     */
    @GetMapping("/api/name/{name}")
    @ResponseBody
    public ResponseEntity<List<Metric>> getMetricsByName(@PathVariable String name) {
        List<Metric> metrics = metricService.getMetricsByName(name);
        return new ResponseEntity<>(metrics, HttpStatus.OK);
    }

    /**
     * REST API for getting metrics by type.
     *
     * @param type The type of the metrics
     * @return The list of metrics
     */
    @GetMapping("/api/type/{type}")
    @ResponseBody
    public ResponseEntity<List<Metric>> getMetricsByType(@PathVariable String type) {
        List<Metric> metrics = metricService.getMetricsByType(type);
        return new ResponseEntity<>(metrics, HttpStatus.OK);
    }

    /**
     * REST API for getting metrics by time range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of metrics
     */
    @GetMapping("/api/time-range")
    @ResponseBody
    public ResponseEntity<List<Metric>> getMetricsByTimeRange(@RequestParam LocalDateTime startTime,
                                                           @RequestParam LocalDateTime endTime) {
        List<Metric> metrics = metricService.getMetricsByTimeRange(startTime, endTime);
        return new ResponseEntity<>(metrics, HttpStatus.OK);
    }

    /**
     * REST API for getting all SLOs.
     *
     * @return The list of SLOs
     */
    @GetMapping("/api/slo")
    @ResponseBody
    public ResponseEntity<List<SLO>> getSLOs() {
        List<SLO> slos = metricService.getAllSLOs();
        return new ResponseEntity<>(slos, HttpStatus.OK);
    }

    /**
     * REST API for getting SLOs by name.
     *
     * @param name The name of the SLOs
     * @return The list of SLOs
     */
    @GetMapping("/api/slo/name/{name}")
    @ResponseBody
    public ResponseEntity<List<SLO>> getSLOsByName(@PathVariable String name) {
        List<SLO> slos = metricService.getSLOsByName(name);
        return new ResponseEntity<>(slos, HttpStatus.OK);
    }

    /**
     * REST API for getting SLOs by status.
     *
     * @param status The status of the SLOs
     * @return The list of SLOs
     */
    @GetMapping("/api/slo/status/{status}")
    @ResponseBody
    public ResponseEntity<List<SLO>> getSLOsByStatus(@PathVariable String status) {
        List<SLO> slos = metricService.getSLOsByStatus(status);
        return new ResponseEntity<>(slos, HttpStatus.OK);
    }

    /**
     * REST API for getting SLOs by time range.
     *
     * @param startTime The start time
     * @param endTime The end time
     * @return The list of SLOs
     */
    @GetMapping("/api/slo/time-range")
    @ResponseBody
    public ResponseEntity<List<SLO>> getSLOsByTimeRange(@RequestParam LocalDateTime startTime,
                                                     @RequestParam LocalDateTime endTime) {
        List<SLO> slos = metricService.getSLOsByTimeRange(startTime, endTime);
        return new ResponseEntity<>(slos, HttpStatus.OK);
    }

    /**
     * REST API for getting an SLO by ID.
     *
     * @param id The ID of the SLO
     * @return The SLO
     */
    @GetMapping("/api/slo/{id}")
    @ResponseBody
    public ResponseEntity<SLO> getSLO(@PathVariable Long id) {
        return metricService.getSLOById(id)
                .map(slo -> new ResponseEntity<>(slo, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for creating an SLO.
     *
     * @param slo The SLO to create
     * @return The created SLO
     */
    @PostMapping("/api/slo")
    @ResponseBody
    public ResponseEntity<SLO> createSLOApi(@RequestBody SLO slo) {
        SLO createdSLO = metricService.createSLO(slo);
        return new ResponseEntity<>(createdSLO, HttpStatus.CREATED);
    }

    /**
     * REST API for updating an SLO.
     *
     * @param id The ID of the SLO to update
     * @param slo The updated SLO
     * @return The updated SLO
     */
    @PutMapping("/api/slo/{id}")
    @ResponseBody
    public ResponseEntity<SLO> updateSLOApi(@PathVariable Long id, @RequestBody SLO slo) {
        return metricService.updateSLO(id, slo)
                .map(updatedSLO -> new ResponseEntity<>(updatedSLO, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for deleting an SLO.
     *
     * @param id The ID of the SLO to delete
     * @return No content
     */
    @DeleteMapping("/api/slo/{id}")
    @ResponseBody
    public ResponseEntity<Void> deleteSLOApi(@PathVariable Long id) {
        metricService.deleteSLO(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
