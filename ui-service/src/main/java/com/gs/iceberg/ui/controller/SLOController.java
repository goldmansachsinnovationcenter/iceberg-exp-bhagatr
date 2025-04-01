package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.SLO;
import com.gs.iceberg.ui.service.MetricService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for SLO/SLI monitoring.
 */
@Controller
@RequestMapping("/metrics/slo")
@Slf4j
public class SLOController {

    private final MetricService metricService;

    @Autowired
    public SLOController(MetricService metricService) {
        this.metricService = metricService;
    }

    /**
     * Displays the SLO/SLI monitoring page.
     *
     * @param model The model
     * @return The SLO/SLI monitoring page
     */
    @GetMapping
    public String sloPage(Model model) {
        List<SLO> slos = metricService.getAllSLOs();
        model.addAttribute("slos", slos);
        return "metrics/slo";
    }

    /**
     * Creates a new SLO.
     *
     * @param slo The SLO to create
     * @return Redirect to the SLO/SLI monitoring page
     */
    @PostMapping
    public String createSLO(@ModelAttribute SLO slo) {
        log.info("Creating SLO: {}", slo.getName());
        metricService.createSLO(slo);
        return "redirect:/metrics/slo";
    }

    /**
     * Updates an existing SLO.
     *
     * @param id The ID of the SLO to update
     * @param slo The updated SLO
     * @return Redirect to the SLO/SLI monitoring page
     */
    @PostMapping("/{id}")
    public String updateSLO(@PathVariable Long id, @ModelAttribute SLO slo) {
        log.info("Updating SLO with ID: {}", id);
        metricService.updateSLO(id, slo);
        return "redirect:/metrics/slo";
    }

    /**
     * Deletes an SLO.
     *
     * @param id The ID of the SLO to delete
     * @return Redirect to the SLO/SLI monitoring page
     */
    @GetMapping("/delete/{id}")
    public String deleteSLO(@PathVariable Long id) {
        log.info("Deleting SLO with ID: {}", id);
        metricService.deleteSLO(id);
        return "redirect:/metrics/slo";
    }
}
