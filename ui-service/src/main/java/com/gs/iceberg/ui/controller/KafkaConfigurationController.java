package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.KafkaConfiguration;
import com.gs.iceberg.ui.service.KafkaConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for managing Kafka configurations.
 */
@Controller
@RequestMapping("/kafka-configs")
@Slf4j
public class KafkaConfigurationController {

    private final KafkaConfigurationService kafkaConfigurationService;

    @Autowired
    public KafkaConfigurationController(KafkaConfigurationService kafkaConfigurationService) {
        this.kafkaConfigurationService = kafkaConfigurationService;
    }

    /**
     * Displays the Kafka configuration list page.
     *
     * @param model The model
     * @return The Kafka configuration list page
     */
    @GetMapping
    public String listConfigurations(Model model) {
        List<KafkaConfiguration> configurations = kafkaConfigurationService.getAllConfigurations();
        model.addAttribute("configurations", configurations);
        return "kafka-configs/list";
    }

    /**
     * Displays the Kafka configuration creation page.
     *
     * @param model The model
     * @return The Kafka configuration creation page
     */
    @GetMapping("/create")
    public String createConfigurationForm(Model model) {
        model.addAttribute("configuration", new KafkaConfiguration());
        return "kafka-configs/create";
    }

    /**
     * Handles Kafka configuration creation.
     *
     * @param configuration The Kafka configuration to create
     * @return Redirect to the Kafka configuration list page
     */
    @PostMapping("/create")
    public String createConfiguration(@ModelAttribute KafkaConfiguration configuration) {
        kafkaConfigurationService.createConfiguration(configuration);
        return "redirect:/kafka-configs";
    }

    /**
     * Displays the Kafka configuration edit page.
     *
     * @param id The ID of the Kafka configuration to edit
     * @param model The model
     * @return The Kafka configuration edit page
     */
    @GetMapping("/edit/{id}")
    public String editConfigurationForm(@PathVariable Long id, Model model) {
        kafkaConfigurationService.getConfigurationById(id).ifPresent(configuration -> {
            model.addAttribute("configuration", configuration);
        });
        return "kafka-configs/edit";
    }

    /**
     * Handles Kafka configuration update.
     *
     * @param id The ID of the Kafka configuration to update
     * @param configuration The updated Kafka configuration
     * @return Redirect to the Kafka configuration list page
     */
    @PostMapping("/edit/{id}")
    public String updateConfiguration(@PathVariable Long id, @ModelAttribute KafkaConfiguration configuration) {
        kafkaConfigurationService.updateConfiguration(id, configuration);
        return "redirect:/kafka-configs";
    }

    /**
     * Handles Kafka configuration deletion.
     *
     * @param id The ID of the Kafka configuration to delete
     * @return Redirect to the Kafka configuration list page
     */
    @GetMapping("/delete/{id}")
    public String deleteConfiguration(@PathVariable Long id) {
        kafkaConfigurationService.deleteConfiguration(id);
        return "redirect:/kafka-configs";
    }

    /**
     * REST API for getting all Kafka configurations.
     *
     * @return The list of Kafka configurations
     */
    @GetMapping("/api")
    @ResponseBody
    public ResponseEntity<List<KafkaConfiguration>> getConfigurations() {
        List<KafkaConfiguration> configurations = kafkaConfigurationService.getAllConfigurations();
        return new ResponseEntity<>(configurations, HttpStatus.OK);
    }

    /**
     * REST API for getting a Kafka configuration by ID.
     *
     * @param id The ID of the Kafka configuration
     * @return The Kafka configuration
     */
    @GetMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<KafkaConfiguration> getConfiguration(@PathVariable Long id) {
        return kafkaConfigurationService.getConfigurationById(id)
                .map(configuration -> new ResponseEntity<>(configuration, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for creating a Kafka configuration.
     *
     * @param configuration The Kafka configuration to create
     * @return The created Kafka configuration
     */
    @PostMapping("/api")
    @ResponseBody
    public ResponseEntity<KafkaConfiguration> createConfigurationApi(@RequestBody KafkaConfiguration configuration) {
        KafkaConfiguration createdConfiguration = kafkaConfigurationService.createConfiguration(configuration);
        return new ResponseEntity<>(createdConfiguration, HttpStatus.CREATED);
    }

    /**
     * REST API for updating a Kafka configuration.
     *
     * @param id The ID of the Kafka configuration to update
     * @param configuration The updated Kafka configuration
     * @return The updated Kafka configuration
     */
    @PutMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<KafkaConfiguration> updateConfigurationApi(@PathVariable Long id, @RequestBody KafkaConfiguration configuration) {
        return kafkaConfigurationService.updateConfiguration(id, configuration)
                .map(updatedConfiguration -> new ResponseEntity<>(updatedConfiguration, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for deleting a Kafka configuration.
     *
     * @param id The ID of the Kafka configuration to delete
     * @return No content
     */
    @DeleteMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<Void> deleteConfigurationApi(@PathVariable Long id) {
        kafkaConfigurationService.deleteConfiguration(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
