package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.StorageConfiguration;
import com.gs.iceberg.ui.service.StorageConfigurationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for managing storage configurations.
 */
@Controller
@RequestMapping("/storage-configs")
@Slf4j
public class StorageConfigurationController {

    private final StorageConfigurationService storageConfigurationService;

    @Autowired
    public StorageConfigurationController(StorageConfigurationService storageConfigurationService) {
        this.storageConfigurationService = storageConfigurationService;
    }

    /**
     * Displays the storage configuration list page.
     *
     * @param model The model
     * @return The storage configuration list page
     */
    @GetMapping
    public String listConfigurations(Model model) {
        List<StorageConfiguration> configurations = storageConfigurationService.getAllConfigurations();
        model.addAttribute("configurations", configurations);
        return "storage-configs/list";
    }

    /**
     * Displays the storage configuration creation page.
     *
     * @param model The model
     * @return The storage configuration creation page
     */
    @GetMapping("/create")
    public String createConfigurationForm(Model model) {
        model.addAttribute("configuration", new StorageConfiguration());
        return "storage-configs/create";
    }

    /**
     * Handles storage configuration creation.
     *
     * @param configuration The storage configuration to create
     * @return Redirect to the storage configuration list page
     */
    @PostMapping("/create")
    public String createConfiguration(@ModelAttribute StorageConfiguration configuration) {
        storageConfigurationService.createConfiguration(configuration);
        return "redirect:/storage-configs";
    }

    /**
     * Displays the storage configuration edit page.
     *
     * @param id The ID of the storage configuration to edit
     * @param model The model
     * @return The storage configuration edit page
     */
    @GetMapping("/edit/{id}")
    public String editConfigurationForm(@PathVariable Long id, Model model) {
        storageConfigurationService.getConfigurationById(id).ifPresent(configuration -> {
            model.addAttribute("configuration", configuration);
        });
        return "storage-configs/edit";
    }

    /**
     * Handles storage configuration update.
     *
     * @param id The ID of the storage configuration to update
     * @param configuration The updated storage configuration
     * @return Redirect to the storage configuration list page
     */
    @PostMapping("/edit/{id}")
    public String updateConfiguration(@PathVariable Long id, @ModelAttribute StorageConfiguration configuration) {
        storageConfigurationService.updateConfiguration(id, configuration);
        return "redirect:/storage-configs";
    }

    /**
     * Handles storage configuration deletion.
     *
     * @param id The ID of the storage configuration to delete
     * @return Redirect to the storage configuration list page
     */
    @GetMapping("/delete/{id}")
    public String deleteConfiguration(@PathVariable Long id) {
        storageConfigurationService.deleteConfiguration(id);
        return "redirect:/storage-configs";
    }

    /**
     * REST API for getting all storage configurations.
     *
     * @return The list of storage configurations
     */
    @GetMapping("/api")
    @ResponseBody
    public ResponseEntity<List<StorageConfiguration>> getConfigurations() {
        List<StorageConfiguration> configurations = storageConfigurationService.getAllConfigurations();
        return new ResponseEntity<>(configurations, HttpStatus.OK);
    }

    /**
     * REST API for getting a storage configuration by ID.
     *
     * @param id The ID of the storage configuration
     * @return The storage configuration
     */
    @GetMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<StorageConfiguration> getConfiguration(@PathVariable Long id) {
        return storageConfigurationService.getConfigurationById(id)
                .map(configuration -> new ResponseEntity<>(configuration, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for creating a storage configuration.
     *
     * @param configuration The storage configuration to create
     * @return The created storage configuration
     */
    @PostMapping("/api")
    @ResponseBody
    public ResponseEntity<StorageConfiguration> createConfigurationApi(@RequestBody StorageConfiguration configuration) {
        StorageConfiguration createdConfiguration = storageConfigurationService.createConfiguration(configuration);
        return new ResponseEntity<>(createdConfiguration, HttpStatus.CREATED);
    }

    /**
     * REST API for updating a storage configuration.
     *
     * @param id The ID of the storage configuration to update
     * @param configuration The updated storage configuration
     * @return The updated storage configuration
     */
    @PutMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<StorageConfiguration> updateConfigurationApi(@PathVariable Long id, @RequestBody StorageConfiguration configuration) {
        return storageConfigurationService.updateConfiguration(id, configuration)
                .map(updatedConfiguration -> new ResponseEntity<>(updatedConfiguration, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for deleting a storage configuration.
     *
     * @param id The ID of the storage configuration to delete
     * @return No content
     */
    @DeleteMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<Void> deleteConfigurationApi(@PathVariable Long id) {
        storageConfigurationService.deleteConfiguration(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
