package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.FieldDefinition;
import com.gs.iceberg.ui.model.MessageSchema;
import com.gs.iceberg.ui.service.MessageSchemaService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for managing message schemas.
 */
@Controller
@RequestMapping("/schemas")
@Slf4j
public class MessageSchemaController {

    private final MessageSchemaService messageSchemaService;

    @Autowired
    public MessageSchemaController(MessageSchemaService messageSchemaService) {
        this.messageSchemaService = messageSchemaService;
    }

    /**
     * Displays the schema list page.
     *
     * @param model The model
     * @return The schema list page
     */
    @GetMapping
    public String listSchemas(Model model) {
        List<MessageSchema> schemas = messageSchemaService.getAllSchemas();
        model.addAttribute("schemas", schemas);
        return "schemas/list";
    }

    /**
     * Displays the schema creation page.
     *
     * @param model The model
     * @return The schema creation page
     */
    @GetMapping("/create")
    public String createSchemaForm(Model model) {
        model.addAttribute("schema", new MessageSchema());
        model.addAttribute("field", new FieldDefinition());
        return "schemas/create";
    }

    /**
     * Handles schema creation.
     *
     * @param schema The schema to create
     * @return Redirect to the schema list page
     */
    @PostMapping("/create")
    public String createSchema(@ModelAttribute MessageSchema schema) {
        messageSchemaService.createSchema(schema);
        return "redirect:/schemas";
    }

    /**
     * Displays the schema edit page.
     *
     * @param id The ID of the schema to edit
     * @param model The model
     * @return The schema edit page
     */
    @GetMapping("/edit/{id}")
    public String editSchemaForm(@PathVariable Long id, Model model) {
        messageSchemaService.getSchemaById(id).ifPresent(schema -> {
            model.addAttribute("schema", schema);
            model.addAttribute("field", new FieldDefinition());
        });
        return "schemas/edit";
    }

    /**
     * Handles schema update.
     *
     * @param id The ID of the schema to update
     * @param schema The updated schema
     * @return Redirect to the schema list page
     */
    @PostMapping("/edit/{id}")
    public String updateSchema(@PathVariable Long id, @ModelAttribute MessageSchema schema) {
        messageSchemaService.updateSchema(id, schema);
        return "redirect:/schemas";
    }

    /**
     * Handles schema deletion.
     *
     * @param id The ID of the schema to delete
     * @return Redirect to the schema list page
     */
    @GetMapping("/delete/{id}")
    public String deleteSchema(@PathVariable Long id) {
        messageSchemaService.deleteSchema(id);
        return "redirect:/schemas";
    }

    /**
     * Adds a field to a schema.
     *
     * @param id The ID of the schema
     * @param field The field to add
     * @return Redirect to the schema edit page
     */
    @PostMapping("/{id}/fields/add")
    public String addField(@PathVariable Long id, @ModelAttribute FieldDefinition field) {
        messageSchemaService.getSchemaById(id).ifPresent(schema -> {
            field.setSchema(schema);
            schema.getFields().add(field);
            messageSchemaService.updateSchema(id, schema);
        });
        return "redirect:/schemas/edit/" + id;
    }

    /**
     * Removes a field from a schema.
     *
     * @param id The ID of the schema
     * @param fieldId The ID of the field to remove
     * @return Redirect to the schema edit page
     */
    @GetMapping("/{id}/fields/remove/{fieldId}")
    public String removeField(@PathVariable Long id, @PathVariable Long fieldId) {
        messageSchemaService.getSchemaById(id).ifPresent(schema -> {
            schema.getFields().removeIf(field -> field.getId().equals(fieldId));
            messageSchemaService.updateSchema(id, schema);
        });
        return "redirect:/schemas/edit/" + id;
    }

    /**
     * REST API for getting all schemas.
     *
     * @return The list of schemas
     */
    @GetMapping("/api")
    @ResponseBody
    public ResponseEntity<List<MessageSchema>> getSchemas() {
        List<MessageSchema> schemas = messageSchemaService.getAllSchemas();
        return new ResponseEntity<>(schemas, HttpStatus.OK);
    }

    /**
     * REST API for getting a schema by ID.
     *
     * @param id The ID of the schema
     * @return The schema
     */
    @GetMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<MessageSchema> getSchema(@PathVariable Long id) {
        return messageSchemaService.getSchemaById(id)
                .map(schema -> new ResponseEntity<>(schema, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for creating a schema.
     *
     * @param schema The schema to create
     * @return The created schema
     */
    @PostMapping("/api")
    @ResponseBody
    public ResponseEntity<MessageSchema> createSchemaApi(@RequestBody MessageSchema schema) {
        MessageSchema createdSchema = messageSchemaService.createSchema(schema);
        return new ResponseEntity<>(createdSchema, HttpStatus.CREATED);
    }

    /**
     * REST API for updating a schema.
     *
     * @param id The ID of the schema to update
     * @param schema The updated schema
     * @return The updated schema
     */
    @PutMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<MessageSchema> updateSchemaApi(@PathVariable Long id, @RequestBody MessageSchema schema) {
        return messageSchemaService.updateSchema(id, schema)
                .map(updatedSchema -> new ResponseEntity<>(updatedSchema, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for deleting a schema.
     *
     * @param id The ID of the schema to delete
     * @return No content
     */
    @DeleteMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<Void> deleteSchemaApi(@PathVariable Long id) {
        messageSchemaService.deleteSchema(id);
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
}
