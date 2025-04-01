package com.gs.iceberg.ui.controller;

import com.gs.iceberg.ui.model.ErrorMessage;
import com.gs.iceberg.ui.service.ErrorMessageService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Controller for managing error messages.
 */
@Controller
@RequestMapping("/error-messages")
@Slf4j
public class ErrorMessageController {

    private final ErrorMessageService errorMessageService;

    @Autowired
    public ErrorMessageController(ErrorMessageService errorMessageService) {
        this.errorMessageService = errorMessageService;
    }

    /**
     * Displays the error message list page.
     *
     * @param model The model
     * @return The error message list page
     */
    @GetMapping
    public String listErrorMessages(Model model) {
        List<ErrorMessage> errorMessages = errorMessageService.getAllErrorMessages();
        model.addAttribute("errorMessages", errorMessages);
        return "error-messages/list";
    }

    /**
     * Displays the error message edit page.
     *
     * @param id The ID of the error message to edit
     * @param model The model
     * @return The error message edit page
     */
    @GetMapping("/edit/{id}")
    public String editErrorMessageForm(@PathVariable Long id, Model model) {
        errorMessageService.getErrorMessageById(id).ifPresent(errorMessage -> {
            model.addAttribute("errorMessage", errorMessage);
        });
        return "error-messages/edit";
    }

    /**
     * Handles error message update.
     *
     * @param id The ID of the error message to update
     * @param errorMessage The updated error message
     * @return Redirect to the error message list page
     */
    @PostMapping("/edit/{id}")
    public String updateErrorMessage(@PathVariable Long id, @ModelAttribute ErrorMessage errorMessage) {
        errorMessageService.updateErrorMessage(id, errorMessage);
        return "redirect:/error-messages";
    }

    /**
     * Handles error message reprocessing.
     *
     * @param id The ID of the error message to reprocess
     * @return Redirect to the error message list page
     */
    @GetMapping("/reprocess/{id}")
    public String reprocessErrorMessage(@PathVariable Long id) {
        errorMessageService.reprocessErrorMessage(id);
        return "redirect:/error-messages";
    }

    /**
     * Handles error message discarding.
     *
     * @param id The ID of the error message to discard
     * @return Redirect to the error message list page
     */
    @GetMapping("/discard/{id}")
    public String discardErrorMessage(@PathVariable Long id) {
        errorMessageService.discardErrorMessage(id);
        return "redirect:/error-messages";
    }

    /**
     * REST API for getting all error messages.
     *
     * @return The list of error messages
     */
    @GetMapping("/api")
    @ResponseBody
    public ResponseEntity<List<ErrorMessage>> getErrorMessages() {
        List<ErrorMessage> errorMessages = errorMessageService.getAllErrorMessages();
        return new ResponseEntity<>(errorMessages, HttpStatus.OK);
    }

    /**
     * REST API for getting error messages by status.
     *
     * @param status The status of the error messages
     * @return The list of error messages
     */
    @GetMapping("/api/status/{status}")
    @ResponseBody
    public ResponseEntity<List<ErrorMessage>> getErrorMessagesByStatus(@PathVariable String status) {
        List<ErrorMessage> errorMessages = errorMessageService.getErrorMessagesByStatus(status);
        return new ResponseEntity<>(errorMessages, HttpStatus.OK);
    }

    /**
     * REST API for getting an error message by ID.
     *
     * @param id The ID of the error message
     * @return The error message
     */
    @GetMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<ErrorMessage> getErrorMessage(@PathVariable Long id) {
        return errorMessageService.getErrorMessageById(id)
                .map(errorMessage -> new ResponseEntity<>(errorMessage, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for updating an error message.
     *
     * @param id The ID of the error message to update
     * @param errorMessage The updated error message
     * @return The updated error message
     */
    @PutMapping("/api/{id}")
    @ResponseBody
    public ResponseEntity<ErrorMessage> updateErrorMessageApi(@PathVariable Long id, @RequestBody ErrorMessage errorMessage) {
        return errorMessageService.updateErrorMessage(id, errorMessage)
                .map(updatedErrorMessage -> new ResponseEntity<>(updatedErrorMessage, HttpStatus.OK))
                .orElse(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * REST API for reprocessing an error message.
     *
     * @param id The ID of the error message to reprocess
     * @return Success or failure
     */
    @PostMapping("/api/{id}/reprocess")
    @ResponseBody
    public ResponseEntity<Void> reprocessErrorMessageApi(@PathVariable Long id) {
        boolean success = errorMessageService.reprocessErrorMessage(id);
        return success ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }

    /**
     * REST API for discarding an error message.
     *
     * @param id The ID of the error message to discard
     * @return Success or failure
     */
    @PostMapping("/api/{id}/discard")
    @ResponseBody
    public ResponseEntity<Void> discardErrorMessageApi(@PathVariable Long id) {
        boolean success = errorMessageService.discardErrorMessage(id);
        return success ? new ResponseEntity<>(HttpStatus.OK) : new ResponseEntity<>(HttpStatus.NOT_FOUND);
    }
}
