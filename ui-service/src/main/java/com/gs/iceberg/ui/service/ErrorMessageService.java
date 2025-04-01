package com.gs.iceberg.ui.service;

import com.gs.iceberg.ui.model.ErrorMessage;
import com.gs.iceberg.ui.repository.ErrorMessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing error messages.
 */
@Service
@Slf4j
public class ErrorMessageService {

    private final ErrorMessageRepository errorMessageRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public ErrorMessageService(ErrorMessageRepository errorMessageRepository,
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.errorMessageRepository = errorMessageRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Gets all error messages.
     *
     * @return The list of error messages
     */
    public List<ErrorMessage> getAllErrorMessages() {
        return errorMessageRepository.findAll();
    }

    /**
     * Gets error messages by status.
     *
     * @param status The status of the error messages
     * @return The list of error messages
     */
    public List<ErrorMessage> getErrorMessagesByStatus(String status) {
        return errorMessageRepository.findByStatus(status);
    }

    /**
     * Gets an error message by ID.
     *
     * @param id The ID of the error message
     * @return The error message, if found
     */
    public Optional<ErrorMessage> getErrorMessageById(Long id) {
        return errorMessageRepository.findById(id);
    }

    /**
     * Creates a new error message.
     *
     * @param errorMessage The error message to create
     * @return The created error message
     */
    @Transactional
    public ErrorMessage createErrorMessage(ErrorMessage errorMessage) {
        log.info("Creating error message");
        return errorMessageRepository.save(errorMessage);
    }

    /**
     * Updates an existing error message.
     *
     * @param id The ID of the error message to update
     * @param errorMessage The updated error message
     * @return The updated error message, if found
     */
    @Transactional
    public Optional<ErrorMessage> updateErrorMessage(Long id, ErrorMessage errorMessage) {
        log.info("Updating error message with ID: {}", id);
        return errorMessageRepository.findById(id)
                .map(existingErrorMessage -> {
                    existingErrorMessage.setOriginalMessage(errorMessage.getOriginalMessage());
                    existingErrorMessage.setErrorReason(errorMessage.getErrorReason());
                    existingErrorMessage.setStatus(errorMessage.getStatus());
                    
                    return errorMessageRepository.save(existingErrorMessage);
                });
    }

    /**
     * Reprocesses an error message.
     *
     * @param id The ID of the error message to reprocess
     * @return true if the message was reprocessed, false otherwise
     */
    @Transactional
    public boolean reprocessErrorMessage(Long id) {
        log.info("Reprocessing error message with ID: {}", id);
        return errorMessageRepository.findById(id)
                .map(errorMessage -> {
                    kafkaTemplate.send(errorMessage.getKafkaConfiguration().getTopic(), 
                                      errorMessage.getOriginalMessage());
                    
                    errorMessage.setStatus("REPROCESSING");
                    errorMessageRepository.save(errorMessage);
                    
                    return true;
                })
                .orElse(false);
    }

    /**
     * Discards an error message.
     *
     * @param id The ID of the error message to discard
     * @return true if the message was discarded, false otherwise
     */
    @Transactional
    public boolean discardErrorMessage(Long id) {
        log.info("Discarding error message with ID: {}", id);
        return errorMessageRepository.findById(id)
                .map(errorMessage -> {
                    errorMessage.setStatus("DISCARDED");
                    errorMessageRepository.save(errorMessage);
                    
                    return true;
                })
                .orElse(false);
    }

    /**
     * Purges expired error messages.
     * Runs every hour to purge messages that have been in the error queue for more than 1 hour.
     */
    @Scheduled(fixedRate = 3600000) // Every hour
    @Transactional
    public void purgeExpiredMessages() {
        log.info("Purging expired error messages");
        LocalDateTime now = LocalDateTime.now();
        int deletedCount = errorMessageRepository.deleteByExpiryTimeBefore(now);
        log.info("Purged {} expired error messages", deletedCount);
    }
    
    /**
     * Reprocesses all error messages with a specific status.
     *
     * @param status The status of the error messages to reprocess
     * @return The number of reprocessed messages
     */
    @Transactional
    public int reprocessErrorMessagesByStatus(String status) {
        log.info("Reprocessing error messages with status: {}", status);
        List<ErrorMessage> errorMessages = errorMessageRepository.findByStatus(status);
        
        int count = 0;
        for (ErrorMessage errorMessage : errorMessages) {
            if (reprocessErrorMessage(errorMessage.getId())) {
                count++;
            }
        }
        
        log.info("Reprocessed {} error messages", count);
        return count;
    }
    
    /**
     * Discards all error messages with a specific status.
     *
     * @param status The status of the error messages to discard
     * @return The number of discarded messages
     */
    @Transactional
    public int discardErrorMessagesByStatus(String status) {
        log.info("Discarding error messages with status: {}", status);
        List<ErrorMessage> errorMessages = errorMessageRepository.findByStatus(status);
        
        int count = 0;
        for (ErrorMessage errorMessage : errorMessages) {
            if (discardErrorMessage(errorMessage.getId())) {
                count++;
            }
        }
        
        log.info("Discarded {} error messages", count);
        return count;
    }
}
