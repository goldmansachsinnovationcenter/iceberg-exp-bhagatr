package com.gs.iceberg.ui.scheduler;

import com.gs.iceberg.ui.repository.ErrorMessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Scheduler for purging expired error messages.
 * Messages not processed in error queue for 1 hour will be purged automatically.
 */
@Component
@EnableScheduling
@Slf4j
public class ErrorMessagePurgeScheduler {

    private final ErrorMessageRepository errorMessageRepository;

    @Autowired
    public ErrorMessagePurgeScheduler(ErrorMessageRepository errorMessageRepository) {
        this.errorMessageRepository = errorMessageRepository;
    }

    /**
     * Purges expired error messages every 5 minutes.
     * Messages not processed in error queue for 1 hour will be purged.
     */
    @Scheduled(fixedRate = 300000) // 5 minutes
    @Transactional
    public void purgeExpiredErrorMessages() {
        log.info("Starting scheduled purge of expired error messages");
        
        LocalDateTime expiryTime = LocalDateTime.now().minusHours(1);
        int purgedCount = errorMessageRepository.deleteByExpiryTimeBefore(expiryTime);
        
        log.info("Purged {} expired error messages", purgedCount);
    }
}
