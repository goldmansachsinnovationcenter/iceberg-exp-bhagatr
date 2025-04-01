package com.gs.iceberg.ui.repository;

import com.gs.iceberg.ui.model.ErrorMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Repository for ErrorMessage entities.
 */
@Repository
public interface ErrorMessageRepository extends JpaRepository<ErrorMessage, Long> {
    
    /**
     * Finds error messages by status.
     *
     * @param status The status of the error messages
     * @return The list of error messages
     */
    List<ErrorMessage> findByStatus(String status);
    
    /**
     * Finds error messages by Kafka configuration ID.
     *
     * @param kafkaConfigurationId The ID of the Kafka configuration
     * @return The list of error messages
     */
    List<ErrorMessage> findByKafkaConfigurationId(Long kafkaConfigurationId);
    
    /**
     * Finds error messages that have expired.
     *
     * @param expiryTime The expiry time
     * @return The list of expired error messages
     */
    List<ErrorMessage> findByExpiryTimeBefore(LocalDateTime expiryTime);
    
    /**
     * Deletes error messages that have expired.
     *
     * @param expiryTime The expiry time
     * @return The number of deleted error messages
     */
    @Query("DELETE FROM ErrorMessage e WHERE e.expiryTime < ?1")
    int deleteByExpiryTimeBefore(LocalDateTime expiryTime);
}
