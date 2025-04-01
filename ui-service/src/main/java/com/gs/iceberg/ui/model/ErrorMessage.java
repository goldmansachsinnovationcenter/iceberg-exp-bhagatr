package com.gs.iceberg.ui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Entity representing an error message.
 */
@Entity
@Table(name = "error_messages")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "kafka_configuration_id", nullable = false)
    private KafkaConfiguration kafkaConfiguration;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String originalMessage;

    @Column(nullable = false)
    private String errorReason;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @Column(nullable = false)
    private String status; // "NEW", "REPROCESSING", "DISCARDED"

    @Column(nullable = false)
    private LocalDateTime expiryTime;

    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }
        if (status == null) {
            status = "NEW";
        }
        if (expiryTime == null) {
            expiryTime = timestamp.plusHours(1);
        }
    }
}
