package com.gs.iceberg.ui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;

/**
 * Entity representing a Service Level Objective (SLO).
 */
@Entity
@Table(name = "slos")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SLO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "metric_id", nullable = false)
    private Metric metric;

    @Column(nullable = false)
    private String name;

    @Column(length = 1000)
    private String description;

    @Column(nullable = false)
    private double target;

    @Column(nullable = false)
    private double actualValue;

    @Column(nullable = false)
    private String status; // "MET", "NOT_MET"

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }
        updateStatus();
    }

    @PreUpdate
    protected void onUpdate() {
        updateStatus();
    }

    /**
     * Updates the status based on the target and actual value.
     */
    private void updateStatus() {
        status = actualValue >= target ? "MET" : "NOT_MET";
    }
}
