package com.gs.iceberg.ui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Entity representing a metric.
 */
@Entity
@Table(name = "metrics")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Metric {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "storage_configuration_id", nullable = false)
    private StorageConfiguration storageConfiguration;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private double value;

    @Column(nullable = false)
    private LocalDateTime timestamp;

    @Column(nullable = false)
    private String type; // "COUNTER", "GAUGE", "TIMER"

    private String tags;

    @OneToMany(mappedBy = "metric", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<SLO> slos = new ArrayList<>();

    @PrePersist
    protected void onCreate() {
        if (timestamp == null) {
            timestamp = LocalDateTime.now();
        }
    }

    /**
     * Adds an SLO to the metric.
     *
     * @param slo The SLO to add
     */
    public void addSLO(SLO slo) {
        slos.add(slo);
        slo.setMetric(this);
    }

    /**
     * Removes an SLO from the metric.
     *
     * @param slo The SLO to remove
     */
    public void removeSLO(SLO slo) {
        slos.remove(slo);
        slo.setMetric(null);
    }
}
