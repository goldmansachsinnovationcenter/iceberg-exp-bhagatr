package com.gs.iceberg.ui.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

/**
 * Entity representing a field definition in a JSON message schema.
 */
@Entity
@Table(name = "field_definitions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FieldDefinition {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "schema_id", nullable = false)
    private MessageSchema schema;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String type;

    @Column(nullable = false)
    private boolean required;

    @Column(length = 1000)
    private String description;
}
