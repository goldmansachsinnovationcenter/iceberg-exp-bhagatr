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
 * Entity representing a JSON message schema.
 */
@Entity
@Table(name = "message_schemas")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageSchema {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(length = 1000)
    private String description;

    @OneToMany(mappedBy = "schema", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<FieldDefinition> fields = new ArrayList<>();

    @Column(nullable = false)
    private LocalDateTime createdAt;

    @Column(nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    /**
     * Adds a field to the schema.
     *
     * @param field The field to add
     */
    public void addField(FieldDefinition field) {
        fields.add(field);
        field.setSchema(this);
    }

    /**
     * Removes a field from the schema.
     *
     * @param field The field to remove
     */
    public void removeField(FieldDefinition field) {
        fields.remove(field);
        field.setSchema(null);
    }
}
