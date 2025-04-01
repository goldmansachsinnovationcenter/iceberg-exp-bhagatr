# Design Documentation

## Architecture Overview

This document outlines the architecture and design decisions for the Kafka-Iceberg application.

### System Architecture

The application is split into two main components:

1. **Ingestion Service**: Responsible for consuming JSON messages from Kafka and storing them in HDFS/S3 using Apache Iceberg.
2. **UI Service**: Provides a web interface for configuration, querying, and monitoring.

![Architecture Diagram](architecture-diagram.png)

### Component Interaction

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Kafka Topics   │────▶│ Ingestion       │────▶│ HDFS/S3         │
│                 │     │ Service         │     │ (Iceberg)       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │                        │
                               │                        │
                               ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │                 │     │                 │
                        │  Error Queue    │◀───▶│  UI Service     │
                        │  (Kafka)        │     │                 │
                        └─────────────────┘     └─────────────────┘
```

## Ingestion Service

### Components

1. **Kafka Consumer**: Consumes JSON messages from Kafka topics.
2. **Schema Validator**: Validates messages against the defined schema.
3. **Spark Streaming Job**: Processes messages in batches and writes to Iceberg.
4. **Error Handler**: Pushes invalid messages to an error queue.
5. **Metrics Collector**: Tracks metrics for monitoring.

### Technologies

- **Apache Spark**: For streaming and processing data.
- **Apache Kafka**: For message consumption and error queue.
- **Apache Iceberg**: For table format and query capabilities.
- **Kerberos**: For authentication with Kafka, HDFS, and S3.
- **Micrometer**: For metrics collection and monitoring.

## UI Service

### Components

1. **Web UI**: React-based frontend for user interaction.
2. **REST API**: Backend services for configuration, querying, and monitoring.
3. **Schema Manager**: Manages JSON schemas for validation.
4. **Query Engine**: Provides search/query capabilities using Iceberg.
5. **Error Queue Manager**: Manages error messages and reprocessing.
6. **Metrics Dashboard**: Displays system metrics and statistics.

### Technologies

- **Spring Boot**: For the backend REST API.
- **React**: For the frontend UI.
- **H2 Database**: In-memory database for app data storage.
- **SQLModel**: ORM for database interactions.
- **Micrometer**: For metrics collection and Prometheus integration.

## Data Flow

1. JSON messages are consumed from Kafka by the Ingestion Service.
2. Messages are validated against the defined schema.
3. Valid messages are processed by Spark Streaming and stored in HDFS/S3 using Iceberg.
4. Invalid messages are pushed to an error queue in Kafka.
5. The UI Service provides interfaces for configuration, querying, and monitoring.
6. Users can reprocess or discard error messages through the UI.

## Authentication

Kerberos keytab file-based authentication is used for:
- Kafka: For consuming messages and pushing to error queue.
- HDFS: For storing data using Iceberg.
- S3: As an alternative storage backend.

## Metrics and Monitoring

- **Message Metrics**: Messages read, ingestion rate, lag at partition level.
- **Storage Metrics**: Number of stored records, storage usage.
- **Performance Metrics**: Processing time, query performance.
- **SLO/SLI Monitoring**: Service level objectives and indicators.

## Error Handling

1. Invalid messages are pushed to a Kafka error queue.
2. The UI provides a way to view, edit, and reprocess error messages.
3. Messages in the error queue are automatically purged after 1 hour.

## Storage Configuration

The application supports both HDFS and S3 as backend storage:
- Configuration options in the UI allow switching between the two.
- Data retention is configurable with a default of 10 days.
