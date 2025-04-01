# Kafka-Iceberg Application

This application reads structured JSON messages from Kafka, stores them in HDFS/S3 using Apache Iceberg, and provides query capabilities through a web UI.

## Components

### Ingestion Service
- Reads JSON messages from Kafka using Spark Streaming
- Stores data in HDFS/S3 using Apache Iceberg
- Handles error messages and pushes them to an error queue
- Tracks metrics for monitoring

### UI Service
- Provides a web interface for configuration, querying, and monitoring
- Allows schema definition for JSON messages
- Provides search/query capabilities based on message fields
- Displays system metrics and statistics
- Allows reprocessing of error messages

## Features
- Kerberos keytab file-based authentication for Kafka, HDFS, and S3
- Configurable message consumption rate (default: 100/minute)
- Search/query capabilities based on any message field or combination
- Error handling with reprocessing queue
- Metrics tracking and monitoring
- Ability to switch between HDFS and S3 as backend storage
- Data retention configuration (default: 10 days)

## Getting Started
See the [User Guide](docs/user-guide/README.md) for detailed instructions on setting up and using the application.

## Development
See the [Design Documentation](docs/design/README.md) for information on the architecture and implementation details.
