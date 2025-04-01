# User Guide

This guide provides instructions for setting up, configuring, and using the Kafka-Iceberg application.

## Table of Contents

1. [Installation](#installation)
2. [Configuration](#configuration)
3. [Using the UI](#using-the-ui)
4. [Querying Data](#querying-data)
5. [Monitoring](#monitoring)
6. [Troubleshooting](#troubleshooting)

## Installation

### Prerequisites

- Java 11 or higher
- Apache Spark 3.2 or higher
- Apache Kafka
- HDFS or S3 storage
- Kerberos authentication setup

### Ingestion Service

1. Clone the repository:
   ```bash
   git clone https://github.com/goldmansachsinnovationcenter/iceberg-exp-bhagatr.git
   cd iceberg-exp-bhagatr
   ```

2. Build the ingestion service:
   ```bash
   cd ingestion-service
   ./gradlew build
   ```

3. Launch the ingestion service:
   ```bash
   ./scripts/start-ingestion-service.sh
   ```

### UI Service

1. Build the UI service:
   ```bash
   cd ui-service
   ./gradlew build
   ```

2. Launch the UI service:
   ```bash
   ./scripts/start-ui-service.sh
   ```

## Configuration

### Message Schema Configuration

1. Navigate to the UI at `http://localhost:8080`
2. Go to the "Configuration" tab
3. Select "Message Schema" from the sidebar
4. Click "Add New Schema"
5. Enter the schema details:
   - Name: A descriptive name for the schema
   - Description: A brief description of the schema
   - Fields: Add fields with name, type, and whether they are required
6. Click "Save" to save the schema

### Kafka Configuration

1. Go to the "Configuration" tab
2. Select "Kafka" from the sidebar
3. Click "Add New Configuration"
4. Enter the Kafka details:
   - Name: A descriptive name for the configuration
   - Bootstrap Servers: Comma-separated list of Kafka bootstrap servers
   - Topic: Kafka topic to consume from
   - Consumer Group: Consumer group ID
   - Security Protocol: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
   - SASL Mechanism: SASL mechanism (if applicable)
   - Keytab Path: Path to the Kerberos keytab file
   - Principal: Kerberos principal
   - Consumption Rate: Message consumption rate (messages/minute)
5. Click "Save" to save the configuration

### Storage Configuration

1. Go to the "Configuration" tab
2. Select "Storage" from the sidebar
3. Click "Add New Configuration"
4. Enter the storage details:
   - Name: A descriptive name for the configuration
   - Storage Type: Type of storage (HDFS, S3)
   - Base Path: Base path for storage
   - Retention Days: Data retention period in days
   - For HDFS:
     - Keytab Path: Path to the Kerberos keytab file
     - Principal: Kerberos principal
   - For S3:
     - Access Key: Access key
     - Secret Key: Secret key
     - Region: Region
5. Click "Save" to save the configuration

## Using the UI

### Dashboard

The dashboard provides an overview of the system, including:
- Number of messages processed
- Ingestion rate
- Lag at partition level
- Number of stored records
- Error queue status

### Configuration Tab

The configuration tab allows you to configure:
- Message schemas
- Kafka connections
- Storage settings

### Query Tab

The query tab allows you to search and query data based on message fields:
1. Select the fields to search on
2. Enter the search criteria
3. Click "Search" to execute the query
4. View the results in the table
5. Export the results to CSV or JSON

### Error Queue Tab

The error queue tab allows you to manage error messages:
1. View error messages in the queue
2. Click on a message to view details
3. Edit the message if needed
4. Click "Reprocess" to reprocess the message
5. Click "Discard" to discard the message

### Statistics Tab

The statistics tab provides metrics about the system:
- Messages read from Kafka
- Ingestion rate
- Lag at partition level
- Number of stored records
- Storage usage

### SLO/SLI Tab

The SLO/SLI tab provides monitoring of service level objectives and indicators:
- Performance metrics
- Availability metrics
- Error rate metrics
- Latency metrics

## Querying Data

### Basic Query

1. Go to the "Query" tab
2. Select the fields to include in the results
3. Add filter conditions:
   - Select a field
   - Select an operator (equals, not equals, greater than, less than, etc.)
   - Enter a value
4. Click "Search" to execute the query
5. View the results in the table

### Advanced Query

1. Go to the "Query" tab
2. Click "Advanced Query"
3. Enter a SQL query in the text area
4. Click "Execute" to run the query
5. View the results in the table

### Bulk Query

1. Go to the "Query" tab
2. Click "Bulk Query"
3. Upload a CSV file with query parameters
4. Click "Execute" to run the bulk query
5. Download the results as a CSV or JSON file

## Monitoring

### Metrics

The application exposes metrics through a Prometheus endpoint at `/actuator/prometheus`. These metrics include:
- Messages read from Kafka
- Ingestion rate
- Lag at partition level
- Number of stored records
- Query performance
- Error rate

### Alerts

The application can be configured to send alerts when certain thresholds are reached:
1. Go to the "Configuration" tab
2. Select "Alerts" from the sidebar
3. Click "Add New Alert"
4. Enter the alert details:
   - Name: A descriptive name for the alert
   - Metric: The metric to monitor
   - Threshold: The threshold value
   - Operator: The comparison operator (greater than, less than, etc.)
   - Notification Channel: Where to send the alert (email, Slack, etc.)
5. Click "Save" to save the alert

## Troubleshooting

### Common Issues

#### Ingestion Service Not Starting

1. Check the logs in `logs/ingestion-service.log`
2. Verify that Kafka is running and accessible
3. Verify that the Kerberos keytab file is valid and accessible
4. Verify that Spark is installed and configured correctly

#### UI Service Not Starting

1. Check the logs in `logs/ui-service.log`
2. Verify that the database is running and accessible
3. Verify that the required ports are available

#### Error Messages Not Being Processed

1. Check the error queue in the UI
2. Verify that the error message format is correct
3. Edit the message if needed and reprocess

#### Queries Not Returning Results

1. Verify that data is being ingested correctly
2. Check the query parameters
3. Verify that the Iceberg table is accessible
4. Check the logs for query errors

### Getting Help

If you encounter issues that are not covered in this guide, please:
1. Check the logs for error messages
2. Consult the [Troubleshooting Guide](troubleshooting-guide.md) for more detailed information
3. Contact the support team at support@example.com
