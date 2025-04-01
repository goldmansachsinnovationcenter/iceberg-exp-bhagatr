# Kafka-Iceberg Application User Manual

This user manual provides comprehensive guidance on using the Kafka-Iceberg application. It covers all aspects of the application, from configuring message schemas to querying data and monitoring system performance.

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Message Configuration](#message-configuration)
4. [Query Interface](#query-interface)
5. [Error Queue Management](#error-queue-management)
6. [Statistics and Metrics](#statistics-and-metrics)
7. [SLO/SLI Monitoring](#slosli-monitoring)
8. [Storage Configuration](#storage-configuration)
9. [Troubleshooting](#troubleshooting)

## Introduction

The Kafka-Iceberg application is a powerful tool for processing structured JSON messages from Kafka topics and storing them in HDFS or S3 using Apache Iceberg. The application provides a user-friendly web interface for configuring message schemas, monitoring system metrics, querying stored data, and managing error messages.

### Key Features

- JSON message processing from Kafka topics
- Flexible storage options (HDFS or S3)
- Schema-based message validation
- Advanced query capabilities
- Error message management and reprocessing
- Comprehensive metrics and monitoring
- SLO/SLI tracking

## Getting Started

### Accessing the Application

1. Open your web browser and navigate to `http://[application-host]:8080`
2. You will be presented with the home page, which provides an overview of the application and quick links to key features

### Navigation

The application has a navigation bar at the top of the page with the following options:

- **Home**: Returns to the main dashboard
- **Message Schemas**: Manage JSON message schemas
- **Kafka Configs**: Configure Kafka connection settings
- **Storage Configs**: Manage storage backend configurations
- **Query**: Search and query stored data
- **Error Queue**: Manage messages that failed processing
- **Metrics**: View system metrics and statistics
- **SLO/SLIs**: Monitor service level objectives and indicators

## Message Configuration

The Message Configuration tab allows you to define the structure of JSON messages that will be processed by the application.

### Creating a New Message Schema

1. Navigate to the **Message Schemas** page
2. Click the **Create New Schema** button
3. Fill in the following information:
   - **Name**: A unique name for the schema
   - **Description**: A brief description of the schema
   - **Fields**: Define the fields in the JSON message
     - **Field Name**: The name of the field in the JSON message
     - **Field Type**: The data type of the field (string, integer, boolean, timestamp, etc.)
     - **Required**: Whether the field is required or optional
4. Click **Save** to create the schema

### Editing an Existing Schema

1. Navigate to the **Message Schemas** page
2. Find the schema you want to edit in the list
3. Click the **Edit** button next to the schema
4. Modify the schema information as needed
5. Click **Save** to update the schema

### Deleting a Schema

1. Navigate to the **Message Schemas** page
2. Find the schema you want to delete in the list
3. Click the **Delete** button next to the schema
4. Confirm the deletion when prompted

### Configuring Kafka Connection

1. Navigate to the **Kafka Configs** page
2. Click the **Create New Configuration** button
3. Fill in the following information:
   - **Name**: A unique name for the configuration
   - **Bootstrap Servers**: The Kafka bootstrap servers (e.g., `kafka:9092`)
   - **Topic**: The Kafka topic to consume messages from
   - **Group ID**: The consumer group ID
   - **Security Protocol**: The security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
   - **SASL Mechanism**: The SASL mechanism to use (if applicable)
   - **Kerberos Configuration**:
     - **Keytab Path**: The path to the Kerberos keytab file
     - **Principal**: The Kerberos principal
   - **Message Consumption Rate**: The rate at which messages are consumed (messages per minute)
4. Click **Save** to create the configuration

## Query Interface

The Query Interface allows you to search and retrieve data stored in the system based on various criteria.

### Basic Query

1. Navigate to the **Query** page
2. Select a message schema from the dropdown menu
3. Enter your search criteria in the filter fields
   - You can filter by any field defined in the schema
   - For string fields, you can use exact matches or wildcards
   - For numeric fields, you can use exact values or ranges
   - For boolean fields, you can select true or false
   - For timestamp fields, you can select a date range
4. Click **Search** to execute the query
5. The results will be displayed in a table below the search form

### Advanced Query

1. Navigate to the **Query** page
2. Click the **Advanced Query** tab
3. Select a message schema from the dropdown menu
4. Build your query using the advanced query builder
   - Add multiple conditions with AND/OR operators
   - Use complex filters with operators like equals, not equals, greater than, less than, contains, etc.
   - Group conditions together for more complex queries
5. Click **Execute Query** to run the query
6. The results will be displayed in a table below the query builder

### Exporting Query Results

1. Execute a query as described above
2. Click the **Export** button above the results table
3. Select the export format (CSV, JSON, or Excel)
4. The file will be downloaded to your computer

### Saving Queries

1. Build a query using either the basic or advanced query interface
2. Click the **Save Query** button
3. Enter a name and description for the query
4. Click **Save**
5. The query will be saved and can be accessed from the **Saved Queries** tab

## Error Queue Management

The Error Queue Management interface allows you to view, edit, reprocess, or discard messages that failed processing.

### Viewing Error Messages

1. Navigate to the **Error Queue** page
2. The page displays a list of all error messages with the following information:
   - **ID**: The unique identifier for the error message
   - **Message**: A preview of the message content
   - **Error Reason**: The reason the message failed processing
   - **Created At**: When the message was added to the error queue
   - **Actions**: Buttons to edit, reprocess, or discard the message

### Editing an Error Message

1. Navigate to the **Error Queue** page
2. Find the message you want to edit in the list
3. Click the **Edit** button next to the message
4. The message content will be displayed in a JSON editor
5. Make the necessary changes to fix the issue
6. Click **Save** to update the message

### Reprocessing an Error Message

1. Navigate to the **Error Queue** page
2. Find the message you want to reprocess in the list
3. Click the **Reprocess** button next to the message
4. The message will be sent back to the processing pipeline
5. If successful, the message will be removed from the error queue
6. If unsuccessful, the message will remain in the error queue with an updated error reason

### Discarding an Error Message

1. Navigate to the **Error Queue** page
2. Find the message you want to discard in the list
3. Click the **Discard** button next to the message
4. Confirm the action when prompted
5. The message will be permanently removed from the error queue

### Batch Operations

1. Navigate to the **Error Queue** page
2. Select multiple messages using the checkboxes next to each message
3. Use the batch action buttons at the top of the list:
   - **Reprocess Selected**: Reprocess all selected messages
   - **Discard Selected**: Discard all selected messages

## Statistics and Metrics

The Statistics tab provides real-time and historical metrics about the system's performance.

### Viewing System Metrics

1. Navigate to the **Metrics** page
2. The dashboard displays various metrics, including:
   - **Messages Processed**: The number of messages processed over time
   - **Processing Rate**: The rate at which messages are being processed
   - **Error Rate**: The percentage of messages that fail processing
   - **Kafka Lag**: The lag between message production and consumption
   - **Storage Usage**: The amount of storage being used
   - **Query Performance**: The average query execution time

### Filtering Metrics

1. Navigate to the **Metrics** page
2. Use the filter controls at the top of the page to adjust the view:
   - **Time Range**: Select the time period for which to display metrics (last hour, day, week, month, or custom range)
   - **Metric Type**: Filter to show only specific types of metrics
   - **Aggregation**: Choose how to aggregate the data (sum, average, min, max)

### Exporting Metrics

1. Navigate to the **Metrics** page
2. Configure the metrics view as desired using the filters
3. Click the **Export** button
4. Select the export format (CSV, JSON, or Excel)
5. The file will be downloaded to your computer

## SLO/SLI Monitoring

The SLO/SLI Monitoring tab allows you to define and track Service Level Objectives (SLOs) and Service Level Indicators (SLIs).

### Creating an SLO

1. Navigate to the **SLO/SLIs** page
2. Click the **Create New SLO** button
3. Fill in the following information:
   - **Name**: A unique name for the SLO
   - **Description**: A brief description of the SLO
   - **Metric**: The metric to track (e.g., message processing rate, error rate, query response time)
   - **Target**: The target value for the metric
   - **Window**: The time window over which to evaluate the SLO (e.g., 1 hour, 1 day, 1 week)
   - **Threshold**: The threshold at which the SLO is considered breached
4. Click **Save** to create the SLO

### Viewing SLO Performance

1. Navigate to the **SLO/SLIs** page
2. The page displays a list of all SLOs with their current status
3. Click on an SLO to view detailed performance information, including:
   - **Current Value**: The current value of the metric
   - **Target Value**: The target value for the metric
   - **Compliance**: The percentage of time the SLO is being met
   - **History**: A chart showing the metric's value over time
   - **Breaches**: A list of times when the SLO was breached

### Editing an SLO

1. Navigate to the **SLO/SLIs** page
2. Find the SLO you want to edit in the list
3. Click the **Edit** button next to the SLO
4. Modify the SLO information as needed
5. Click **Save** to update the SLO

### Deleting an SLO

1. Navigate to the **SLO/SLIs** page
2. Find the SLO you want to delete in the list
3. Click the **Delete** button next to the SLO
4. Confirm the deletion when prompted

## Storage Configuration

The Storage Configuration tab allows you to manage the storage backend for the application.

### Creating a Storage Configuration

1. Navigate to the **Storage Configs** page
2. Click the **Create New Configuration** button
3. Fill in the following information:
   - **Name**: A unique name for the configuration
   - **Storage Type**: Select either HDFS or S3
   - **Base Path**: The base path in the storage system
   - **Retention Days**: The number of days to retain data (default: 10)
   - For HDFS:
     - **Keytab Path**: The path to the Kerberos keytab file
     - **Principal**: The Kerberos principal
   - For S3:
     - **Access Key**: The AWS access key
     - **Secret Key**: The AWS secret key
     - **Region**: The AWS region
   - **Active**: Whether this configuration should be active
4. Click **Save** to create the configuration

### Switching Storage Backends

1. Navigate to the **Storage Configs** page
2. Find the configuration you want to activate in the list
3. Click the **Activate** button next to the configuration
4. The selected configuration will be set as active, and all other configurations will be deactivated
5. Click the **Apply Active Configuration** button to apply the changes to the system

### Editing a Storage Configuration

1. Navigate to the **Storage Configs** page
2. Find the configuration you want to edit in the list
3. Click the **Edit** button next to the configuration
4. Modify the configuration information as needed
5. Click **Save** to update the configuration

### Deleting a Storage Configuration

1. Navigate to the **Storage Configs** page
2. Find the configuration you want to delete in the list
3. Click the **Delete** button next to the configuration
4. Confirm the deletion when prompted

## Troubleshooting

### Common Issues and Solutions

#### Message Processing Failures

**Issue**: Messages are failing to process and appearing in the error queue.

**Solution**:
1. Check the error reason in the error queue
2. Verify that the message schema matches the actual message structure
3. Ensure that the Kafka connection is properly configured
4. Check the Kerberos authentication if enabled

#### Storage Connection Issues

**Issue**: Unable to connect to the storage backend.

**Solution**:
1. Verify that the storage configuration is correct
2. Check the Kerberos authentication if using HDFS
3. Verify AWS credentials if using S3
4. Ensure that the storage system is accessible from the application server

#### Query Performance Issues

**Issue**: Queries are running slowly or timing out.

**Solution**:
1. Simplify the query by reducing the number of conditions
2. Add appropriate filters to narrow down the result set
3. Check the storage system's performance
4. Consider optimizing the Iceberg table properties

#### High Kafka Lag

**Issue**: The Kafka lag is increasing, indicating that messages are not being processed fast enough.

**Solution**:
1. Increase the message consumption rate
2. Check for bottlenecks in the processing pipeline
3. Verify that the storage system can handle the write load
4. Consider scaling up the application resources

### Getting Help

If you encounter issues that cannot be resolved using this guide, please contact your system administrator or the application support team.
