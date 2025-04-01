# Function Flow Design

This document outlines the functional flow of the Kafka-Iceberg application, describing how data and control flow through the system.

## Ingestion Service Flow

### 1. Kafka Message Consumption

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Kafka Topic    │────▶│ Kafka Consumer  │────▶│ Schema Validator│
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Valid Message?  │
                                               │                 │
                                               └─────────────────┘
                                                 /             \
                                                /               \
                                               /                 \
                                              ▼                   ▼
                                    ┌─────────────────┐   ┌─────────────────┐
                                    │                 │   │                 │
                                    │ Spark Streaming │   │ Error Handler   │
                                    │                 │   │                 │
                                    └─────────────────┘   └─────────────────┘
                                              │                   │
                                              │                   │
                                              ▼                   ▼
                                    ┌─────────────────┐   ┌─────────────────┐
                                    │                 │   │                 │
                                    │ Iceberg Writer  │   │ Error Queue     │
                                    │                 │   │ (Kafka)         │
                                    └─────────────────┘   └─────────────────┘
```

**Function Flow:**

1. **Kafka Consumer**
   - `consumeMessages()`: Consumes messages from Kafka topic
   - `authenticateWithKerberos()`: Authenticates using Kerberos keytab
   - `configureConsumptionRate()`: Configures message consumption rate

2. **Schema Validator**
   - `validateMessage(message, schema)`: Validates message against schema
   - `getSchemaFromRegistry()`: Gets schema from registry

3. **Spark Streaming**
   - `createSparkSession()`: Creates Spark session
   - `createStreamingContext()`: Creates streaming context
   - `processMessages(messages)`: Processes messages in batches
   - `transformMessages(messages)`: Transforms messages for storage

4. **Iceberg Writer**
   - `initializeIcebergTable()`: Initializes Iceberg table
   - `writeToIceberg(messages)`: Writes messages to Iceberg
   - `configureStorage(type)`: Configures storage type (HDFS/S3)
   - `authenticateStorage()`: Authenticates with storage using Kerberos

5. **Error Handler**
   - `handleInvalidMessage(message, error)`: Handles invalid message
   - `pushToErrorQueue(message, error)`: Pushes message to error queue
   - `logError(message, error)`: Logs error for monitoring

6. **Metrics Collector**
   - `collectMetrics()`: Collects metrics for monitoring
   - `recordMessagesRead(count)`: Records number of messages read
   - `recordIngestionRate(rate)`: Records ingestion rate
   - `recordLag(partition, lag)`: Records lag at partition level
   - `recordStoredRecords(count)`: Records number of stored records
   - `publishMetrics()`: Publishes metrics to monitoring system

### 2. Error Queue Management

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Error Queue    │────▶│ Error Queue     │────▶│ Error Message   │
│  (Kafka)        │     │ Consumer        │     │ Processor       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Message Purger  │
                                               │                 │
                                               └─────────────────┘
```

**Function Flow:**

1. **Error Queue Consumer**
   - `consumeErrorMessages()`: Consumes messages from error queue
   - `trackErrorMessages()`: Tracks error messages for monitoring

2. **Error Message Processor**
   - `processErrorMessage(message)`: Processes error message
   - `updateErrorStatus(message, status)`: Updates error message status

3. **Message Purger**
   - `purgeExpiredMessages()`: Purges messages older than 1 hour
   - `scheduleMessagePurge()`: Schedules message purge job

## UI Service Flow

### 1. Configuration Management

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Web UI         │────▶│ Configuration   │────▶│ Configuration   │
│  (Config Tab)   │     │ Controller      │     │ Service         │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Database        │
                                               │                 │
                                               └─────────────────┘
```

**Function Flow:**

1. **Configuration Controller**
   - `getSchemaConfigurations()`: Gets schema configurations
   - `saveSchemaConfiguration(schema)`: Saves schema configuration
   - `getKafkaConfigurations()`: Gets Kafka configurations
   - `saveKafkaConfiguration(config)`: Saves Kafka configuration
   - `getStorageConfigurations()`: Gets storage configurations
   - `saveStorageConfiguration(config)`: Saves storage configuration

2. **Configuration Service**
   - `validateSchemaConfiguration(schema)`: Validates schema configuration
   - `validateKafkaConfiguration(config)`: Validates Kafka configuration
   - `validateStorageConfiguration(config)`: Validates storage configuration
   - `applyConfiguration(config)`: Applies configuration to system

### 2. Query Management

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Web UI         │────▶│ Query           │────▶│ Query Service   │
│  (Query Tab)    │     │ Controller      │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Iceberg Reader  │
                                               │                 │
                                               └─────────────────┘
```

**Function Flow:**

1. **Query Controller**
   - `executeQuery(query)`: Executes query
   - `getBulkQueryResults(query)`: Gets bulk query results
   - `getSingleQueryResult(query)`: Gets single query result
   - `exportQueryResults(query, format)`: Exports query results

2. **Query Service**
   - `buildQuery(params)`: Builds query from parameters
   - `optimizeQuery(query)`: Optimizes query for performance
   - `executeQuery(query)`: Executes query against Iceberg
   - `transformResults(results)`: Transforms results for UI

3. **Iceberg Reader**
   - `initializeIcebergTable()`: Initializes Iceberg table
   - `readFromIceberg(query)`: Reads data from Iceberg
   - `authenticateStorage()`: Authenticates with storage using Kerberos

### 3. Error Queue Management UI

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Web UI         │────▶│ Error Queue     │────▶│ Error Queue     │
│  (Error Tab)    │     │ Controller      │     │ Service         │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Error Queue     │
                                               │ (Kafka)         │
                                               └─────────────────┘
```

**Function Flow:**

1. **Error Queue Controller**
   - `getErrorMessages()`: Gets error messages from queue
   - `getErrorMessage(id)`: Gets specific error message
   - `updateErrorMessage(id, message)`: Updates error message
   - `reprocessErrorMessage(id)`: Reprocesses error message
   - `discardErrorMessage(id)`: Discards error message

2. **Error Queue Service**
   - `fetchErrorMessages()`: Fetches error messages from Kafka
   - `updateErrorStatus(id, status)`: Updates error message status
   - `pushToReprocessingQueue(message)`: Pushes message to reprocessing queue
   - `markAsDiscarded(id)`: Marks message as discarded

### 4. Metrics and Monitoring

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Web UI         │────▶│ Metrics         │────▶│ Metrics Service │
│  (Stats Tab)    │     │ Controller      │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │                 │
                                               │ Prometheus      │
                                               │ Integration     │
                                               └─────────────────┘
```

**Function Flow:**

1. **Metrics Controller**
   - `getMetrics()`: Gets system metrics
   - `getMetric(name)`: Gets specific metric
   - `getSLOs()`: Gets SLOs/SLIs
   - `getSLO(name)`: Gets specific SLO/SLI

2. **Metrics Service**
   - `collectMetrics()`: Collects metrics from system
   - `calculateSLOs()`: Calculates SLOs/SLIs
   - `storeMetrics()`: Stores metrics in database
   - `exposeMetricsEndpoint()`: Exposes metrics endpoint for Prometheus

3. **Prometheus Integration**
   - `configurePrometheusEndpoint()`: Configures Prometheus endpoint
   - `exposeMetrics()`: Exposes metrics for Prometheus scraping

## Cross-Cutting Concerns

### 1. Authentication

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Kerberos       │────▶│ Authentication  │────▶│ Service         │
│  Keytab         │     │ Service         │     │ (Kafka/HDFS/S3) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Function Flow:**

1. **Authentication Service**
   - `authenticateWithKerberos(service)`: Authenticates with Kerberos
   - `loadKeytab(path)`: Loads Kerberos keytab
   - `validateAuthentication()`: Validates authentication
   - `renewAuthentication()`: Renews authentication

### 2. Logging and Monitoring

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Application    │────▶│ Logging Service │────▶│ Log Storage     │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                               │
                               │
                               ▼
                        ┌─────────────────┐
                        │                 │
                        │ Monitoring      │
                        │ Service         │
                        └─────────────────┘
```

**Function Flow:**

1. **Logging Service**
   - `logInfo(message)`: Logs info message
   - `logWarning(message)`: Logs warning message
   - `logError(message)`: Logs error message
   - `logMetric(metric)`: Logs metric

2. **Monitoring Service**
   - `monitorSystem()`: Monitors system
   - `alertOnThreshold(metric, threshold)`: Alerts when threshold is reached
   - `publishAlerts()`: Publishes alerts to Kafka topic
