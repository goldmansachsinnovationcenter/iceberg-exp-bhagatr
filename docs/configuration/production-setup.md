# Production Environment Configuration Guide

This document provides detailed instructions for configuring the Kafka-Iceberg application in a production environment. It covers both the ingestion layer and service UI layer components.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Ingestion Service Configuration](#ingestion-service-configuration)
3. [UI Service Configuration](#ui-service-configuration)
4. [Kafka Configuration](#kafka-configuration)
5. [Storage Configuration](#storage-configuration)
6. [Authentication Setup](#authentication-setup)
7. [Performance Tuning](#performance-tuning)
8. [Monitoring and Metrics](#monitoring-and-metrics)

## Prerequisites

Before deploying the Kafka-Iceberg application to a production environment, ensure the following prerequisites are met:

- Java 11 or higher
- Apache Spark 3.2.0 or higher
- Apache Kafka 2.8.0 or higher
- Apache Iceberg 0.13.1 or higher
- Hadoop 3.2.0 or higher (for HDFS storage)
- AWS SDK (for S3 storage)
- Kerberos client libraries

## Ingestion Service Configuration

The ingestion service is responsible for consuming messages from Kafka and storing them in HDFS or S3 using Iceberg. Configure the ingestion service using the following properties in `application.properties`:

```properties
# Server Configuration
server.port=8081
spring.application.name=iceberg-ingestion-service

# Spark Configuration
spark.master=local[*]
spark.app.name=Iceberg Ingestion Service
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.local.type=hadoop
spark.sql.catalog.local.warehouse=${storage.path}

# Kafka Configuration
kafka.bootstrap.servers=kafka:9092
kafka.group.id=iceberg-ingestion-group
kafka.auto.offset.reset=earliest
kafka.enable.auto.commit=false
kafka.max.poll.records=100
kafka.poll.timeout.ms=5000

# Kerberos Configuration
kerberos.enabled=true
kerberos.keytab.path=/etc/security/keytabs/kafka.keytab
kerberos.principal=kafka@EXAMPLE.COM
kerberos.krb5.conf=/etc/krb5.conf

# Storage Configuration
storage.type=HDFS
storage.path=hdfs://namenode:8020/iceberg
storage.retention.days=10

# Message Processing
message.consumption.rate=100
message.batch.size=50

# Error Queue
error.topic=iceberg-error-queue
```

For production environments, adjust the following parameters:

- `spark.master`: Set to your Spark cluster URL (e.g., `spark://master:7077`)
- `kafka.bootstrap.servers`: Set to your Kafka broker addresses
- `storage.path`: Set to your HDFS or S3 path
- `message.consumption.rate`: Adjust based on your throughput requirements

## UI Service Configuration

The UI service provides a web interface for managing message schemas, monitoring metrics, and handling error messages. Configure the UI service using the following properties in `application.properties`:

```properties
# Server Configuration
server.port=8080
spring.application.name=iceberg-ui-service

# Database Configuration
spring.datasource.url=jdbc:h2:mem:icebergdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=password
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.jpa.hibernate.ddl-auto=update
spring.h2.console.enabled=true

# Kafka Configuration
spring.kafka.bootstrap-servers=kafka:9092
spring.kafka.consumer.group-id=iceberg-ui-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer

# Kerberos Configuration
kerberos.enabled=true
kerberos.keytab.path=/etc/security/keytabs/kafka.keytab
kerberos.principal=kafka@EXAMPLE.COM
kerberos.krb5.conf=/etc/krb5.conf

# Error Queue Configuration
error.topic=iceberg-error-queue
error.reprocessing.topic=iceberg-reprocessing-topic
error.purge.interval.minutes=60

# Metrics Configuration
management.endpoints.web.exposure.include=prometheus,health,info
management.endpoint.prometheus.enabled=true
```

For production environments, replace the in-memory H2 database with a production-grade database:

```properties
# PostgreSQL Configuration
spring.datasource.url=jdbc:postgresql://postgres:5432/icebergdb
spring.datasource.driverClassName=org.postgresql.Driver
spring.datasource.username=postgres
spring.datasource.password=password
spring.jpa.database-platform=org.hibernate.dialect.PostgreSQLDialect
```

## Kafka Configuration

### Topic Configuration

Create the following Kafka topics with appropriate configurations:

1. **Data Topic**: For incoming JSON messages
   ```
   bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 3 --partitions 6 --topic iceberg-data-topic
   ```

2. **Error Queue Topic**: For messages that fail processing
   ```
   bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 3 --partitions 3 --topic iceberg-error-queue
   ```

3. **Reprocessing Topic**: For reprocessing corrected messages
   ```
   bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 3 --partitions 3 --topic iceberg-reprocessing-topic
   ```

4. **Metrics Topic**: For system metrics
   ```
   bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 3 --partitions 3 --topic iceberg-metrics-topic
   ```

5. **Alerts Topic**: For system alerts
   ```
   bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 3 --partitions 3 --topic iceberg-alerts-topic
   ```

### Consumer Group Configuration

Configure consumer groups with appropriate settings:

```
bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group iceberg-ingestion-group
bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group iceberg-ui-group
```

## Storage Configuration

### HDFS Configuration

To configure HDFS as the storage backend:

1. Ensure Hadoop is properly configured with the following properties in `core-site.xml`:

```xml
<property>
  <name>hadoop.security.authentication</name>
  <value>kerberos</value>
</property>
<property>
  <name>hadoop.security.authorization</name>
  <value>true</value>
</property>
```

2. Configure the HDFS path in the application:

```properties
storage.type=HDFS
storage.path=hdfs://namenode:8020/iceberg
storage.hdfs.keytab.path=/etc/security/keytabs/hdfs.keytab
storage.hdfs.principal=hdfs@EXAMPLE.COM
```

### S3 Configuration

To configure S3 as the storage backend:

1. Ensure AWS credentials are properly configured:

```properties
storage.type=S3
storage.path=s3a://my-bucket/iceberg
storage.s3.access.key=YOUR_ACCESS_KEY
storage.s3.secret.key=YOUR_SECRET_KEY
storage.s3.region=us-east-1
```

2. For Kerberos authentication with S3:

```properties
storage.s3.kerberos.enabled=true
storage.s3.keytab.path=/etc/security/keytabs/s3.keytab
storage.s3.principal=s3@EXAMPLE.COM
```

### Switching Between Storage Backends

The application provides a UI interface for switching between HDFS and S3 storage backends. To switch programmatically:

1. Update the active storage configuration in the database
2. Restart the ingestion service to apply the changes

## Authentication Setup

### Kerberos Authentication

1. Install Kerberos client libraries:

```bash
apt-get install krb5-user libpam-krb5 libpam-ccreds auth-client-config
```

2. Configure `/etc/krb5.conf` with your Kerberos realm information:

```
[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    EXAMPLE.COM = {
        kdc = kdc.example.com
        admin_server = kdc.example.com
    }

[domain_realm]
    .example.com = EXAMPLE.COM
    example.com = EXAMPLE.COM
```

3. Create keytab files for each service:

```bash
kadmin -p admin/admin -q "ktadd -k /etc/security/keytabs/kafka.keytab kafka/kafka.example.com@EXAMPLE.COM"
kadmin -p admin/admin -q "ktadd -k /etc/security/keytabs/hdfs.keytab hdfs/hdfs.example.com@EXAMPLE.COM"
kadmin -p admin/admin -q "ktadd -k /etc/security/keytabs/s3.keytab s3/s3.example.com@EXAMPLE.COM"
```

4. Set appropriate permissions on keytab files:

```bash
chmod 400 /etc/security/keytabs/*.keytab
chown app-user:app-group /etc/security/keytabs/*.keytab
```

### JAAS Configuration

Create a JAAS configuration file at `/etc/kafka/kafka_jaas.conf`:

```
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/kafka.keytab"
    principal="kafka/kafka.example.com@EXAMPLE.COM";
};

Client {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/etc/security/keytabs/hdfs.keytab"
    principal="hdfs/hdfs.example.com@EXAMPLE.COM";
};
```

Set the JAAS configuration file path in the JVM options:

```
-Djava.security.auth.login.config=/etc/kafka/kafka_jaas.conf
```

## Performance Tuning

### Spark Tuning

Optimize Spark performance with the following settings:

```properties
spark.executor.memory=4g
spark.executor.cores=2
spark.executor.instances=4
spark.driver.memory=2g
spark.sql.shuffle.partitions=200
spark.default.parallelism=200
spark.memory.fraction=0.8
spark.memory.storageFraction=0.3
```

### Kafka Tuning

Optimize Kafka consumer performance:

```properties
kafka.max.poll.records=500
kafka.fetch.max.bytes=52428800
kafka.fetch.max.wait.ms=500
kafka.max.partition.fetch.bytes=1048576
```

### Iceberg Tuning

Optimize Iceberg write performance:

```properties
spark.sql.catalog.local.write.format.default=parquet
spark.sql.catalog.local.write.parquet.compression-codec=snappy
spark.sql.catalog.local.write.metadata.compression-codec=gzip
spark.sql.catalog.local.write.distribution-mode=hash
```

## Monitoring and Metrics

### Prometheus Integration

The application exposes metrics at the `/actuator/prometheus` endpoint. Configure Prometheus to scrape these metrics:

```yaml
scrape_configs:
  - job_name: 'iceberg-ingestion-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['ingestion-service:8081']
  - job_name: 'iceberg-ui-service'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['ui-service:8080']
```

### Grafana Dashboards

Import the provided Grafana dashboards for monitoring:

1. Kafka Metrics Dashboard
2. Iceberg Storage Dashboard
3. Application Performance Dashboard
4. SLO/SLI Dashboard

### Alert Configuration

Configure alerts based on the following thresholds:

1. Kafka Lag > 1000 messages for > 5 minutes
2. Error Queue Size > 100 messages
3. Message Processing Rate < 50 messages/minute
4. Storage Usage > 80%
5. API Response Time > 500ms
