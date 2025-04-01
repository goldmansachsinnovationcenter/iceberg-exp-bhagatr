# Entity Relationship Design

This document outlines the entity relationships for the Kafka-Iceberg application.

## Entities

### Message Schema
- **SchemaId**: Unique identifier for the schema
- **Name**: Name of the schema
- **Description**: Description of the schema
- **Fields**: List of field definitions
- **CreatedAt**: Timestamp when the schema was created
- **UpdatedAt**: Timestamp when the schema was last updated

### Field Definition
- **FieldId**: Unique identifier for the field
- **SchemaId**: Reference to the schema this field belongs to
- **Name**: Name of the field
- **Type**: Data type of the field (string, number, boolean, etc.)
- **Required**: Whether the field is required
- **Description**: Description of the field

### Kafka Configuration
- **ConfigId**: Unique identifier for the configuration
- **Name**: Name of the configuration
- **BootstrapServers**: Kafka bootstrap servers
- **Topic**: Kafka topic to consume from
- **ConsumerGroup**: Consumer group ID
- **SecurityProtocol**: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)
- **SaslMechanism**: SASL mechanism (if applicable)
- **KeytabPath**: Path to the Kerberos keytab file
- **Principal**: Kerberos principal
- **ConsumptionRate**: Message consumption rate (messages/minute)
- **CreatedAt**: Timestamp when the configuration was created
- **UpdatedAt**: Timestamp when the configuration was last updated

### Storage Configuration
- **ConfigId**: Unique identifier for the configuration
- **Name**: Name of the configuration
- **StorageType**: Type of storage (HDFS, S3)
- **BasePath**: Base path for storage
- **RetentionDays**: Data retention period in days
- **KeytabPath**: Path to the Kerberos keytab file (for HDFS)
- **Principal**: Kerberos principal (for HDFS)
- **AccessKey**: Access key (for S3)
- **SecretKey**: Secret key (for S3)
- **Region**: Region (for S3)
- **CreatedAt**: Timestamp when the configuration was created
- **UpdatedAt**: Timestamp when the configuration was last updated

### Error Message
- **ErrorId**: Unique identifier for the error message
- **OriginalMessage**: Original JSON message that failed processing
- **ErrorReason**: Reason for the error
- **Timestamp**: Timestamp when the error occurred
- **Status**: Status of the error message (NEW, REPROCESSING, DISCARDED)
- **ExpiryTime**: Time when the message will be automatically purged

### Metric
- **MetricId**: Unique identifier for the metric
- **Name**: Name of the metric
- **Value**: Value of the metric
- **Timestamp**: Timestamp when the metric was recorded
- **Type**: Type of metric (COUNTER, GAUGE, TIMER)
- **Tags**: Tags associated with the metric

### SLO/SLI
- **SloId**: Unique identifier for the SLO
- **Name**: Name of the SLO
- **Description**: Description of the SLO
- **Target**: Target value for the SLO
- **ActualValue**: Actual value for the SLO
- **Status**: Status of the SLO (MET, NOT_MET)
- **Timestamp**: Timestamp when the SLO was last evaluated

## Relationships

```
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  Message Schema │─────┤ Field Definition│
│                 │1   *│                 │
└─────────────────┘     └─────────────────┘
        │
        │
        │1
        ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  Kafka Config   │─────┤ Error Message   │
│                 │1   *│                 │
└─────────────────┘     └─────────────────┘
        │
        │
        │1
        ▼
┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │
│  Storage Config │─────┤ Metric          │
│                 │1   *│                 │
└─────────────────┘     └─────────────────┘
                               │
                               │
                               │*
                               ▼
                        ┌─────────────────┐
                        │                 │
                        │  SLO/SLI        │
                        │                 │
                        └─────────────────┘
```

## Database Schema

Since we're using an in-memory database (H2) with SQLModel as the ORM, here's the schema definition:

```python
from sqlmodel import Field, SQLModel, Relationship
from typing import List, Optional
from datetime import datetime

class FieldDefinition(SQLModel, table=True):
    field_id: Optional[int] = Field(default=None, primary_key=True)
    schema_id: int = Field(foreign_key="messageschema.schema_id")
    name: str
    type: str
    required: bool = False
    description: Optional[str] = None
    
    schema: "MessageSchema" = Relationship(back_populates="fields")

class MessageSchema(SQLModel, table=True):
    schema_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    fields: List[FieldDefinition] = Relationship(back_populates="schema")

class KafkaConfiguration(SQLModel, table=True):
    config_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    bootstrap_servers: str
    topic: str
    consumer_group: str
    security_protocol: str
    sasl_mechanism: Optional[str] = None
    keytab_path: Optional[str] = None
    principal: Optional[str] = None
    consumption_rate: int = 100  # Default: 100 messages/minute
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    error_messages: List["ErrorMessage"] = Relationship(back_populates="kafka_config")

class StorageConfiguration(SQLModel, table=True):
    config_id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    storage_type: str  # "HDFS" or "S3"
    base_path: str
    retention_days: int = 10  # Default: 10 days
    keytab_path: Optional[str] = None
    principal: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    region: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    metrics: List["Metric"] = Relationship(back_populates="storage_config")

class ErrorMessage(SQLModel, table=True):
    error_id: Optional[int] = Field(default=None, primary_key=True)
    kafka_config_id: int = Field(foreign_key="kafkaconfiguration.config_id")
    original_message: str
    error_reason: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = "NEW"  # "NEW", "REPROCESSING", "DISCARDED"
    expiry_time: datetime
    
    kafka_config: KafkaConfiguration = Relationship(back_populates="error_messages")

class Metric(SQLModel, table=True):
    metric_id: Optional[int] = Field(default=None, primary_key=True)
    storage_config_id: int = Field(foreign_key="storageconfiguration.config_id")
    name: str
    value: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    type: str  # "COUNTER", "GAUGE", "TIMER"
    tags: Optional[str] = None
    
    storage_config: StorageConfiguration = Relationship(back_populates="metrics")
    slos: List["SLO"] = Relationship(back_populates="metric")

class SLO(SQLModel, table=True):
    slo_id: Optional[int] = Field(default=None, primary_key=True)
    metric_id: int = Field(foreign_key="metric.metric_id")
    name: str
    description: Optional[str] = None
    target: float
    actual_value: float
    status: str  # "MET", "NOT_MET"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    metric: Metric = Relationship(back_populates="slos")
```

This schema represents the relationships between the entities in the application and will be used for storing configuration, metrics, and error messages in the in-memory database.
