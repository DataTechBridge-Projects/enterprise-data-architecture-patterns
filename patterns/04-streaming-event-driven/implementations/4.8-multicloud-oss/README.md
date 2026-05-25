---
layout: default
title: "4.8 — Streaming / Event-Driven · Multi-Cloud OSS Portable"
---

# 4.8 — Streaming / Event-Driven · Multi-Cloud OSS Portable

**Stack:** Apache Kafka · Apache Flink · Apache Iceberg · ksqlDB
**Processing:** Streaming-first · Kappa Architecture · Cloud-Portable
**Buy vs Build:** Build (fully OSS, deployable on any cloud or on-prem)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  EVENT SOURCES  (any cloud or on-prem)                                      │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │  Web /   │  │  Mobile  │  │   IoT /  │  │Microserv-│  │  DB CDC  │    │
│  │  Click-  │  │  App     │  │  Edge    │  │  ices    │  │(Debezium)│    │
│  │  stream  │  │  Events  │  │  Devices │  │  Events  │  │          │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        │             │             │              │             │
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EVENT BROKER — Apache Kafka (managed MSK / Event Hubs / Confluent OSS)     │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Kafka Cluster (3+ brokers, multi-AZ)                        │          │
│  │                                                              │          │
│  │  Topics (partitioned by entity_id):                          │          │
│  │  • clickstream.raw   • iot.telemetry   • cdc.{table}        │          │
│  │  • orders.events     • alerts.output   • dlq.{topic}        │          │
│  │                                                              │          │
│  │  Apicurio Registry / Confluent SR → Avro / Protobuf schema  │          │
│  │  Kafka Connect (Debezium, S3 Sink, JDBC)                    │          │
│  │  ksqlDB → lightweight stateful streaming SQL                │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STREAM PROCESSING — Apache Flink (on K8s / YARN / standalone)              │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  Enrichment     │   │  Windowed        │   │  CEP / Fraud    │          │
│  │  (Async I/O     │   │  Aggregations    │   │  Pattern        │          │
│  │   Cassandra /   │   │  tumbling/       │   │  Detection      │          │
│  │   Redis lookup) │   │  sliding/session │   │  (Flink CEP)    │          │
│  └────────┬────────┘   └────────┬─────────┘   └────────┬────────┘          │
└───────────┼────────────────────┼────────────────────── ┼───────────────────┘
            │                    │                        │
            └────────────────────┼────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SERVING STORES                                                             │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  HOT STORE      │   │  MATERIALIZED    │   │  COLD STORE     │          │
│  │  Apache         │   │  VIEWS           │──▶│  Apache Iceberg │          │
│  │  Cassandra /    │   │  ksqlDB          │   │  on S3/ADLS/GCS │          │
│  │  ScyllaDB       │   │  (push queries)  │   │                 │          │
│  │                 │   │                  │   │ • ACID + time   │          │
│  │ • <5ms reads    │   │ • pull queries   │   │   travel        │          │
│  │ • Wide rows     │   │ • topic → table  │   │ • Z-order       │          │
│  │ • Linear scale  │   │   materialization│   │   compaction    │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
            │                    │                        │
            ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Apache Iceberg REST Catalog (Nessie or Polaris)                  │
│  · Tables registered centrally; Flink + Trino + Spark share catalog         │
│  · Apicurio Registry = Avro/Protobuf schema contract per topic              │
└──────────┬──────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CONSUMERS                                                                  │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  Operational    │  │  BI / Analytics  │  │  ML Training    │            │
│  │  APIs           │  │  Apache Superset │  │  Spark / Ray    │            │
│  │  (Cassandra)    │  │  (Trino backend) │  │  (Iceberg)      │            │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources["Sources — any cloud"]
        A1[Web Clickstream]
        A2[Mobile Events]
        A3[IoT Devices]
        A4[Microservices]
        A5[Debezium CDC]
    end

    subgraph Broker["Apache Kafka"]
        B1[Kafka Topics\npartitioned by entity_id]
        B2[Apicurio / Confluent SR\nAvro/Protobuf schema]
        B3[ksqlDB\nstateful SQL views]
    end

    subgraph Processing["Apache Flink"]
        C1[Enrichment\nAsync I/O Cassandra]
        C2[Window Aggregation\ntumbling · sliding · session]
        C3[CEP Pattern Detection\nFlink CEP library]
    end

    subgraph Serving["Serving Stores"]
        D1[(Cassandra / ScyllaDB\nHot — <5ms)]
        D2[Iceberg on S3/ADLS/GCS\nCold — ACID]
        D3[ksqlDB\nMaterialized Views]
    end

    subgraph Catalog["Iceberg Catalog"]
        E1[Nessie / Polaris\nREST Catalog]
        E2[Trino\nSQL federation]
    end

    subgraph Consume
        F1[REST APIs\nCassandra reads]
        F2[Apache Superset\nTrino]
        F3[Spark / Ray\nIceberg ML data]
        F4[ksqlDB pull queries\nreal-time lookups]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1
    A5 --> B1
    B1 -. schema enforce .-> B2
    B1 --> B3

    B1 --> C1
    C1 --> C2
    C1 --> C3
    C2 --> D1
    C3 --> D1
    C2 --> D2
    B3 --> D3
    B1 -.->|Kafka Connect S3/ADLS/GCS Sink| D2

    D2 -. register .-> E1
    E1 --> E2

    D1 --> F1
    E2 --> F2
    D2 --> F3
    D3 --> F4
```

---

## Stream Store Design

```
HOT PATH  — Apache Cassandra / ScyllaDB
  Keyspace: streaming_hot  (RF=3, NTS per region)
  Table: entity_state
    PK: entity_id  CK: event_type, event_time
    TTL: 86400s (1 day default per table)

  Table: window_aggregates
    PK: (entity_id, window_type)  CK: window_start
    Columns: count, sum, p50, p99, min, max
    TTL: 604800s (7 days)

MATERIALIZED — ksqlDB
  Persistent Query: CREATE TABLE entity_latest AS
    SELECT entity_id, LATEST_BY_OFFSET(event_type) AS event_type,
           COUNT(*) AS event_count
    FROM clickstream_raw
    GROUP BY entity_id
    EMIT CHANGES;

COLD PATH — Apache Iceberg on object storage (S3 / ADLS / GCS)
  {bucket}/streaming-iceberg/
  ├── raw_events/
  │   └── {topic}/
  │       ├── data/  (Parquet, Snappy)
  │       └── metadata/  (manifests, snapshots, table metadata)
  └── aggregated_events/
      └── {domain}/{window_type}/
          ├── data/  (Z-order by entity_id + event_date)
          └── metadata/
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Kafka ACLs + Cassandra RBAC + Iceberg + Apache Ranger (opt.)    │
│                                                                  │
│  Principal               Access Level     Scope                  │
│  ──────────────────────  ─────────────    ──────────────────── │
│  producer-service         WRITE            own Kafka topics      │
│  flink-job-sa             READ (all topics) Kafka consumer group │
│                           WRITE             Cassandra + Iceberg  │
│  ksqldb-cluster           READ+WRITE        ksqlDB topics        │
│  cassandra-api-sa         SELECT            own keyspace         │
│  trino-query-sa           SELECT            Iceberg catalog      │
│  analyst-role             SELECT (filtered) Iceberg via Ranger   │
│  ml-engineer-sa           READ              Iceberg all tables   │
│                                                                  │
│  Kafka mTLS           → inter-broker + producer/consumer TLS     │
│  Kafka ACLs           → topic-level READ/WRITE per service       │
│  Cassandra CQL RBAC   → keyspace + table + column permissions    │
│  Iceberg + Ranger     → row/column filter policies (optional)    │
│  ksqlDB auth          → HTTP Basic / Kafka-delegated auth        │
│  Object storage       → bucket policy + VPC endpoint only        │
│  KMS / Vault          → HashiCorp Vault for key management       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    EV1[📡 Kafka topic\nreceives event]
    EV2[⏰ Flink watermark\nwindow boundary]
    EV3[🚨 CEP rule fires\nalert pattern matched]

    EV1 --> F1[Flink Job: enrich\nCassandra Async I/O]
    F1 --> F2[Flink Job: window aggregate]
    EV2 --> F2
    F2 --> W1[Cassandra Write\nhot aggregate row]
    F2 --> W2[Iceberg Write\nFlink Iceberg sink commit]
    EV1 -.->|Kafka Connect\nS3/ADLS/GCS Sink| W3[Iceberg raw table\nraw event archive]

    EV3 --> A1[Kafka alerts topic\ndownstream consumers]
    A1 --> A2[Alertmanager / PagerDuty]

    W2 -->|Iceberg snapshot commit| C1[Nessie / Polaris catalog\nsnapshot registered]
    C1 --> C2[Trino auto-picks up\nnew partitions]

    W2 -->|weekly Airflow DAG| COMP[Spark compaction job\nIceberg z-order + expire snapshots]

    F1 -->|TM failure| CKPT[Flink checkpoint on\nobject storage → restart]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Event Broker | Apache Kafka | Self-managed or cloud-managed (MSK / Event Hubs / Confluent) |
| Schema Registry | Apicurio Registry / Confluent Schema Registry | Avro/Protobuf; compatibility enforcement |
| CDC Capture | Debezium (Kafka Connect) | MySQL/PostgreSQL/Oracle/MongoDB CDC |
| Lightweight Streaming | ksqlDB | Materialized views + stateful queries; runs alongside Kafka |
| Stream Processor | Apache Flink | YARN / Kubernetes Operator; Java/Python/SQL API |
| State Backend | RocksDB + object storage checkpoints | Incremental checkpoints every 30s; exactly-once sink |
| Hot Store | Apache Cassandra / ScyllaDB | ScyllaDB for 5–10× higher throughput at same cost |
| Cold Store Format | Apache Iceberg | Flink + Kafka Connect S3/ADLS/GCS Sink writers |
| Iceberg Catalog | Project Nessie / Apache Polaris | Git-like branching for Iceberg; REST API |
| Query Engine | Trino | Federation across Iceberg + Cassandra + Kafka connectors |
| BI Layer | Apache Superset | Trino connection; community dashboards |
| ML Training | Apache Spark / Ray | Read Iceberg via Spark catalog or Ray Dataset |
| Compaction | Apache Spark (Airflow-triggered) | Weekly Iceberg compaction + z-order + snapshot expiry |
| Orchestration | Apache Airflow | Backfill Flink jobs, compaction, catalog tasks |
| Monitoring | Prometheus + Grafana | Flink job metrics, Kafka consumer lag, Cassandra ops |
| Encryption | HashiCorp Vault / Cloud KMS | Key management for Kafka, Cassandra, object storage |

---

## Comparison vs 4.7 (Multi-Cloud Managed)

| Dimension | 4.8 Multi-Cloud OSS | 4.7 Multi-Cloud Managed |
|-----------|--------------------|-----------------------|
| Kafka | Self-managed | Confluent Cloud |
| Flink | Self-managed | Confluent Flink Cloud |
| Serving store | Cassandra + Iceberg | MongoDB Atlas + Snowflake |
| Schema registry | Apicurio (OSS) | Confluent Cloud SR |
| Ops overhead | High (all self-managed) | Very low |
| Cost (at scale) | Lower (infra only) | Higher (CKU + credits) |
| Vendor lock-in | None | High |
| Portability | Maximum | Low |
| Latency | 10–100ms (tunable) | <1s (Snowpipe Streaming) |

---

## When to Choose This Implementation

✅ Maximum portability — must run on any cloud or on-prem
✅ Avoid vendor lock-in (Confluent, Snowflake) on principle or cost
✅ Team has strong Kafka + Flink + Cassandra expertise
✅ Sub-100ms stream processing latency required with full tuning control
✅ Cost optimization at high scale (>TB/day) justifies operational investment

❌ Limited platform engineering team → use 4.7 (managed SaaS)
❌ Snowflake already central platform → use 4.7
❌ Need 200+ managed Kafka connectors out of the box → use 4.7
