---
layout: default
title: "4.4 — Streaming / Event-Driven · Azure OSS on Cloud"
---

# 4.4 — Streaming / Event-Driven · Azure OSS on Cloud

**Stack:** Azure Event Hubs (Kafka API) · Apache Flink on AKS · Delta Lake · Cosmos DB
**Processing:** Streaming-first · Kappa Architecture
**Buy vs Build:** Build (OSS processing on managed Azure infra)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  EVENT SOURCES                                                              │
│                                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
│  │  Web /   │  │  Mobile  │  │   IoT /  │  │Microserv-│  │  DB CDC  │    │
│  │  Click-  │  │  App     │  │  Azure   │  │  ices    │  │(Debezium │    │
│  │  stream  │  │  Events  │  │  IoT Hub │  │  Events  │  │→ EH)     │    │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        │             │             │              │             │
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  EVENT BROKER — Azure Event Hubs (Kafka API endpoint)                       │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Event Hubs Namespace → exposed as Kafka bootstrap endpoint  │          │
│  │  Kafka Producer SDK → uses SASL/OAUTHBEARER (Azure AD)       │          │
│  │                                                              │          │
│  │  Topics:                                                     │          │
│  │  • clickstream.raw   • iot.telemetry   • cdc.{table}        │          │
│  │  • orders.events     • alerts.output                        │          │
│  │                                                              │          │
│  │  Confluent Schema Registry (on AKS) → Avro enforcement      │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STREAM PROCESSING — Apache Flink on AKS (Kubernetes Operator)              │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  Enrichment     │   │  Windowed        │   │  CEP / Fraud    │          │
│  │  (Async I/O     │   │  Aggregations    │   │  Pattern        │          │
│  │   Cosmos DB     │   │  tumbling/       │   │  Detection      │          │
│  │   lookups)      │   │  sliding/session │   │  (Flink CEP     │          │
│  │                 │   │                  │   │   library)      │          │
│  └────────┬────────┘   └────────┬─────────┘   └────────┬────────┘          │
└───────────┼────────────────────┼────────────────────── ┼───────────────────┘
            │                    │                        │
            └────────────────────┼────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  SERVING STORES                                                             │
│                                                                             │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  HOT STORE      │   │  WARM STORE      │   │  COLD STORE     │          │
│  │  Azure Cosmos DB│   │  Synapse Server- │──▶│  Delta Lake on  │          │
│  │  (NoSQL API)    │   │  less SQL        │   │  ADLS Gen2      │          │
│  │                 │   │  (external table │   │                 │          │
│  │ • <10ms reads   │   │   over Delta)    │   │ • ACID writes   │          │
│  │ • Change feed   │   │                  │   │ • Flink native  │          │
│  │ • Global dist.  │   │                  │   │   Delta sink    │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
            │                    │                        │
            ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Microsoft Purview + Hive Metastore (Flink SQL)                   │
│  · Delta tables registered via Purview auto-scan of ADLS                    │
│  · Flink SQL catalog → Hive-compatible for Delta table DDL                  │
└──────────┬──────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CONSUMERS                                                                  │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  APIs / Funcs   │  │  BI / Reporting  │  │  Azure ML       │            │
│  │  (Cosmos feed)  │  │  Apache Superset │  │  (ADLS Delta)   │            │
│  │  Azure Functions│  │  Power BI        │  │  / Databricks   │            │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[Web Clickstream]
        A2[IoT Hub]
        A3[Microservices]
        A4[Debezium CDC]
    end

    subgraph Broker["Event Hubs — Kafka API"]
        B1[Kafka Topics\npartitioned by entity_id]
        B2[Confluent Schema Registry\non AKS]
    end

    subgraph Processing["Apache Flink — AKS"]
        C1[Enrichment\nAsync I/O Cosmos]
        C2[Window Aggregation\ntumbling · sliding]
        C3[CEP Fraud Detection\nFlink CEP library]
    end

    subgraph Serving["Serving Stores"]
        D1[(Cosmos DB NoSQL\nHot)]
        D2[Delta Lake on ADLS\nCold — ACID]
    end

    subgraph Query["Query / Catalog"]
        E1[Synapse Serverless SQL\nexternal Delta tables]
        E2[Microsoft Purview\nlineage + scan]
    end

    subgraph Consume
        F1[Azure Functions\nCosmos change feed]
        F2[Apache Superset / Power BI]
        F3[Azure ML\nDelta training data]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1
    B1 -. schema validate .-> B2

    B1 --> C1
    C1 --> C2
    C1 --> C3
    C2 --> D1
    C3 --> D1
    C2 --> D2
    B1 -.->|Event Hubs Capture| D2

    D2 -. scan .-> E2
    D2 --> E1

    D1 --> F1
    E1 --> F2
    D2 --> F3
```

---

## Stream Store Design

```
HOT PATH  — Azure Cosmos DB (NoSQL API)
  Container: stream_state
    Partition Key: /entity_id
    TTL: 86400s (1 day)
    Change Feed → Azure Functions

  Container: window_aggregates
    Partition Key: /window_key  (entity_id#window_type#window_start)
    TTL: 604800s (7 days)

COLD PATH — Delta Lake on ADLS Gen2
  abfss://streaming@<account>.dfs.core.windows.net/
  ├── raw/
  │   └── {topic}/
  │       └── _delta_log/ + *.parquet
  │           (Flink Delta sink, partitioned by event_date)
  └── aggregated/
      └── {domain}/{window_type}/
          └── _delta_log/ + *.parquet
              (Z-order by entity_id + event_date)
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────────┐
│  Azure AD + Kafka ACLs (Event Hubs) + ADLS ACLs + Cosmos RBAC   │
│                                                                  │
│  Azure AD Identity       Access Level     Scope                  │
│  ─────────────────────   ────────────     ──────────────────── │
│  producer-workload-id    Kafka Send        Event Hubs topics     │
│  flink-job-pod-id        Kafka Consume     Event Hubs (all)      │
│                          Write             Cosmos DB + ADLS      │
│  schema-registry-id      Admin             Schema Registry AKS   │
│  api-function-id         Read              Cosmos DB container   │
│  analyst-aad-group       Storage Blob      ADLS Delta (read)     │
│                          Data Reader                             │
│  ml-engineer-group       Contributor       ADLS full             │
│                                                                  │
│  Event Hubs mTLS      → SASL/OAUTHBEARER via Azure AD           │
│  Flink checkpoints    → ADLS + Azure Key Vault secret ref        │
│  Delta table ACLs     → ADLS Gen2 fine-grained ACLs             │
│  Cosmos DB RBAC       → built-in roles per container            │
│  AKS network policy   → restrict Flink pods to Event Hubs VNet  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    EV1[📡 Event Hubs\nKafka topic receives event]
    EV2[⏰ Flink watermark\nwindow boundary]
    EV3[🚨 CEP rule fires\nfraud pattern]

    EV1 --> F1[Flink Job: enrich\nCosmos Async I/O]
    F1 --> F2[Flink Job: window aggregate]
    EV2 --> F2
    F2 --> W1[Cosmos DB Write\nhot state]
    F2 --> W2[Delta Lake Write\ncold Parquet commit]

    EV3 --> A1[Flink sink → Event Hubs\nalerts topic]
    A1 --> A2[Azure Service Bus\ndownstream notification]
    A2 --> A3[Azure Functions\nauto-block action]

    W2 -->|Delta transaction log| C1[Purview auto-scan\nschema + lineage refresh]
    C1 --> C2[Synapse Serverless\nnew partition visible]

    F1 -->|pod crash| ERR[Flink restart from\nRocksDB checkpoint on ADLS]
    ERR --> F1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Event Broker | Azure Event Hubs (Kafka API) | SASL/OAUTHBEARER auth; Kafka 2.x protocol compatible |
| CDC Capture | Debezium → Event Hubs (Kafka Connect) | PostgreSQL/MySQL/SQL Server connectors |
| Schema Registry | Confluent Schema Registry on AKS | Avro; managed as AKS Deployment |
| Stream Processor | Apache Flink on AKS (Flink Operator) | Kubernetes-native; RocksDB state; ADLS checkpoints |
| State Backend | RocksDB + ADLS Gen2 checkpoints | Incremental checkpoint every 30s; exactly-once sink |
| Hot Store | Azure Cosmos DB (NoSQL API) | Async I/O source for enrichment lookups + output sink |
| Cold Store Format | Delta Lake on ADLS Gen2 | Flink `delta-flink` connector; ACID transactions |
| Cold Query Engine | Azure Synapse Serverless SQL | External tables on Delta ADLS; no data movement |
| Catalog | Microsoft Purview | Auto-scan ADLS Delta; lineage from Event Hubs → Flink → Delta |
| BI Layer | Apache Superset / Power BI | Superset via Synapse Serverless; Power BI DirectQuery |
| ML Training | Azure Machine Learning | ADLS Delta tables as MLTable datasets |
| Event Routing | Azure Service Bus | Durable delivery for CEP alert outputs |
| Serverless Triggers | Azure Functions | Cosmos change feed; Service Bus message triggers |
| Monitoring | Prometheus + Grafana (AKS) | Flink job metrics, Kafka consumer lag |
| Orchestration | Apache Airflow (Azure MWAA / AKS) | Delta compaction jobs, backfill Flink jobs |
| Encryption | Azure Key Vault | ADLS CMK, Cosmos DB CMK, AKS secrets via CSI driver |

---

## Comparison vs 4.3 (Azure Managed)

| Dimension | 4.4 Azure OSS | 4.3 Azure Managed |
|-----------|--------------|------------------|
| Stream processor | Apache Flink (Java/Python/SQL) | Azure Stream Analytics (SQL only) |
| CEP capabilities | Full Flink CEP library | None (ASA limitation) |
| Latency | 50–200ms | ~1s (ASA checkpoint interval) |
| Delta writes | Flink native delta-flink connector | ADF batch conversion |
| Schema enforcement | Confluent Schema Registry (OSS) | Event Hubs Schema Registry |
| Ops overhead | Medium (AKS + Flink operator) | Very low |
| Portability | High (Kafka + Flink portable) | Low (ASA proprietary) |
| Cost | AKS node hours (predictable) | ASA SU-hours (variable) |

---

## When to Choose This Implementation

✅ Complex CEP patterns required (fraud, sequence detection, graph traversal)
✅ Sub-200ms stream processing latency needed
✅ Kafka API compatibility for producer SDK reuse
✅ Team has Flink expertise
✅ Want portability to avoid ASA proprietary SQL lock-in

❌ Team unfamiliar with Kubernetes / Flink operations → use 4.3
❌ SQL-only streaming team, no Flink engineers → use 4.3
❌ Pure Azure-managed ops required → use 4.3
