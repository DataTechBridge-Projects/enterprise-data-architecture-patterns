---
layout: default
title: "7.8 — Operational Analytics · Hybrid OSS Self-Hosted"
---

# 7.8 — Operational Analytics · Hybrid OSS Self-Hosted

**Stack:** PostgreSQL · Debezium · Apache Kafka · Apache Pinot / Apache Druid · Apache Superset · Apache Ranger
**Processing:** Streaming-first · sub-second query latency · full self-hosted
**Buy vs Build:** Full Build (on-prem + cloud hybrid, maximum control)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources — On-Prem + Cloud"]
        S1[PostgreSQL\non-prem primary]
        S2[Oracle DB\nlegacy ERP on-prem]
        S3[MySQL\ncloud instances]
        S4[App Services\nREST / gRPC events]
    end

    subgraph CDC["CDC Layer — Debezium on K8s"]
        I1[Debezium PG\nConnector]
        I2[Debezium Oracle\nLogMiner Connector]
        I3[Debezium MySQL\nConnector]
        I4[Kafka Connect\nHTTP Source]
    end

    subgraph BROKER["Apache Kafka — On-Prem / Cloud"]
        K1[Topic: cdc.postgres.*]
        K2[Topic: cdc.oracle.*]
        K3[Topic: app.events]
    end

    subgraph PROC["Real-Time Ingestion — Apache Pinot + Druid"]
        P1[Pinot Realtime Table\nKafka stream ingest]
        P2[Druid Kafka Supervisor\nstream ingest]
        P3[Pinot Offline Table\nbatch historical]
    end

    subgraph STORAGE["ODS — Apache Pinot / Druid + Object Store"]
        O1[Pinot Segments\nsub-second OLAP]
        O2[Druid Segments\ntime-series rollup]
        O3[MinIO / S3\nDeep storage]
    end

    subgraph CATALOG["Catalog & Governance\nApache Atlas + Apache Ranger"]
        C1[Apache Atlas\nlineage + tags]
        C2[Apache Ranger\nRBAC enforcement]
    end

    subgraph CONSUME["Consumption"]
        F1[Apache Superset\nOperational Dashboards]
        F2[Pinot SQL / Druid SQL\nAd-hoc queries]
        F3[App API\nPinot REST / Druid REST]
        F4[Kafka Streams / KSQL\nReal-time alerts]
    end

    SRC --> CDC
    I1 --> BROKER
    I2 --> BROKER
    I3 --> BROKER
    I4 --> BROKER
    BROKER --> PROC
    P1 --> O1
    P2 --> O2
    P3 --> O3
    O1 & O2 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    O1 --> F3
    BROKER --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(PostgreSQL on-prem)]
        A2[(Oracle on-prem)]
        A3[(MySQL cloud)]
        A4[App gRPC / REST]
    end

    subgraph Debezium["Debezium on K8s"]
        B1[PG Connector]
        B2[Oracle LogMiner]
        B3[MySQL Connector]
        B4[HTTP Source]
    end

    subgraph Kafka["Apache Kafka — Multi-broker cluster"]
        C1[cdc.* topics]
        C2[app.events]
    end

    subgraph Pinot["Apache Pinot"]
        D1[Realtime Table\nKafka → segments]
        D2[Offline Table\nbatch historical]
    end

    subgraph Druid["Apache Druid"]
        D3[Kafka Supervisor\nrolling aggregates]
        D4[Batch Ingestion\nhistorical backfill]
    end

    subgraph DeepStore["Deep Storage — MinIO / S3"]
        E1[Pinot segments]
        E2[Druid segments]
    end

    subgraph Catalog["Atlas + Ranger"]
        F1[Lineage + Tags]
        F2[RBAC Policies]
    end

    subgraph Consume
        G1[Superset\nDashboards]
        G2[Pinot SQL\nAd-hoc]
        G3[App REST API\nPinot Broker]
        G4[Kafka Streams\nCEP Alerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C2

    C1 --> D1 --> E1
    C1 & C2 --> D3 --> E2
    C2 --> D2 --> E1

    E1 & E2 -->|register| F1 --> F2

    F2 -->|reads| G1
    F2 -->|reads| G2
    E1 -->|REST API| G3
    C1 --> G4
```

---

## Zone Design

```
MinIO / S3 (deep storage):
│
├── pinot-segments/
│   └── {table}/REALTIME/{date}/     -- Pinot realtime segments (Kafka-consumed)
│   └── {table}/OFFLINE/{date}/      -- Pinot offline segments (batch)
│
├── druid-segments/
│   └── {datasource}/{interval}/     -- Druid time-series segments
│       Granularity: MINUTE / HOUR / DAY
│
└── kafka-archive/
    └── {topic}/year=YYYY/month=MM/day=DD/
        Raw Kafka topics archived (Kafka Connect S3 Sink); TTL 90d

Kafka (on-prem cluster):
  └── Partitions: 12–48 per topic (based on throughput)
  └── Retention: 7 days (raw topics) / 30 days (agg topics)

Pinot:
  └── REALTIME table: Kafka direct consume; sub-second ingest
  └── OFFLINE table: batch merge for historical accuracy
  └── Hybrid table: unified query across both
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Apache Ranger + Kerberos + mTLS               │
│                                                       │
│  Role / Principal        Access Level  System         │
│  ──────────────────────  ────────────  ──────────     │
│  debezium/kafka          Produce only  Kafka topics   │
│  pinot-server            Read + Write  MinIO segments │
│  druid-middlemanager     Read + Write  MinIO segments │
│  ops-engineer            Admin         All systems    │
│  data-analyst            Read only     Pinot · Druid  │
│  app-service             Read only     Pinot REST API │
│  superset                Read only     Pinot · Druid  │
│                                                       │
│  Column masking → Ranger Pinot/Druid policies         │
│  Row filtering  → Ranger row-level filters            │
│  Encryption     → TLS in-transit; AES-256 at-rest     │
│  Auth           → Kerberos (on-prem) + mTLS (cloud)   │
│  Kafka          → SASL/SCRAM + TLS inter-broker       │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Debezium\ncontinuous CDC]
    T2[⚡ Pinot Realtime\nKafka stream ingest]
    T3[⚡ Druid Kafka Supervisor\nstream ingest]
    T4[⏰ Airflow DAG\nhourly Pinot offline build]
    T5[⏰ Airflow DAG\ndaily Druid compact + kill]
    T6[⚡ Kafka Streams App\nCEP anomaly detection]

    T1 -->|change events| K1[Kafka Topics]
    K1 --> T2
    K1 --> T3
    K1 --> T6

    T4 --> J1[Pinot Push Job\nbatch segments from S3]
    T5 --> J2[Druid Compact Task\nsmall segment merge]
    T5 --> J3[Druid Kill Task\nexpire old segments]

    T6 -->|pattern match| N1[Kafka Topic: alerts\n→ Slack / PagerDuty]

    T2 -->|lag > 10s| A1[Prometheus Alert\n→ PagerDuty]
    T3 -->|lag > 10s| A1
    K1 -->|consumer lag| A1
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| OLTP Sources | PostgreSQL / Oracle / MySQL (on-prem + cloud) | WAL / LogMiner / binlog |
| CDC Engine | Debezium on K8s (Kafka Connect) | Oracle LogMiner for legacy ERP |
| Message Broker | Apache Kafka (self-hosted, multi-broker) | ZooKeeper or KRaft mode |
| OLAP Engine (realtime) | Apache Pinot | Sub-second SQL on streaming data |
| OLAP Engine (time-series) | Apache Druid | Time-series rollup + retention |
| Deep Storage | MinIO (on-prem) or S3 (cloud) | Segment persistence for Pinot + Druid |
| Catalog | Apache Atlas | Lineage, tags, business glossary |
| Access Control | Apache Ranger | RBAC + column masking for Pinot/Druid |
| Query Interface | Pinot Broker SQL / Druid SQL | ANSI SQL; sub-second responses |
| BI / Dashboards | Apache Superset | Native Pinot + Druid datasources |
| App-facing API | Pinot Broker REST API | JSON response; < 100 ms p99 |
| CEP Alerts | Kafka Streams / ksqlDB | Pattern detection → alert topic |
| Compaction | Druid Compact Task + Pinot Offline merge | Daily via Airflow |
| Orchestration | Apache Airflow on K8s | Batch ingestion + maintenance |
| Monitoring | Prometheus + Grafana | Pinot JMX, Druid metrics, Kafka lag |
| Auth (on-prem) | Kerberos + LDAP | Integrated with AD/LDAP |

---

## Comparison vs 7.7 (Multi-Cloud Managed)

| Dimension | 7.8 Hybrid OSS | 7.7 Multi-Cloud Managed |
|-----------|---------------|------------------------|
| CDC Engine | Debezium | Fivetran |
| Message Bus | Kafka (self-hosted) | None (Fivetran direct) |
| ODS / OLAP Store | Apache Pinot / Druid | Snowflake |
| Query Latency | Sub-second (Pinot) | Seconds (Snowflake) |
| Data Freshness | ~1–5 s | ~5 min |
| Open Format | Yes | No (Snowflake) |
| Ops Overhead | Very High | Low |
| On-Prem Support | Yes (full) | No |
| Cost at Scale | Infrastructure cost | SaaS subscription |
| Oracle CDC | Yes (LogMiner) | Fivetran Oracle (limited) |

---

## When to Choose This Implementation

✅ On-prem operational sources that cannot send data to cloud SaaS  
✅ Oracle legacy ERP requiring LogMiner CDC  
✅ Sub-second query latency required (Pinot)  
✅ Data residency / sovereignty constraints prevent cloud SaaS  
✅ Team has deep Kafka + Pinot/Druid operational expertise  
✅ High cardinality, high-throughput time-series analytics  

❌ No Kafka/Pinot/Druid expertise → use 7.7 (Fivetran + Snowflake)  
❌ Cloud-first, no on-prem → use 7.1 (AWS) / 7.3 (Azure) / 7.5 (GCP)  
❌ Sub-5-minute latency is sufficient → use 7.7 (lower ops overhead)
