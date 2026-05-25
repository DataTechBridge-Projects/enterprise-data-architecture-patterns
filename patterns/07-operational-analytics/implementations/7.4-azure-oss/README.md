---
layout: default
title: "7.4 — Operational Analytics · Azure OSS on Cloud"
---

# 7.4 — Operational Analytics · Azure OSS on Cloud

**Stack:** PostgreSQL · Debezium · Azure Event Hubs · Apache Flink on AKS · Delta Lake · Trino · Apache Superset
**Processing:** Streaming-first · sub-15-second latency
**Buy vs Build:** Build (OSS on Azure cloud infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Operational Sources"]
        S1[PostgreSQL on Azure\nOrders · Inventory]
        S2[MySQL on Azure\nCRM · Legacy apps]
        S3[MongoDB Atlas\nProduct catalog]
        S4[App Services\nREST event stream]
    end

    subgraph CDC["CDC Layer — Debezium on AKS"]
        I1[Debezium PostgreSQL\nConnector]
        I2[Debezium MySQL\nConnector]
        I3[Debezium MongoDB\nConnector]
    end

    subgraph BROKER["Azure Event Hubs — Kafka API"]
        K1[Topic: cdc.postgres.*]
        K2[Topic: cdc.mysql.*]
        K3[Topic: app.events]
    end

    subgraph PROC["Stream Processing — Apache Flink on AKS"]
        P1[Flink Upsert Job\nDelta Lake MERGE]
        P2[Flink Enrichment Job\ndim lookups]
        P3[Flink Window Agg\nrolling metrics]
    end

    subgraph STORAGE["ODS — Delta Lake on ADLS Gen2"]
        O1[delta.ods\ncurrent state]
        O2[delta.agg\nrolling aggregates]
        O3[delta.history\nfull CDC log]
    end

    subgraph CATALOG["Catalog\nAzure Purview + Unity Catalog"]
        C1[Purview Catalog\nlineage + tags]
        C2[Apache Ranger\nTrino RBAC]
    end

    subgraph CONSUME["Consumption"]
        F1[Superset\nOperational Dashboards]
        F2[Trino on AKS\nAd-hoc SQL]
        F3[App API\nDelta REST / Trino]
        F4[Flink CEP\nReal-time alerts]
    end

    SRC --> CDC
    I1 --> BROKER
    I2 --> BROKER
    I3 --> BROKER
    BROKER --> PROC
    P1 --> O1
    P2 --> O1
    P3 --> O2
    BROKER --> O3
    O1 & O2 -. register .-> C1
    C1 -. enforce .-> C2
    C2 --> F1
    C2 --> F2
    O1 --> F3
    PROC --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(PostgreSQL)]
        A2[(MySQL)]
        A3[(MongoDB)]
        A4[App REST Events]
    end

    subgraph Debezium["Debezium on AKS"]
        B1[PG Connector]
        B2[MySQL Connector]
        B3[MongoDB Connector]
        B4[HTTP Source Connector]
    end

    subgraph EventHubs["Azure Event Hubs — Kafka Topics"]
        C1[cdc.* topics]
        C2[app.events]
    end

    subgraph Flink["Apache Flink on AKS"]
        D1[Upsert → Delta ODS]
        D2[Enrichment join]
        D3[Window Agg → Delta AGG]
    end

    subgraph Delta["Delta Lake on ADLS Gen2"]
        E1[ods tables]
        E2[agg tables]
    end

    subgraph Catalog["Purview + Ranger"]
        F1[Catalog + Lineage]
        F2[RBAC Policies]
    end

    subgraph Consume
        G1[Trino\nAd-hoc SQL]
        G2[Superset\nDashboards]
        G3[App API]
        G4[Flink CEP\nAlerts]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> B4 --> C2

    C1 --> D1 --> E1
    C1 --> D2 --> E1
    C1 & C2 --> D3 --> E2

    E1 & E2 -->|scan| F1 --> F2

    F2 -->|reads| G1
    F2 -->|reads| G2
    E1 -->|REST| G3
    C1 --> G4
```

---

## Zone Design

```
ADLS Gen2: adls://<company>-ops-delta/
│
├── ods/
│   └── {domain}/{entity}/             -- Delta table (current state)
│       Flink MERGE on primary key; CDF (Change Data Feed) enabled
│
├── agg/
│   └── {domain}/{metric}/{grain}/     -- Delta table (rolling aggregates)
│       Flink tumbling window: 1-min, 5-min, 1-hr
│
└── history/
    └── cdc/{topic}/year=YYYY/month=MM/day=DD/
        Raw Event Hubs events archived as Delta; TTL 90 days
```

---

## Security Model

```
┌──────────────────────────────────────────────────────┐
│         Apache Ranger + Azure AD + ADLS ACLs          │
│                                                       │
│  Azure AD SP / Group     Access Level  Layer          │
│  ──────────────────────  ────────────  ──────────     │
│  debezium-sp             Produce only  Event Hubs     │
│  flink-job-sp            Read + Write  Event Hubs + ADLS |
│  ops-engineer            Read + Write  All Delta      │
│  data-analyst            Read only     ods · agg      │
│  app-service-sp          Read only     ods (cols)     │
│  superset-sp             Read only     agg            │
│                                                       │
│  Column masking → Ranger masking on Trino queries     │
│  Row filtering  → Trino row-level filter policies     │
│  Encryption     → Azure KMS (CMK) on ADLS Gen2        │
│  mTLS           → Event Hubs SASL_SSL + AKS mTLS      │
└──────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⚡ Debezium\ncontinuous CDC]
    T2[⚡ Flink Jobs\nalways-on streaming]
    T3[⏰ Airflow DAG\nhourly Delta OPTIMIZE]
    T4[⏰ Airflow DAG\ndaily VACUUM]

    T1 -->|change events| K1[Event Hubs Topics]
    K1 --> F1[Flink Upsert Job\n→ Delta ODS]
    K1 --> F2[Flink Window Agg\n→ Delta AGG]
    K1 --> F3[Flink CEP\nalerts]

    T3 --> C1[Delta OPTIMIZE\nsmall-file compaction]
    T4 --> C2[Delta VACUUM\nremove old snapshots]

    F1 -->|lag > 30s| A1[Azure Monitor Alert\n→ Teams / PagerDuty]
    F2 -->|fail| A1
    C1 -->|fail| A2[Airflow Alert → Slack]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| OLTP Sources | PostgreSQL / MySQL / MongoDB on Azure | WAL / binlog / oplog enabled |
| CDC Engine | Debezium on AKS (Kafka Connect) | Connector pods per source |
| Message Broker | Azure Event Hubs (Kafka API) | Kafka-compatible; managed |
| Stream Processor | Apache Flink on AKS | Upsert, enrichment, window agg |
| ODS Store | Delta Lake on ADLS Gen2 | MERGE on PK; CDF enabled |
| Aggregate Store | Delta Lake on ADLS Gen2 | Tumbling window output |
| Table Catalog | Azure Purview + Delta log | Auto-scan; lineage tracking |
| Access Control | Apache Ranger on Trino | RBAC + column masking |
| Query Engine | Trino on AKS | Delta Lake connector |
| BI / Dashboards | Apache Superset | Connects via Trino SQLAlchemy |
| App-facing API | Trino JDBC / Delta REST | HTTP query for microservices |
| CEP Alerts | Flink CEP | Pattern match → Azure Service Bus |
| Compaction | Delta OPTIMIZE + VACUUM via Airflow | Hourly small-file merge |
| Orchestration | Apache Airflow on AKS | Maintenance + batch DAGs |
| Monitoring | Azure Monitor + Prometheus + Grafana | Flink, Event Hubs, AKS metrics |

---

## Comparison vs 7.3 (Azure Managed)

| Dimension | 7.4 Azure OSS | 7.3 Azure Fully Managed |
|-----------|--------------|------------------------|
| CDC Engine | Debezium | Synapse Link (zero ETL) |
| Message Bus | Event Hubs (Kafka API) | None / Synapse Link |
| Stream Processor | Apache Flink | Synapse pipelines |
| ODS Store | Delta Lake on ADLS | Synapse Dedicated Pool |
| Latency | ~5–15 s | ~1–5 min |
| Open Format | Yes (Delta Lake) | No (Synapse proprietary) |
| Ops Overhead | High (AKS + Debezium) | Low |
| Cost at Scale | Lower (ADLS storage) | Higher (DWU cost) |

---

## When to Choose This Implementation

✅ Need sub-15-second end-to-end latency  
✅ Open table format required (Delta Lake)  
✅ Sources outside Azure SQL / Cosmos DB (PostgreSQL, MySQL, MongoDB)  
✅ Team has Flink + Kafka expertise  
✅ Trino multi-engine consumption required  

❌ No Flink/Kafka expertise → use 7.3 (Synapse Link)  
❌ Power BI DirectQuery preferred → use 7.3 (Synapse)  
❌ Fully managed preference → use 7.3
