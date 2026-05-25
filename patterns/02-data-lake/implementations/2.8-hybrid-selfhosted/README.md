---
layout: default
title: "2.8 — Data Lake · Hybrid Self-Hosted"
---

# 2.8 — Data Lake · Hybrid Self-Hosted (On-Prem + Cloud)

**Stack:** MinIO / HDFS · Spark · Apache Hive · Apache Ranger · Airflow · Superset
**Processing:** Batch-first · Schema-on-Read
**Buy vs Build:** Full Build (self-hosted OSS — maximum control, highest ops cost)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources — On-Prem"]
        S1[Oracle DB]
        S2[SQL Server]
        S3[SAP ERP]
        S4[Mainframe COBOL]
        S5[Files / FTP]
    end

    subgraph INGEST["Ingestion — On-Prem"]
        I1[Debezium\nCDC → Kafka]
        I2[Apache Sqoop\nbulk extract]
        I3[Custom ETL\nSAP / Mainframe]
    end

    subgraph STORAGE["On-Prem Storage — MinIO / HDFS"]
        Z1[LANDING\n/landing/]
        Z2[RAW\n/raw/\nParquet · Snappy]
        Z3[CURATED\n/curated/\nclean Parquet]
        Z4[Cloud Object Store\nS3 / ADLS / GCS\ncold archive]
    end

    subgraph PROC["Processing — Spark on YARN"]
        P1[Spark on Hadoop YARN\nLanding → Raw → Curated]
    end

    subgraph CATALOG["Catalog & Security"]
        C1[Apache Hive Metastore\ntable definitions · partitions]
        C2[Apache Ranger\ncolumn/row security · LDAP]
    end

    subgraph CONSUME["Consumption"]
        F1[Spark / MLflow\nML training]
        F2[Trino / Presto\nad-hoc SQL]
        F3[Apache Superset\ndashboards]
    end

    SRC --> INGEST
    INGEST --> Z1 --> Z2 --> Z3
    Z3 -.->|lifecycle rule| Z4
    PROC --> Z2 & Z3
    Z2 & Z3 -. register .-> C1
    C1 -. enforce .-> C2
    Z2 --> F1
    C2 --> F2 & F3
```
└──────────┬──────────┘               └──────────────┬───────────────────────┘
           ▼                                         ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  Spark / MLflow     │               │  Trino / Presto → ad-hoc SQL         │
│  (ML training)      │               │  Apache Superset → dashboards        │
│                     │               │  Hive           → batch queries      │
└─────────────────────┘               └──────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph OnPremSources["On-Prem Sources"]
        A1[(Oracle DB)]
        A2[(SQL Server)]
        A3[SAP / Mainframe]
        A4[Files / FTP]
    end

    subgraph Ingestion["Ingestion — On-Prem"]
        B1[Debezium CDC\non-prem Kafka]
        B2[Apache Sqoop\nbulk extract]
        B3[Custom ETL\nSAP / Mainframe]
    end

    subgraph Storage["MinIO / HDFS — On-Prem"]
        C1[📁 /landing/]
        C2[📁 /raw/ Parquet]
        C3[📁 /curated/ Parquet]
        C4[☁️ Cloud Cold Storage\noptional offload]
    end

    subgraph Catalog["Hive Metastore + Ranger"]
        D1[Table Definitions\nAccess Policies]
    end

    subgraph Consume
        E1[Trino / Presto\nAd-hoc SQL]
        E2[Superset\nDashboards]
        E3[Spark / MLflow\nML Training]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> C1

    C1 -->|Spark Job\nconvert + partition| C2
    C2 -->|Spark Job\nclean + conform| C3
    C3 -.->|lifecycle policy\narchive| C4

    C2 --> D1
    C3 --> D1

    D1 -->|location → MinIO raw| C2
    D1 -->|location → MinIO curated| C3

    C2 --> E3
    C3 --> E1
    C3 --> E2
```

---

## Hybrid Cloud Offload Pattern

```
┌─────────────────────────────────────────────────────────────┐
│  DATA TEMPERATURE TIERING                                    │
│                                                              │
│  Hot  (0–90 days)    → MinIO on-prem / HDFS                 │
│  Warm (90–365 days)  → MinIO on-prem (lower-cost nodes)     │
│  Cold (365d+)        → S3 / GCS / ADLS (cloud archive tier) │
│                                                              │
│  Trino queries both on-prem and cloud transparently          │
│  Hive Metastore stores location for both tiers              │
└─────────────────────────────────────────────────────────────┘
```

---

## Component Map

| Component | Tool | Notes |
|-----------|------|-------|
| On-prem Storage | MinIO | S3-compatible API; runs on bare metal |
| Alt Storage | HDFS | For orgs with existing Hadoop clusters |
| CDC Ingestion | Debezium | Oracle / SQL Server / PostgreSQL WAL |
| Bulk Ingestion | Apache Sqoop | Full load / incremental from RDBMS |
| Message Broker | Apache Kafka on-prem | Strimzi on K8s or standalone |
| Processing | Spark on YARN / K8s | On-prem cluster; HDFS or MinIO storage |
| Catalog | Apache Hive Metastore | MySQL/PostgreSQL backend |
| Security | Apache Ranger | Column/row policies; LDAP / Active Directory |
| Ad-hoc Query | Trino or Presto | Federation across on-prem + cloud |
| Dashboards | Apache Superset | Connects to Trino |
| ML | MLflow + Spark | Self-hosted; reads from raw zone |
| Orchestration | Apache Airflow | On-prem; DAGs trigger Spark jobs |
| Cloud Offload | S3 / ADLS / GCS | Cold archive; Trino reads transparently |

---

## When to Choose This Implementation

✅ Data must not leave on-prem (regulatory, sovereignty)
✅ Existing Hadoop / on-prem investment to leverage
✅ SAP, mainframe, or legacy sources that can't egress to cloud
✅ Air-gapped environments (defence, gov, finance)
✅ Latency-sensitive ops that need local data proximity

❌ No dedicated ops team → use managed cloud (2.1 / 2.3 / 2.5)
❌ Need elastic scale on demand → cloud is cheaper
❌ Green-field deployment → start in cloud; avoid HDFS complexity

---

## vs Cloud Implementations

| | On-Prem (2.8) | Cloud Managed (2.1/2.3/2.5) |
|--|--------------|----------------------------|
| Control | Full | Limited to service APIs |
| Ops overhead | Very High | Low |
| Upfront cost | High (hardware) | Low (pay as you go) |
| Elastic scaling | Manual | Automatic |
| Data sovereignty | Guaranteed | Depends on region config |
| Legacy integration | Easier (same network) | Requires VPN / Direct Connect |
