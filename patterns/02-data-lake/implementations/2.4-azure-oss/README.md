---
layout: default
title: "2.4 — Data Lake · Azure OSS on Cloud"
---

# 2.4 — Data Lake · Azure OSS on Cloud

**Stack:** ADLS Gen2 · Spark on AKS · Apache Atlas · Trino · Airflow · Superset
**Processing:** Batch-first · Schema-on-Read
**Buy vs Build:** Build (OSS on Azure managed infra)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ Azure SQL│  │ SaaS APIs│  │  Files   │  │  Kafka   │                  │
│  │ / Postgres│  │          │  │          │  │ on AKS   │                  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
└───────┼─────────────┼─────────────┼──────────────┼──────────────────────┘
        ▼             ▼             ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  Debezium       │  │  Airbyte        │  │  Kafka Connect  │            │
│  │  (CDC → Kafka)  │  │  (SaaS / Files) │  │  ADLS Sink      │            │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘            │
└───────────┼────────────────────┼────────────────────┼────────────────────┘
            └────────────────────┼────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — ADLS Gen2                                                        │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  LANDING        │──▶│   RAW           │──▶│  CURATED        │          │
│  │  /landing/      │   │  /raw/          │   │  /curated/      │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Apache Spark on AKS                                           │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Spark Operator on Kubernetes                                │          │
│  │  Landing → Raw : format conversion, partitioning             │          │
│  │  Raw → Curated : dedup, type casting, business rules         │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Apache Atlas (on AKS)                                            │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Hive-compatible metastore · lineage graph · classifications  │          │
│  │  Ranger integration for access policy enforcement             │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
           │                                      │
           ▼                                      ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  ADLS /raw/         │               │  ADLS /curated/                      │
└──────────┬──────────┘               └──────────────┬───────────────────────┘
           ▼                                         ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  Spark / Azure ML   │               │  Trino       → ad-hoc SQL            │
│  (ML training)      │               │  Superset    → dashboards            │
└─────────────────────┘               └──────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Azure SQL)]
        A2[SaaS APIs]
        A3[Kafka on AKS]
    end

    subgraph Ingestion
        B1[Debezium CDC]
        B2[Airbyte]
        B3[Kafka Connect\nADLS Sink]
    end

    subgraph ADLS["ADLS Gen2"]
        C1[📁 /landing/]
        C2[📁 /raw/ Parquet]
        C3[📁 /curated/ Parquet]
    end

    subgraph Catalog["Apache Atlas"]
        D1[Table Definitions\nLineage Graph]
    end

    subgraph Consume
        E1[Trino\nAd-hoc SQL]
        E2[Superset\nDashboards]
        E3[Spark / Azure ML\nML Training]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1

    C1 -->|Spark Job\nconvert + partition| C2
    C2 -->|Spark Job\nclean + conform| C3

    C2 --> D1
    C3 --> D1

    D1 -->|location → ADLS raw| C2
    D1 -->|location → ADLS curated| C3

    C2 --> E3
    C3 --> E1
    C3 --> E2
```

---

## Component Map

| Component | Tool | Notes |
|-----------|------|-------|
| Object Storage | ADLS Gen2 | Hierarchical namespace; AAD + ACL security |
| CDC Ingestion | Debezium on AKS | PostgreSQL / SQL Server WAL → Kafka |
| SaaS Ingestion | Airbyte on AKS | Self-hosted; 300+ connectors |
| Stream Ingestion | Kafka Connect ADLS Sink | Kafka → ADLS in Parquet |
| Processing | Spark on AKS (Spark Operator) | K8s-native; auto-scale via KEDA |
| Catalog | Apache Atlas on AKS | Lineage + classification; Ranger policies |
| Ad-hoc Query | Trino on AKS | Federated across ADLS + other stores |
| Dashboards | Apache Superset on AKS | Connects to Trino |
| ML | Azure ML or self-hosted MLflow | Reads raw zone via ADLS SDK |
| Orchestration | Apache Airflow on AKS | DAGs trigger Spark jobs |
| Access Control | Apache Ranger + AAD | Tag-based + role-based policies |
