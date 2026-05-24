---
layout: default
title: "2.7 — Data Lake · Multi-Cloud OSS Portable"
---

# 2.7 — Data Lake · Multi-Cloud OSS Portable

**Stack:** S3 / ADLS / GCS · Trino · Apache Atlas · Airflow · Superset
**Processing:** Batch-first · Schema-on-Read · Federated Query across clouds
**Buy vs Build:** Build (cloud-agnostic OSS — maximum portability, highest ops overhead)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES  (any cloud or on-prem)                                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ AWS RDS  │  │ Azure SQL│  │ GCP Cloud│  │  SaaS    │  │ On-prem  │   │
│  │          │  │          │  │ SQL      │  │  APIs    │  │  DBs     │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION — Airbyte + Debezium + Kafka                                     │
│  ┌───────────────────────────────────────────────────────┐                 │
│  │  Airbyte (centralised connector hub — cloud-agnostic) │                 │
│  │  Debezium (CDC from any RDBMS → Kafka)                │                 │
│  │  Kafka Connect (any cloud object store sink)          │                 │
│  └───────────────────────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│  AWS S3          │  │  Azure ADLS Gen2 │  │  GCP GCS         │
│  LANDING / RAW   │  │  LANDING / RAW   │  │  LANDING / RAW   │
│  / CURATED       │  │  / CURATED       │  │  / CURATED       │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         └───────────────────┬─┘                      │
                              └──────────────────┬─────┘
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Apache Spark on Kubernetes (cloud-agnostic)                   │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Spark Operator on K8s — runs on any cloud                   │          │
│  │  Reads from and writes to any object store via Hadoop S3A     │          │
│  │  Landing → Raw → Curated per source cloud                    │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Apache Atlas (on K8s)                                            │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Single unified catalog across all clouds                    │          │
│  │  Registers tables from S3, ADLS, GCS — all in one place      │          │
│  │  Lineage graph · data classification · business glossary      │          │
│  │  Apache Ranger for policy enforcement                         │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                    │ table locations → object stores
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CONSUMPTION — Trino (federated query across all clouds)                    │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Trino Connectors:                                           │          │
│  │  • Hive connector  → S3 + ADLS + GCS (via catalog)          │          │
│  │  • Iceberg connector → open table format (optional)          │          │
│  │  • RDBMS connectors → query operational DBs directly         │          │
│  └──────────────────────────────────────────────────────────────┘          │
│                                                                             │
│  Trino → Superset (dashboards) · Jupyter (exploration)                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart TD
    subgraph Sources
        A1[(AWS RDS)]
        A2[(Azure SQL)]
        A3[(GCP Cloud SQL)]
        A4[SaaS / Files]
    end

    subgraph Ingestion["Airbyte + Debezium + Kafka"]
        B1[Debezium CDC\nall sources]
        B2[Airbyte\nSaaS connectors]
    end

    subgraph Storage["Multi-Cloud Object Storage"]
        C1[🪣 AWS S3\nLanding/Raw/Curated]
        C2[🪣 Azure ADLS\nLanding/Raw/Curated]
        C3[🪣 GCP GCS\nLanding/Raw/Curated]
    end

    subgraph Processing["Spark on K8s (any cloud)"]
        P1[Landing → Raw\nconvert + partition]
        P2[Raw → Curated\nclean + conform]
    end

    subgraph Catalog["Apache Atlas — Unified Catalog"]
        D1[All tables\nAll clouds]
    end

    subgraph Consume
        E1[Trino\nFederated SQL\nacross all stores]
        E2[Superset\nDashboards]
        E3[Jupyter\nExploration]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B2

    B1 --> C1
    B1 --> C2
    B1 --> C3
    B2 --> C1

    C1 --> P1 --> P2
    C2 --> P1
    C3 --> P1

    P2 --> D1

    D1 -->|table locations\nacross all clouds| E1
    E1 --> E2
    E1 --> E3
```

---

## Why Multi-Cloud for a Data Lake?

```
Use case                              Data lands where source is
──────────────────────────────────    ────────────────────────────────────
M&A — acquired company on Azure       ADLS; avoid egress; process in-place
Compliance: EU data must stay in GCP  GCS eu-region; no cross-region copy
Cost: AWS Spot for processing         Spark on AWS EMR Spot, reads ADLS/GCS
DR: replicate curated to 2nd cloud    S3 primary → GCS replica via rclone
Avoid egress: SaaS data born in AWS   Land in S3; federate via Trino
```

---

## Component Map

| Component | Tool | Deployment |
|-----------|------|-----------|
| Object Storage | S3 + ADLS + GCS | One primary per data domain |
| CDC Ingestion | Debezium | K8s; writes to Kafka |
| SaaS Ingestion | Airbyte | K8s; cloud-agnostic |
| Broker | Apache Kafka | K8s (Strimzi operator) |
| Processing | Spark on K8s | Spark Operator; any cloud K8s |
| Catalog | Apache Atlas | K8s; single instance across clouds |
| Policy | Apache Ranger | Sidecar to Atlas |
| Query | Trino | K8s; multiple workers |
| Dashboards | Apache Superset | K8s; connects to Trino |
| Orchestration | Apache Airflow | K8s; cloud-agnostic DAGs |

---

## Trade-offs

| Factor | Rating | Notes |
|--------|--------|-------|
| Portability | ★★★★★ | No cloud-specific services |
| Operational overhead | ★☆☆☆☆ | Highest — all OSS to operate |
| Cost at scale | ★★★★☆ | No managed service markup |
| Vendor lock-in | ★★★★★ | Minimal — swap clouds freely |
| Egress costs | ★★☆☆☆ | Cross-cloud reads incur egress |
| Federated query speed | ★★★☆☆ | Trino good but cross-cloud slower |
