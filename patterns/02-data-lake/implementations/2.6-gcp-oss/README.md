---
layout: default
title: "2.6 — Data Lake · GCP OSS on Cloud"
---

# 2.6 — Data Lake · GCP OSS on Cloud

**Stack:** GCS · Spark on Dataproc · Hive Metastore · Trino · Airflow · Superset
**Processing:** Batch-first · Schema-on-Read
**Buy vs Build:** Build (OSS on GCP managed infra)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐                  │
│  │ Cloud SQL│  │ SaaS APIs│  │  Files   │  │  Kafka   │                  │
│  │ / AlloyDB│  │          │  │  on GCS  │  │ on GKE   │                  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘                  │
└───────┼─────────────┼─────────────┼──────────────┼──────────────────────┘
        ▼             ▼             ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  Debezium       │  │  Airbyte        │  │  Kafka Connect  │            │
│  │  (CDC → Kafka)  │  │  on GKE         │  │  GCS Sink       │            │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘            │
└───────────┼────────────────────┼────────────────────┼────────────────────┘
            └────────────────────┼────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — Google Cloud Storage (GCS)                                       │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  gs://landing/  │──▶│  gs://raw/      │──▶│  gs://curated/  │          │
│  │  original files │   │  Parquet+Snappy │   │  clean Parquet  │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Apache Spark on Dataproc                                      │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Dataproc (managed Hadoop/Spark clusters; auto-scaling)       │          │
│  │  Landing → Raw  : convert, partition by date                 │          │
│  │  Raw → Curated  : dedup, type-cast, business rules           │          │
│  │  Dataproc Serverless for ad-hoc Spark workloads              │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG — Hive Metastore on Dataproc Metastore                             │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Dataproc Metastore (managed Hive Metastore service)         │          │
│  │  Shared across Spark + Trino + Dataproc jobs                 │          │
│  │  GCS locations registered as external tables                 │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
           │                                      │
           ▼                                      ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  gs://raw/          │               │  gs://curated/                       │
└──────────┬──────────┘               └──────────────┬───────────────────────┘
           ▼                                         ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  Spark / Vertex AI  │               │  Trino on GKE → ad-hoc SQL           │
│  (ML training)      │               │  Superset     → dashboards           │
└─────────────────────┘               └──────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Cloud SQL / AlloyDB)]
        A2[SaaS APIs]
        A3[Kafka on GKE]
    end

    subgraph Ingestion
        B1[Debezium CDC]
        B2[Airbyte on GKE]
        B3[Kafka Connect GCS Sink]
    end

    subgraph GCS["Google Cloud Storage"]
        C1[🪣 gs://landing/]
        C2[🪣 gs://raw/ Parquet]
        C3[🪣 gs://curated/ Parquet]
    end

    subgraph Catalog["Dataproc Metastore\nHive Metastore"]
        D1[Table Definitions\nGCS Locations]
    end

    subgraph Consume
        E1[Trino on GKE\nAd-hoc SQL]
        E2[Superset\nDashboards]
        E3[Spark / Vertex AI\nML Training]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1

    C1 -->|Spark Job on Dataproc\nconvert + partition| C2
    C2 -->|Spark Job on Dataproc\nclean + conform| C3

    C2 --> D1
    C3 --> D1

    D1 -->|location → GCS raw| C2
    D1 -->|location → GCS curated| C3

    C2 --> E3
    C3 --> E1
    C3 --> E2
```

---

## Component Map

| Component | Tool | Notes |
|-----------|------|-------|
| Object Storage | GCS | Same zones as managed; IAM per bucket |
| CDC Ingestion | Debezium on GKE | WAL-based; low source impact |
| SaaS Ingestion | Airbyte on GKE | Self-hosted; portable across clouds |
| Stream Ingestion | Kafka Connect GCS Sink | Parquet output; configurable buffer |
| Processing | Spark on Dataproc | Managed Hadoop/Spark; ephemeral clusters |
| Catalog | Dataproc Metastore (Hive) | Managed Hive; shared by Spark + Trino |
| Ad-hoc Query | Trino on GKE | Federated query beyond GCS |
| Dashboards | Apache Superset on GKE | Connects to Trino |
| ML | Vertex AI or self-hosted MLflow | Reads GCS directly |
| Orchestration | Cloud Composer (Airflow) | DAGs submit Dataproc jobs |
| Access Control | IAM + Apache Ranger | Fine-grained column/row policies |
