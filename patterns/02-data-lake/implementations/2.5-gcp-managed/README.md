---
layout: default
title: "2.5 — Data Lake · GCP Fully Managed"
---

# 2.5 — Data Lake · GCP Fully Managed

**Stack:** GCS · Cloud Dataflow · Dataplex · BigQuery External Tables · Data Catalog
**Processing:** Batch-first · Schema-on-Read
**Buy vs Build:** Buy (fully managed GCP-native services)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                               │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Cloud SQL│  │ SaaS Apps│  │  Files   │  │ Pub/Sub  │  │  IoT     │   │
│  │ / AlloyDB│  │(SFDC/SAP)│  │(GCS drops│  │ Topics   │  │  Core    │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘   │
└───────┼─────────────┼─────────────┼──────────────┼─────────────┼──────────┘
        ▼             ▼             ▼              ▼             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐            │
│  │  Datastream     │  │  Cloud Data      │  │  Pub/Sub +      │            │
│  │  (CDC → GCS)    │  │  Fusion / AIS    │  │  Dataflow       │            │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘            │
└───────────┼────────────────────┼────────────────────┼────────────────────┘
            └────────────────────┼────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE — Google Cloud Storage (GCS)                                       │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │  LANDING        │──▶│   RAW           │──▶│  CURATED        │          │
│  │  gs://landing/  │   │  gs://raw/      │   │  gs://curated/  │          │
│  │  original files │   │  Parquet+Snappy │   │  clean Parquet  │          │
│  └─────────────────┘   └─────────────────┘   └─────────────────┘          │
│                                                                             │
│  IAM + VPC Service Controls · CMEK via Cloud KMS                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PROCESSING — Cloud Dataflow (managed Apache Beam)                          │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Dataflow Jobs (auto-scaling, serverless)                     │          │
│  │  Landing → Raw  : format conversion, partitioning            │          │
│  │  Raw → Curated  : dedup, type-cast, conform                  │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  CATALOG & GOVERNANCE — Dataplex + Data Catalog                             │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │  Dataplex : manages lakes, zones, assets across GCS + BQ     │          │
│  │  Data Catalog : asset discovery, schema, tags, lineage        │          │
│  │  DLP API  : auto PII classification on GCS assets            │          │
│  └──────────────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────────────┘
           │ schema + location                    │ schema + location
           ▼                                      ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  gs://raw/          │               │  gs://curated/                       │
└──────────┬──────────┘               └──────────────┬───────────────────────┘
           │                                         │
           ▼                                         ▼
┌─────────────────────┐               ┌──────────────────────────────────────┐
│  Vertex AI          │               │  BigQuery External Tables → SQL      │
│  (ML training)      │               │  Looker Studio    → dashboards       │
│                     │               │  Looker           → semantic layer   │
└─────────────────────┘               └──────────────────────────────────────┘
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Cloud SQL / AlloyDB)]
        A2[SaaS APIs]
        A3[Pub/Sub Topics]
        A4[Files on GCS]
    end

    subgraph Ingestion
        B1[Datastream\nCDC to GCS]
        B2[Cloud Data Fusion\nSaaS connectors]
        B3[Dataflow\nPub/Sub → GCS]
    end

    subgraph GCS["Google Cloud Storage"]
        C1[🪣 gs://landing/]
        C2[🪣 gs://raw/ Parquet]
        C3[🪣 gs://curated/ Parquet]
    end

    subgraph Catalog["Dataplex + Data Catalog"]
        D1[Zone Assets\nSchema + Tags + Lineage]
    end

    subgraph Consume
        E1[BigQuery\nExternal Tables + SQL]
        E2[Looker / Looker Studio\nDashboards]
        E3[Vertex AI\nML Training]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> B3 --> C1
    A4 --> C1

    C1 -->|Dataflow Job\nconvert + partition| C2
    C2 -->|Dataflow Job\nclean + conform| C3

    C2 --> D1
    C3 --> D1

    D1 -->|location → GCS raw| C2
    D1 -->|location → GCS curated| C3

    C2 --> E3
    C3 --> E1
    C3 --> E2
```

---

## Dataplex Zone Mapping

```
Dataplex Lake: enterprise-data-lake
│
├── Raw Zone  (type: RAW)
│   └── Asset → gs://raw/
│       • Schema-on-read
│       • No quality rules enforced
│
└── Curated Zone  (type: CURATED)
    └── Asset → gs://curated/
        • Dataplex enforces data quality checks
        • Auto-registered in Data Catalog
        • DLP scan for PII on ingest
```

---

## Component Map

| Component | GCP Service | Notes |
|-----------|------------|-------|
| Object Storage | Google Cloud Storage | Multi-region buckets for HA |
| CDC Ingestion | Datastream | MySQL, PostgreSQL, Oracle → GCS in Avro/JSON |
| SaaS Ingestion | Cloud Data Fusion | Managed CDAP; 150+ connectors |
| Stream Ingestion | Pub/Sub + Dataflow | Pub/Sub → Dataflow → GCS in Parquet |
| Processing | Cloud Dataflow | Serverless Apache Beam; auto-scale |
| Catalog | Dataplex + Data Catalog | Zone management + tag templates |
| PII Detection | Cloud DLP API | Auto-scan GCS on landing |
| Ad-hoc Query | BigQuery External Tables | Query GCS Parquet without loading |
| Dashboards | Looker Studio / Looker | Looker for semantic layer |
| ML | Vertex AI | Dataset registry points to GCS |
| Orchestration | Cloud Composer (Airflow) | DAGs trigger Dataflow jobs |
| Access Control | IAM + VPC Service Controls | Service account per pipeline step |
| Encryption | Cloud KMS (CMEK) | Customer-managed keys on GCS |

---

## vs Other Managed Implementations

| | 2.1 AWS | 2.3 Azure | 2.5 GCP |
|--|---------|-----------|---------|
| Storage | S3 | ADLS Gen2 | GCS |
| Catalog | Glue Data Catalog | Purview | Dataplex + Data Catalog |
| Processing | Glue (Spark) | ADF Data Flows | Dataflow (Beam) |
| Query Engine | Athena | Synapse Serverless | BigQuery External Tables |
| Lineage | Limited | ADF auto-capture | Dataplex auto-capture |
| BI | QuickSight | Power BI | Looker / Looker Studio |
| Best for | AWS-first orgs | Microsoft / O365 orgs | GCP / Google Workspace orgs |
