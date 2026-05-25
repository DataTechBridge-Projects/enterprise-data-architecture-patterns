---
layout: default
title: "5.2 — Data Mesh · AWS OSS on Cloud"
---

# 5.2 — Data Mesh · AWS OSS on Cloud

**Stack:** S3 · Apache Iceberg · Trino · DataHub · dbt Core · Apache Airflow (MWAA)
**Processing:** Domain-chosen (Batch / Streaming / Hybrid per domain)
**Buy vs Build:** Build (OSS on AWS infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph GOV["Federated Governance Plane"]
        G1[DataHub\nData Catalog + Lineage + Discovery]
        G2[Apache Ranger\nFine-Grained Access Control]
        G3[OpenMetadata\nData Contract Registry]
    end

    subgraph PLATFORM["Self-Serve Data Platform"]
        P1[Terraform Modules\nDomain Bootstrap]
        P2[MWAA — Apache Airflow\nOrchestration Templates]
        P3[dbt Core\nTransformation Framework]
        P4[Great Expectations\nData Quality]
    end

    subgraph DOM_A["Domain: Sales"]
        A1[S3 — sales-raw/\nIceberg tables]
        A2[dbt Core\nTransform]
        A3[Iceberg — sales.products]
        A4[Data Product\nsales.orders_v1]
    end

    subgraph DOM_B["Domain: Marketing"]
        B1[S3 — marketing-raw/\nIceberg tables]
        B2[dbt Core\nTransform]
        B3[Iceberg — marketing.products]
        B4[Data Product\nmarketing.campaigns_v1]
    end

    subgraph DOM_C["Domain: Finance"]
        C1[S3 — finance-raw/\nIceberg tables]
        C2[dbt Core\nTransform]
        C3[Iceberg — finance.products]
        C4[Data Product\nfinance.revenue_v1]
    end

    subgraph CONSUME["Consumers"]
        E1[Trino\nFederated Cross-Domain SQL]
        E2[Apache Superset\nDashboards]
        E3[SageMaker\nML Training]
        E4[Jupyter\nData Science]
    end

    G1 -. govern .-> DOM_A & DOM_B & DOM_C
    G2 -. enforce access .-> DOM_A & DOM_B & DOM_C
    P1 -. bootstrap .-> DOM_A & DOM_B & DOM_C

    A1 --> A2 --> A3 --> A4
    B1 --> B2 --> B3 --> B4
    C1 --> C2 --> C3 --> C4

    A4 & B4 & C4 -. register .-> G1
    G1 -. discover + subscribe .-> CONSUME
    A3 & B3 & C3 --> E1 --> E2 & E3 & E4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        S1[(Operational DBs)]
        S2[SaaS APIs]
        S3[Event Streams]
    end

    subgraph DomainIngestion["Domain Ingestion (per domain)"]
        I1[Airbyte\nBatch Connectors]
        I2[Debezium + Kafka\nCDC]
        I3[Kafka → S3\nStream Landing]
    end

    subgraph DomainStorage["Domain Storage — S3 + Iceberg"]
        Z1[Iceberg Raw\ns3://domain-raw/]
        Z2[Iceberg Curated\ns3://domain-curated/]
        Z3[Iceberg Products\ns3://domain-products/]
    end

    subgraph Catalog["DataHub + Apache Ranger"]
        C1[DataHub Ingestion\nAuto Schema + Lineage]
        C2[Ranger Policies\nDomain RBAC]
        C3[DataHub Contracts\nProduct SLA Registry]
    end

    subgraph Consume
        E1[Trino\nFederated SQL]
        E2[Superset\nDashboards]
        E3[SageMaker\nML]
        E4[Jupyter Notebooks]
    end

    S1 --> I2 --> Z1
    S2 --> I1 --> Z1
    S3 --> I3 --> Z1
    Z1 -->|dbt Core| Z2 -->|dbt Core| Z3
    Z2 & Z3 -. ingest metadata .-> C1 --> C2
    Z3 -. register contract .-> C3
    Z3 -->|reads via Trino| E1 --> E2 & E3 & E4
    Z2 -->|reads data| E3
```

---

## Zone Design

```
s3://{domain}-lake/
│
├── raw/
│   └── {source}/{table}/         ← Iceberg table (metadata/ + data/)
│       └── metadata/             ← Iceberg manifest + snapshot
│       └── data/year=YYYY/month=MM/
│
├── curated/
│   └── {entity}/                 ← Iceberg table
│       └── dbt-transformed · MERGE-capable · ACID
│
└── products/
    └── {product-name}/           ← Iceberg table · schema-locked · SLA-backed
        └── version pinned in DataHub contract

Iceberg catalog: AWS Glue Data Catalog (Hive-compatible)
  database: {domain}_raw, {domain}_curated, {domain}_products
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│  Apache Ranger — domain-scoped policies                   │
│                                                           │
│  Principal             Access Level     Scope             │
│  ──────────────────    ────────────     ──────────────    │
│  domain-owner          Read + Write     Own domain tables │
│  domain-engineer       Read + Write     Own domain tables │
│  cross-domain-reader   Read only        Products schema   │
│  platform-admin        Admin            All schemas       │
│  ml-consumer           Read only        Curated + Products│
│  bi-consumer           Read only        Products only     │
│                                                           │
│  Ranger Tag Policies   → domain tag gates all access      │
│  Iceberg Row Filters   → row-level via Trino view + Ranger│
│  S3 Bucket Policy      → domain isolation enforced        │
│  Column Masking        → Ranger masking policies on PII   │
│  DataHub Contracts     → schema + SLA enforcement         │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG Schedule\ndomain cron]
    T2[📡 S3 Sensor\nnew partition landed]

    T1 --> J1[Airbyte / Debezium\nIngestion to Raw Iceberg]
    T2 --> J1

    J1 --> J2[dbt Core Run\nRaw → Curated Iceberg]
    J2 --> J3[Great Expectations\nData Quality Gate]
    J3 -->|pass| J4[dbt Core Run\nCurated → Product Iceberg]
    J3 -->|fail| A1[Airflow Alert → Slack → domain team]
    J4 --> J5[DataHub API\nPublish product metadata]
    J5 --> N1[Airflow SLA Callback\nproduct delivery confirmed]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|----------------|-------|
| Domain Storage | S3 + Apache Iceberg | ACID, time-travel, schema evolution per domain |
| Batch Ingestion | Airbyte (on EC2/ECS) | 300+ connectors; domain-managed instances |
| CDC Ingestion | Debezium + MSK (Kafka) | Low-latency DB replication |
| Stream Landing | Kafka → S3 (Iceberg sink) | Flink or Kafka Connect Iceberg sink |
| Transformation | dbt Core | Domain-owned dbt projects |
| Iceberg Catalog | AWS Glue Data Catalog | Hive-compatible; shared across Trino + EMR |
| Query Engine | Trino (on EMR Serverless) | Federated cross-domain SQL |
| Access Control | Apache Ranger | Tag-based + column masking + row filter |
| Data Catalog | DataHub (on ECS/Fargate) | Lineage, contracts, discovery |
| Data Quality | Great Expectations | Domain pipeline quality gates |
| Orchestration | Apache Airflow (MWAA) | Managed Airflow; domain DAG namespaces |
| Dashboards | Apache Superset | Connected to Trino |
| ML Consumption | SageMaker | Reads Iceberg curated/products directly |
| Infra Provisioning | Terraform + AWS CDK | Domain bootstrap modules |
| Observability | Prometheus + Grafana | Airflow + Trino + Iceberg metrics |

---

## Comparison vs 5.1 (AWS Managed)

| Dimension | 5.2 AWS OSS | 5.1 AWS Managed |
|-----------|------------|----------------|
| Governance tool | DataHub | AWS DataZone |
| Table format | Apache Iceberg | Redshift native + S3 Parquet |
| Query engine | Trino | Athena + Redshift Spectrum |
| Access control | Apache Ranger | Lake Formation |
| Catalog | Glue + DataHub | Glue + DataZone |
| Infra overhead | Medium — Ranger, DataHub, Trino | Low — fully managed |
| Vendor lock-in | Low (portable) | High (AWS-specific) |
| Cost model | EC2/EMR compute | DPU + RPU serverless |
| Time-travel | Iceberg native | Redshift limited |
| Data product API | DataHub REST API | DataZone subscription flow |
