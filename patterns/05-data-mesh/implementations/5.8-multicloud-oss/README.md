---
layout: default
title: "5.8 — Data Mesh · Multi-Cloud OSS Portable"
---

# 5.8 — Data Mesh · Multi-Cloud OSS Portable

**Stack:** Apache Iceberg · Project Nessie (Catalog) · DataHub · OpenMetadata · dbt Core · Apache Airflow · Trino
**Processing:** Domain-chosen (Batch / Streaming / Hybrid per domain)
**Buy vs Build:** Build (fully portable OSS — runs on any cloud or on-prem)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph GOV["Federated Governance Plane"]
        G1[DataHub\nUniversal Catalog + Lineage + Discovery]
        G2[OpenMetadata\nData Contract + Quality Registry]
        G3[Apache Ranger\nFine-Grained Access Control]
    end

    subgraph PLATFORM["Self-Serve Data Platform"]
        P1[Terraform + Helm\nCloud-Agnostic Domain Bootstrap]
        P2[Apache Airflow\nOrchestration Templates]
        P3[dbt Core\nTransformation Framework]
        P4[Soda Core\nData Quality Gates]
    end

    subgraph DOM_A["Domain: Sales (AWS)"]
        A1[S3 — Iceberg Tables\nnessie:sales.raw]
        A2[dbt Core + Trino\nTransform]
        A3[Iceberg Products\nnessie:sales.products]
        A4[Data Product\nsales.orders@v1]
    end

    subgraph DOM_B["Domain: Marketing (Azure)"]
        B1[ADLS — Iceberg Tables\nnessie:marketing.raw]
        B2[dbt Core + Trino\nTransform]
        B3[Iceberg Products\nnessie:marketing.products]
        B4[Data Product\nmarketing.campaigns@v1]
    end

    subgraph DOM_C["Domain: Finance (GCP)"]
        C1[GCS — Iceberg Tables\nnessie:finance.raw]
        C2[dbt Core + Trino\nTransform]
        C3[Iceberg Products\nnessie:finance.products]
        C4[Data Product\nfinance.revenue@v1]
    end

    subgraph CONSUME["Consumers"]
        E1[Trino\nFederated Cross-Cloud SQL]
        E2[Apache Superset\nDashboards]
        E3[MLflow + Jupyter\nML + Data Science]
        E4[Kafka / REST API\nData Product Subscription]
    end

    G1 -. govern .-> DOM_A & DOM_B & DOM_C
    G3 -. enforce access .-> DOM_A & DOM_B & DOM_C
    P1 -. bootstrap .-> DOM_A & DOM_B & DOM_C

    A1 --> A2 --> A3 --> A4
    B1 --> B2 --> B3 --> B4
    C1 --> C2 --> C3 --> C4

    A4 & B4 & C4 -. register contract .-> G2
    G2 -. publish to catalog .-> G1
    G1 -. discover + subscribe .-> CONSUME
    A3 & B3 & C3 --> E1 --> E2 & E3 & E4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        S1[(RDBMS — any cloud or on-prem)]
        S2[SaaS APIs]
        S3[Kafka / Event Streams]
    end

    subgraph DomainIngestion["Domain Ingestion (per domain)"]
        I1[Airbyte\nBatch Connectors]
        I2[Debezium\nCDC → Kafka]
        I3[Apache Flink\nStream → Iceberg]
    end

    subgraph DomainStorage["Domain Storage — Object Store + Iceberg + Nessie"]
        Z1[Iceberg Raw\nNessie branch: raw]
        Z2[Iceberg Curated\nNessie branch: curated]
        Z3[Iceberg Products\nNessie tag: v1]
    end

    subgraph Catalog["Nessie + DataHub + Ranger"]
        C1[Nessie Catalog\nGit-like Iceberg versioning]
        C2[DataHub Ingestion\nAuto Lineage + Schema]
        C3[Ranger Policies\nDomain RBAC]
        C4[OpenMetadata\nContracts + SLA]
    end

    subgraph Consume
        E1[Trino\nFederated SQL]
        E2[Superset\nDashboards]
        E3[MLflow + Jupyter\nML]
        E4[REST / Kafka API]
    end

    S1 --> I2 --> Z1
    S2 --> I1 --> Z1
    S3 --> I3 --> Z1
    Z1 -->|dbt Core| Z2 -->|dbt Core| Z3
    Z1 & Z2 & Z3 -. Nessie ref .-> C1
    C1 -. sync .-> C2 --> C3
    Z3 -. contract .-> C4
    Z3 -->|reads via Trino| E1 --> E2 & E3
    C4 --> E4
```

---

## Zone Design

```
Object Storage (per domain, any cloud):
  AWS:   s3://{domain}-iceberg/
  Azure: abfss://{domain}@{account}.dfs.core.windows.net/
  GCP:   gs://{domain}-iceberg/
  MinIO: http://minio/{domain}-iceberg/   ← on-prem / hybrid
│
├── raw/                      ← Iceberg table (metadata/ + data/)
│   └── {source}/{table}/
│       └── metadata/ + data/year=YYYY/month=MM/
│
├── curated/                  ← Iceberg table · MERGE capable
│   └── {entity}/
│
└── products/                 ← Iceberg table · schema-locked
    └── {product-name}/

Nessie Catalog (shared across domains):
  branches:
    main/{domain}/raw          → raw tables
    main/{domain}/curated      → curated tables
  tags:
    {domain}/{product}@v1      → immutable product snapshot

OpenMetadata contract per product:
  schema version · SLA (latency + freshness) · owner · data quality rules
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────┐
│  Apache Ranger + Cloud IAM — fully portable              │
│                                                           │
│  Principal             Access Level     Scope             │
│  ──────────────────    ────────────     ──────────────    │
│  domain-owner          Read + Write     Own Nessie branch │
│  domain-engineer       Read + Write     Own Nessie branch │
│  cross-domain-reader   Read only        Products tag only │
│  platform-admin        Admin            All domains       │
│  ml-consumer           Read only        Curated + Products│
│  bi-consumer           Read only        Products only     │
│                                                           │
│  Ranger Tag Policies   → Iceberg namespace tag           │
│  Cloud IAM Conditions  → bucket prefix per domain        │
│  Trino + Ranger        → row filter + column masking     │
│  Nessie Branch ACL     → write-lock product tags         │
│  OpenMetadata SLA      → quality gate blocks product push│
│  HashiCorp Vault       → cross-cloud secret management   │
│  BYOK (cloud KMS)      → domain-scoped encryption        │
└──────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow DAG Schedule\ndomain cron — any cloud]
    T2[📡 Kafka Topic Trigger\nnew partition event]

    T1 --> J1[Airbyte / Debezium\nIngestion to Iceberg Raw via Nessie]
    T2 --> J1

    J1 --> J2[Nessie Merge\nraw branch → curated branch]
    J2 --> J3[dbt Core Run\nRaw → Curated Iceberg]
    J3 --> J4[Soda Core Scan\nData Quality Gate]
    J4 -->|pass| J5[dbt Core Run\nCurated → Product Iceberg]
    J4 -->|fail| A1[Airflow Alert → Slack → domain team]
    J5 --> J6[Nessie Tag\ncreate product@v1 snapshot]
    J6 --> J7[DataHub + OpenMetadata API\nPublish contract + metadata]
    J7 --> N1[Kafka Topic\nproduct.published event → subscribers]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|----------------|-------|
| Domain Storage | Cloud Object Store + Apache Iceberg | S3/ADLS/GCS/MinIO — same API |
| Iceberg Catalog | Project Nessie | Git-like branching for Iceberg tables; cross-cloud |
| Batch Ingestion | Airbyte (on K8s) | Cloud-agnostic; 300+ connectors |
| CDC Ingestion | Debezium + Apache Kafka | Portable CDC; Kafka Connect Iceberg sink |
| Stream Landing | Apache Flink + Iceberg sink | Flink on K8s; real-time Iceberg writes |
| Transformation | dbt Core | Domain dbt projects; Trino adapter |
| Query Engine | Trino | Federated SQL over Iceberg on any object store |
| Access Control | Apache Ranger | Tag-based; column masking; row filter |
| Universal Catalog | DataHub | Auto lineage from dbt + Airflow + Trino |
| Contract Registry | OpenMetadata | Schema contracts + SLA + quality rules |
| Data Quality | Soda Core | Pipeline-embedded quality checks |
| Secret Management | HashiCorp Vault | Cross-cloud credentials; no cloud lock-in |
| Orchestration | Apache Airflow | Cloud Composer / MWAA / self-hosted on K8s |
| Dashboards | Apache Superset | Connected to Trino |
| ML Platform | MLflow + Jupyter | Reads Iceberg tables via PyIceberg |
| Infra Provisioning | Terraform + Helm | Cloud-agnostic Kubernetes modules |
| Observability | Prometheus + Grafana | Trino + Flink + Airflow + Nessie metrics |

---

## Comparison vs 5.7 (Multi-Cloud Managed)

| Dimension | 5.8 Multi-Cloud OSS | 5.7 Multi-Cloud Managed |
|-----------|---------------------|------------------------|
| Central catalog | Nessie + DataHub | Databricks Unity Catalog |
| Business catalog | OpenMetadata | Collibra / Alation |
| Table format | Apache Iceberg | Delta Lake |
| Query engine | Trino | Databricks SQL Warehouse |
| Orchestration | Airflow + dbt Core | Databricks Workflows + dbt Cloud |
| Ingestion | Airbyte + Debezium | Fivetran |
| Stream ingest | Flink + Iceberg sink | Databricks Auto Loader |
| Infra overhead | High — full K8s stack | Low — managed SaaS |
| Vendor lock-in | None — fully portable | Medium (Databricks) |
| Cost model | K8s compute | DBU serverless |
| Table versioning | Nessie git branches | Delta log |
| Product API | Kafka events + REST | Webhook + Unity |
