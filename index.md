---
layout: default
title: Enterprise Data Architecture — Master Index
---

# Enterprise Data Architecture — Master Index

> **Scope:** Enterprise-only. 10 core architectural patterns × 4 variation axes.
> **How to use:** Pick a pattern → choose your axes → get a concrete architecture document.

---

## The 10 Core Patterns

| # | Pattern | The Core Idea | Primary Driver |
|---|---------|---------------|----------------|
| [1](#1--enterprise-data-warehouse-edw) | Enterprise Data Warehouse (EDW) | Centralized, governed, structured, history-preserving analytical store | Reporting & BI at scale |
| [2](#2--data-lake) | Data Lake | Raw, flexible, schema-on-read repository for all data types | Exploration & cost-efficient storage |
| [3](#3--data-lakehouse) | Data Lakehouse | Lake economics + warehouse reliability via open table formats | Unified batch + query at scale |
| [4](#4--streaming--event-driven-data-platform) | Streaming / Event-Driven Data Platform | All data as ordered events; real-time first | Operational intelligence & real-time |
| [5](#5--data-mesh) | Data Mesh | Decentralized, domain-owned data products with federated governance | Scale of teams & domains |
| [6](#6--data-fabric) | Data Fabric | Automated, metadata-driven, virtual integration across hybrid/multi-cloud | Heterogeneous estate unification |
| [7](#7--operational-analytics-platform) | Operational Analytics Platform | Analytics on live operational data; near-zero latency (ODS / HTAP) | Operational decisions in-the-moment |
| [8](#8--aiml-data-platform) | AI / ML Data Platform | Feature stores, model lifecycle, vector stores, LLM pipelines | ML/AI production at enterprise scale |
| [9](#9--governed--compliance-first-platform) | Governed / Compliance-First Platform | Privacy, residency, masking, audit as the primary architectural constraint | Regulated industries (GDPR/HIPAA/PCI) |
| [10](#10--self-serve-analytics-engineering-platform) | Self-Serve Analytics Engineering Platform | Domain users build with guardrails; semantic layer + metrics + headless BI | Democratized analytics, reduced IT bottleneck |

---

## The 4 Variation Axes

Every pattern is documented across these axes. Each combination = one concrete architecture document.

```
AXIS 1 — Modeling Approach       AXIS 2 — Cloud & Deployment
  ├── Kimball (Star/Snowflake)      ├── AWS-native
  ├── Inmon (3NF + Data Marts)      ├── Azure-native
  ├── Data Vault 2.0                ├── GCP-native
  └── Activity Schema / Wide Table  ├── Multi-Cloud (portable/open)
      (modern analytics engineering) └── Hybrid (on-prem + cloud)

AXIS 3 — Tool Stack               AXIS 4 — Processing Paradigm
  ├── Fully Managed SaaS (Buy)       ├── Batch-first
  ├── Cloud-Native OSS               ├── Streaming-first
  └── Self-Hosted OSS (Full Build)   └── Hybrid (Lambda / Kappa embedded)
```

> **Note:** Not every axis combination applies to every pattern.
> Each pattern section below lists which axis values are in scope.

---

## Cross-Cutting Concerns
> These are not separate architectures. They are layers applied to every pattern.

| Concern | Applies To | Covered In |
|---------|-----------|------------|
| Ingestion & Integration (ETL / ELT / CDC / API / Streaming) | All patterns | Each pattern's ingestion section |
| Data Modeling (Kimball / Inmon / Data Vault / Activity Schema) | Patterns 1–3, 5 | Axis 1 of each pattern |
| Orchestration (Airflow / Prefect / Dagster / dbt Cloud) | All patterns | Each pattern's orchestration section |
| Data Quality & Observability (Great Expectations / Monte Carlo / Soda) | All patterns | Each pattern's quality section |
| Governance & Catalog (Collibra / Alation / DataHub / Unity Catalog) | All patterns | Each pattern's governance section |
| Security (RBAC / ABAC / Column & Row Security / Encryption / KMS) | All patterns | Each pattern's security section |
| DataOps / CI-CD for Data | All patterns | Each pattern's DataOps section |
| FinOps / Cost Management | All patterns | Each pattern's platform section |

---

## Document Map

Each cell below represents one architecture document to be created.
Format: `[Cloud] [Tool Stack] — [Notes]`

---

### 1 — Enterprise Data Warehouse (EDW)

**When to use:** Structured enterprise reporting, regulatory history, single source of truth for finance/operations/HR.
**Core components:** Ingestion layer → Staging → EDW → Data Marts → Semantic Layer → BI.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Kimball** | Star/Snowflake schema; fact + dimension tables; optimized for business user queries |
| **Inmon** | 3NF normalized central warehouse; subject-area data marts fed downstream |
| **Data Vault 2.0** | Hubs / Links / Satellites; agile, auditable, handles schema change well |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 1.1 | AWS | Fully Managed | Buy | Redshift + AWS Glue + dbt Cloud + QuickSight / Tableau |
| 1.2 | AWS | OSS on Cloud | Build | Redshift + Airbyte + dbt Core + Airflow + Superset |
| 1.3 | Azure | Fully Managed | Buy | Synapse Analytics + ADF + dbt Cloud + Power BI |
| 1.4 | Azure | OSS on Cloud | Build | Synapse + Airbyte + dbt Core + Airflow + Superset |
| 1.5 | GCP | Fully Managed | Buy | BigQuery + Datastream + dbt Cloud + Looker |
| 1.6 | GCP | OSS on Cloud | Build | BigQuery + Airbyte + dbt Core + Airflow + Superset |
| 1.7 | Multi-Cloud | Fully Managed | Buy | Snowflake + Fivetran + dbt Cloud + Tableau |
| 1.8 | Multi-Cloud | OSS on Cloud | Build | Snowflake + Airbyte + dbt Core + Airflow + Metabase |
| 1.9 | Hybrid | Fully Managed | Buy | Teradata Vantage / IBM Db2 Warehouse + Informatica |
| 1.10 | Hybrid | OSS on Cloud | Build | PostgreSQL + dbt Core + Airbyte + Airflow |

#### Axis 4 — Processing Paradigm
- EDW is inherently **Batch-first**
- Near-real-time variant: add CDC (Debezium) → Kafka → micro-batch load into EDW

---

### 2 — Data Lake

**When to use:** Need to store all data cheaply before knowing how it will be used; data science exploration; multiple raw data types (logs, files, events, media metadata).
**Core components:** Ingestion → Object Storage (Landing Zone) → Cataloging → Processing → Consumption.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Schema-on-Read** | Raw files landed as-is; schema applied at query time |
| **Medallion (Bronze/Silver)** | Lightweight Bronze (raw) → Silver (cleaned) without full warehouse Gold layer |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| [2.1](patterns/02-data-lake/implementations/2.1-aws-managed/) | AWS | Fully Managed | Buy | S3 + AWS Glue + Lake Formation + Athena + AWS Glue Catalog |
| [2.2](patterns/02-data-lake/implementations/2.2-aws-oss/) | AWS | OSS on Cloud | Build | S3 + Spark on EMR + Hive Metastore + Airflow + Presto/Trino |
| [2.3](patterns/02-data-lake/implementations/2.3-azure-managed/) | Azure | Fully Managed | Buy | ADLS Gen2 + Azure Data Factory + Purview + Synapse Serverless |
| [2.4](patterns/02-data-lake/implementations/2.4-azure-oss/) | Azure | OSS on Cloud | Build | ADLS + Spark on HDInsight/AKS + Apache Atlas + Trino |
| [2.5](patterns/02-data-lake/implementations/2.5-gcp-managed/) | GCP | Fully Managed | Buy | GCS + Dataflow + Dataplex + BigQuery (external tables) |
| [2.6](patterns/02-data-lake/implementations/2.6-gcp-oss/) | GCP | OSS on Cloud | Build | GCS + Spark on Dataproc + Hive Metastore + Trino |
| [2.7](patterns/02-data-lake/implementations/2.7-multicloud-oss/) | Multi-Cloud | OSS Portable | Build | S3/ADLS/GCS + Trino + Apache Atlas + Airflow |
| [2.8](patterns/02-data-lake/implementations/2.8-hybrid-selfhosted/) | Hybrid | OSS Self-Hosted | Full Build | HDFS / MinIO + Hive + Spark + Ranger + Airflow |

#### Axis 4 — Processing Paradigm
- **Batch-first** (standard)
- **Streaming ingest variant:** Kafka → S3/ADLS/GCS landing in near-real-time, batch processing unchanged

---

### 3 — Data Lakehouse

**When to use:** Want lake storage costs + ACID reliability + SQL performance; replacing separate lake + warehouse; unified batch and streaming on the same store.
**Core components:** Ingestion → Object Storage + Open Table Format → Processing → Medallion Layers → Serving.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Medallion (Bronze/Silver/Gold)** | Standard layered approach; most common in lakehouse |
| **Data Vault 2.0 on Lakehouse** | Raw Vault in Silver, Business Vault in Gold; suits complex enterprises |
| **Wide Table / Activity Schema** | Denormalized Gold layer; suits analytics engineering teams using dbt |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| [3.1](patterns/03-data-lakehouse/implementations/3.1-aws-managed/) | AWS | Fully Managed | Buy | S3 + AWS Glue + Apache Iceberg + Redshift Spectrum + dbt Cloud |
| [3.2](patterns/03-data-lakehouse/implementations/3.2-aws-oss/) | AWS | OSS on Cloud | Build | S3 + Iceberg + Spark on EMR + dbt Core + Airflow + Trino |
| [3.3](patterns/03-data-lakehouse/implementations/3.3-azure-managed/) | Azure | Fully Managed | Buy | ADLS + Azure Databricks + Delta Lake + dbt Cloud + Synapse |
| [3.4](patterns/03-data-lakehouse/implementations/3.4-azure-oss/) | Azure | OSS on Cloud | Build | ADLS + Delta Lake + Spark/Databricks OSS + dbt Core + Airflow |
| [3.5](patterns/03-data-lakehouse/implementations/3.5-gcp-managed/) | GCP | Fully Managed | Buy | GCS + BigLake + Iceberg + dbt Cloud + BigQuery |
| [3.6](patterns/03-data-lakehouse/implementations/3.6-gcp-oss/) | GCP | OSS on Cloud | Build | GCS + Iceberg + Spark on Dataproc + dbt Core + Airflow |
| [3.7](patterns/03-data-lakehouse/implementations/3.7-multicloud-managed/) | Multi-Cloud | Fully Managed | Buy | Databricks (multi-cloud) + Delta Lake + dbt Cloud + Tableau |
| [3.8](patterns/03-data-lakehouse/implementations/3.8-multicloud-oss/) | Multi-Cloud | OSS Portable | Build | S3/ADLS/GCS + Apache Iceberg + Spark + dbt Core + Airflow + Trino |
| [3.9](patterns/03-data-lakehouse/implementations/3.9-hybrid-selfhosted/) | Hybrid | OSS Self-Hosted | Full Build | MinIO + Apache Iceberg + Spark on K8s + dbt Core + Airflow |

#### Axis 4 — Processing Paradigm
- **Batch-first** (standard Medallion)
- **Streaming-first (Streaming Lakehouse):** Kafka/Kinesis → Flink → Iceberg/Delta direct write → same Gold layer
- **Hybrid:** Batch for history, streaming for incremental updates (Merge-on-Read / UPSERT)

---

### 4 — Streaming / Event-Driven Data Platform

**When to use:** Fraud detection, real-time personalization, IoT telemetry, operational alerting, event sourcing, sub-second dashboards.
**Core components:** Event Sources → Event Broker → Stream Processor → Serving Store → Consumers.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Kappa Architecture** | Single streaming path; batch views rebuilt by replaying the stream |
| **Lambda Architecture** | Parallel batch + speed layers; batch for accuracy, stream for freshness |
| **Event Sourcing + CQRS** | Append-only event log as system of record; separate read models |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 4.1 | AWS | Fully Managed | Buy | Kinesis + MSK (Kafka) + Flink on KDA + DynamoDB / Redshift |
| 4.2 | AWS | OSS on Cloud | Build | Kafka on MSK/EC2 + Apache Flink on EMR + Cassandra / Iceberg |
| 4.3 | Azure | Fully Managed | Buy | Event Hubs + Azure Stream Analytics + Cosmos DB + Synapse |
| 4.4 | Azure | OSS on Cloud | Build | Event Hubs (Kafka API) + Apache Flink on AKS + Delta Lake |
| 4.5 | GCP | Fully Managed | Buy | Pub/Sub + Dataflow (Beam) + BigQuery Streaming + Bigtable |
| 4.6 | GCP | OSS on Cloud | Build | Pub/Sub + Apache Flink on GKE + Iceberg on GCS |
| 4.7 | Multi-Cloud | Fully Managed | Buy | Confluent Cloud (Kafka) + Flink Cloud + Snowflake Streaming |
| 4.8 | Multi-Cloud | OSS Portable | Build | Apache Kafka + Apache Flink + Apache Iceberg + ksqlDB |
| 4.9 | Hybrid | OSS Self-Hosted | Full Build | Kafka on K8s + Flink on K8s + Cassandra + MinIO |

#### Axis 4 — Processing Paradigm
- **Streaming-first** (core of this pattern)
- **Micro-batch variant:** Spark Structured Streaming instead of Flink where sub-second latency not required

---

### 5 — Data Mesh

**When to use:** Large enterprise with 10+ data domains; central data team is a bottleneck; need domain autonomy with enterprise governance; data products need SLAs.
**Core components:** Domain Data Products + Self-Serve Platform + Federated Governance + Interoperability Standards.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Domain-Aligned Lakehouse** | Each domain owns a Lakehouse; Gold layer = data product output |
| **Domain-Aligned Warehouse** | Each domain owns a schema/mart inside a shared warehouse |
| **Polyglot Domain Storage** | Each domain picks its own storage; platform enforces contracts at the interface |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 5.1 | AWS | Fully Managed | Buy | S3 + Lake Formation (domain isolation) + Glue Catalog + Redshift (per domain) + DataZone |
| 5.2 | AWS | OSS on Cloud | Build | S3 + Iceberg + Trino + DataHub + dbt Core + Airflow |
| 5.3 | Azure | Fully Managed | Buy | ADLS (per domain) + Purview + Synapse (per domain) + dbt Cloud |
| 5.4 | Azure | OSS on Cloud | Build | ADLS + Delta Lake + DataHub + dbt Core + Airflow |
| 5.5 | GCP | Fully Managed | Buy | GCS + Dataplex (domain zoning) + BigQuery (per domain) + dbt Cloud |
| 5.6 | GCP | OSS on Cloud | Build | GCS + Iceberg + DataHub + dbt Core + Airflow |
| 5.7 | Multi-Cloud | Fully Managed | Buy | Databricks Unity Catalog + dbt Cloud + Alation / Collibra |
| 5.8 | Multi-Cloud | OSS Portable | Build | Iceberg + Nessie Catalog + DataHub + dbt Core + Airflow + OpenMetadata |

#### Axis 4 — Processing Paradigm
- Each domain independently chooses Batch / Streaming / Hybrid
- Platform enforces data product output SLAs regardless of processing type

---

### 6 — Data Fabric

**When to use:** Heterogeneous estate (on-prem + multi-cloud + legacy); need unified access without physically moving all data; active metadata to automate governance and discovery.
**Core components:** Metadata Layer → Virtual Integration / Query Federation → Active Governance → Unified Access API.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Virtual / Federation-First** | No data movement; all access via federated query |
| **Hybrid (Selective Replication)** | Hot data replicated for performance; cold data virtualized |
| **Semantic / Knowledge Fabric** | Ontology + knowledge graph driving automated discovery and lineage |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 6.1 | AWS | Fully Managed | Buy | AWS Glue + Lake Formation + Athena Federation + Macie + DataZone |
| 6.2 | Azure | Fully Managed | Buy | Azure Purview + Synapse Link + Azure API Management + Defender |
| 6.3 | GCP | Fully Managed | Buy | Dataplex + BigQuery Omni + Data Catalog + Chronicle |
| 6.4 | Multi-Cloud | Fully Managed | Buy | Informatica IDMC + Collibra + Talend + Starburst |
| 6.5 | Multi-Cloud | OSS Portable | Build | Apache Atlas + Trino + OpenMetadata + Apache Ranger |
| 6.6 | Hybrid | Fully Managed | Buy | Informatica IDMC + Collibra + IBM Cloud Pak for Data |
| 6.7 | Hybrid | OSS Self-Hosted | Full Build | Apache Atlas + Trino + Apache Ranger + DataHub |

#### Axis 4 — Processing Paradigm
- **On-Demand / Federated** (query time, no pre-processing)
- **Batch replication** for performance-critical paths

---

### 7 — Operational Analytics Platform

**When to use:** Need analytics on live transactional data; operational dashboards; next-best-action for front-line staff; customer-facing real-time metrics.
**Core components:** OLTP Source → CDC / Direct Query → ODS / HTAP Store → Operational BI / API.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **ODS (Operational Data Store)** | Integrated near-real-time copy of operational data; light transformation |
| **HTAP (Hybrid Transactional/Analytical)** | Same store handles both OLTP and OLAP; no separate analytical copy |
| **Operational Lakehouse** | CDC into Lakehouse with sub-minute latency; serves both ops and analytics |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 7.1 | AWS | Fully Managed | Buy | Aurora + DMS (CDC) + Redshift (ODS) + QuickSight |
| 7.2 | AWS | OSS on Cloud | Build | PostgreSQL + Debezium + Kafka + Flink + Iceberg + Superset |
| 7.3 | Azure | Fully Managed | Buy | Azure SQL + Synapse Link + Cosmos DB Analytical Store + Power BI |
| 7.4 | Azure | OSS on Cloud | Build | PostgreSQL + Debezium + Event Hubs + Delta Lake + Superset |
| 7.5 | GCP | Fully Managed | Buy | Cloud Spanner + Datastream (CDC) + BigQuery + Looker |
| 7.6 | GCP | OSS on Cloud | Build | PostgreSQL + Debezium + Pub/Sub + Iceberg + Superset |
| 7.7 | Multi-Cloud | Fully Managed | Buy | CockroachDB / SingleStore + Fivetran + Snowflake + Tableau |
| 7.8 | Hybrid | OSS Self-Hosted | Full Build | PostgreSQL + Debezium + Kafka + Apache Pinot / Druid + Superset |

#### Axis 4 — Processing Paradigm
- **Streaming / Near-Real-Time** (core requirement)
- **On-Demand (HTAP)** — queries run directly against operational store

---

### 8 — AI / ML Data Platform

**When to use:** Enterprise ML in production; need feature reuse across models; managing model versions; LLM-powered applications with RAG; vector search.
**Core components:** Feature Store (Online + Offline) → Model Registry → Serving Layer → Vector Store + Embedding Pipeline → LLM Orchestration.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Feature Store–Centric** | Central registry of features; shared across model training and inference |
| **RAG / LLM Pipeline** | Embedding + Vector DB + retrieval layer feeding LLM responses |
| **Unified AI + Analytics** | Single platform for BI, ML training, and inference (Lakehouse + ML) |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 8.1 | AWS | Fully Managed | Buy | SageMaker Feature Store + SageMaker Pipelines + OpenSearch (vector) + Bedrock |
| 8.2 | AWS | OSS on Cloud | Build | Feast + MLflow + Airflow + Qdrant/Weaviate + LangChain |
| 8.3 | Azure | Fully Managed | Buy | Azure ML Feature Store + Azure ML + Azure AI Search + Azure OpenAI |
| 8.4 | Azure | OSS on Cloud | Build | Feast + MLflow + Airflow + Qdrant + LangChain on AKS |
| 8.5 | GCP | Fully Managed | Buy | Vertex AI Feature Store + Vertex Pipelines + Vertex Vector Search + Gemini |
| 8.6 | GCP | OSS on Cloud | Build | Feast + MLflow + Kubeflow + Weaviate on GKE + LangChain |
| 8.7 | Multi-Cloud | Fully Managed | Buy | Databricks (Feature Store + MLflow + Vector Search) + dbt Cloud |
| 8.8 | Multi-Cloud | OSS Portable | Build | Feast + MLflow + Airflow + Qdrant + LangChain + dbt Core |
| 8.9 | Hybrid | OSS Self-Hosted | Full Build | Feast + MLflow + Kubeflow + Qdrant on K8s + Ollama (local LLM) |

#### Axis 4 — Processing Paradigm
- **Batch** for offline feature computation and model training
- **Real-Time** for online feature serving and inference
- **Hybrid** (both required in production ML)

---

### 9 — Governed / Compliance-First Platform

**When to use:** Financial services, healthcare, pharma, government — where compliance (GDPR, HIPAA, PCI DSS, SOX) drives architecture decisions before performance or cost.
**Core components:** Data Classification → Access Control (RBAC/ABAC) → Masking/Tokenization → Lineage + Audit → Residency Enforcement → Consent Management.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Centralized Governed Warehouse** | Single governed store with strict access tiers |
| **Governed Lakehouse** | Open format lake with policy enforcement at catalog layer |
| **Privacy Vault + Clean Room** | Sensitive data separated into vault; analytical access via clean room |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 9.1 | AWS | Fully Managed | Buy | Redshift + Lake Formation (RBAC/ABAC) + Macie + CloudTrail + AWS KMS |
| 9.2 | AWS | OSS on Cloud | Build | S3 + Iceberg + Apache Ranger + OpenMetadata + Debezium (audit) |
| 9.3 | Azure | Fully Managed | Buy | Synapse + Purview + Azure Policy + Key Vault + Microsoft Defender |
| 9.4 | Azure | OSS on Cloud | Build | ADLS + Delta Lake + Apache Ranger + Collibra + Azure Key Vault |
| 9.5 | GCP | Fully Managed | Buy | BigQuery + Data Catalog + DLP API + Cloud KMS + Dataplex |
| 9.6 | GCP | OSS on Cloud | Build | GCS + Iceberg + Apache Ranger + OpenMetadata + Cloud KMS |
| 9.7 | Multi-Cloud | Fully Managed | Buy | Snowflake (RBAC + Dynamic Masking + Data Clean Room) + Collibra + Fivetran |
| 9.8 | Multi-Cloud | OSS Portable | Build | Iceberg + Apache Ranger + OpenMetadata + HashiCorp Vault + Trino |
| 9.9 | Hybrid | Fully Managed | Buy | Informatica IDMC + Collibra + IBM Guardium + Protegrity |
| 9.10 | Hybrid | OSS Self-Hosted | Full Build | Apache Atlas + Ranger + HashiCorp Vault + OpenMetadata + Audit Sinks |

#### Axis 4 — Processing Paradigm
- **Batch** (standard compliance reporting)
- **Real-Time** for consent enforcement, right-to-be-forgotten propagation, audit streaming

---

### 10 — Self-Serve Analytics Engineering Platform

**When to use:** Data team wants to enable business analysts / domain teams to build trusted metrics and reports with guardrails; eliminate dependency on central BI team; semantic layer as the control point.
**Core components:** Transformation Layer (dbt) → Semantic / Metric Layer → BI / Exploration Tools → Data Catalog (discovery) → Governance Guardrails.

#### Axis 1 — Modeling Variants

| Variant | Description |
|---------|-------------|
| **Kimball Gold Layer + Semantic Layer** | dbt builds dimensional Gold; semantic layer exposes metrics |
| **Wide Table + Metrics Store** | Denormalized dbt models; headless BI serves consistent metrics via API |
| **Data Product–Oriented** | Each team owns dbt domain; semantic layer federates across domains |

#### Axis 2 × Axis 3 — Platform Documents

| # | Cloud | Tool Stack | Managed/OSS | Key Tools |
|---|-------|-----------|-------------|-----------|
| 10.1 | AWS | Fully Managed | Buy | Redshift + dbt Cloud + AtScale / Cube Cloud + Tableau / Looker |
| 10.2 | AWS | OSS on Cloud | Build | Redshift/Trino + dbt Core + Cube.js + Superset + Airflow |
| 10.3 | Azure | Fully Managed | Buy | Synapse + dbt Cloud + Power BI (semantic model) + AtScale |
| 10.4 | Azure | OSS on Cloud | Build | Synapse/DuckDB + dbt Core + Cube.js + Superset + Airflow |
| 10.5 | GCP | Fully Managed | Buy | BigQuery + dbt Cloud + Looker (LookML semantic layer) |
| 10.6 | GCP | OSS on Cloud | Build | BigQuery + dbt Core + Cube.js + Superset + Airflow |
| 10.7 | Multi-Cloud | Fully Managed | Buy | Snowflake + dbt Cloud + Tableau + AtScale / dbt Semantic Layer |
| 10.8 | Multi-Cloud | OSS Portable | Build | DuckDB / Trino + dbt Core + Cube.js + Superset + Airflow |
| 10.9 | Hybrid | OSS Self-Hosted | Full Build | PostgreSQL + dbt Core + Cube.js + Superset + Airflow on K8s |

#### Axis 4 — Processing Paradigm
- **Batch-first** (dbt runs on schedule)
- **On-Demand** (semantic layer serves live queries to BI tools)

---

## Domain Applications
> Built on top of the 10 core patterns. Not separate architectures — they are use-case implementations.

| Domain Application | Built On Top Of | Notes |
|--------------------|-----------------|-------|
| Customer Data Platform (CDP) | Pattern 1 or 3 | Unified customer identity + behavioral data |
| Master Data Management (MDM) | Pattern 1 | Golden record management for critical entities |
| Reverse ETL | Patterns 1, 3, 10 | Syncing warehouse data back to CRM/ERP/marketing tools |
| Data Clean Room | Pattern 9 | Multi-party analytics without exposing raw data |
| IoT / Telemetry Platform | Pattern 4 | High-volume time-series events at edge scale |
| Product Analytics Platform | Patterns 3, 10 | Funnel, cohort, retention analysis at product scale |
| Financial Regulatory Reporting | Pattern 9 | SOX, Basel III, IFRS 9 compliance data pipelines |

---

## Total Document Count

| Layer | Count |
|-------|-------|
| Core pattern overviews | 10 |
| Modeling variant docs (per pattern) | ~25 |
| Platform + tool stack docs (cells above) | ~80 |
| Domain application docs | 7 |
| Cross-cutting concern guides | 8 |
| **Total** | **~130** |
