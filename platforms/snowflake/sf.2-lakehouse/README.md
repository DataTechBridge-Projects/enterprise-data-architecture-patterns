---
layout: default
title: "sf.2 — Snowflake · Lakehouse with Iceberg"
---

# sf.2 — Snowflake · Lakehouse with Iceberg

**Stack:** Airbyte / Fivetran · Snowflake · Apache Iceberg External Tables · dbt Core · Apache Airflow
**Processing:** Batch + micro-batch · Schema-on-Read for lake · Schema-on-Write for warehouse
**Buy vs Build:** Hybrid (managed Snowflake + self-hosted OSS orchestration)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[OLTP Databases\nPostgres · MySQL · Oracle]
        S2[SaaS Applications\nSalesforce · Stripe · HubSpot]
        S3[Object Storage\nS3 · GCS · ADLS parquet drops]
        S4[Streaming Events\nKafka · Kinesis topics]
    end

    subgraph INGEST["Ingestion Layer"]
        I1[Airbyte\nOSS connectors · self-hosted]
        I2[Fivetran\nManaged SaaS connectors]
        I3[Kafka Connect\nStream-to-object-storage sink]
    end

    subgraph LAKE["Object Storage Lake — S3 / GCS / ADLS"]
        L1[Landing Zone\nraw · as-received · short TTL]
        L2[Raw Iceberg Tables\nParquet · partitioned · versioned]
        L3[Curated Iceberg Tables\ncleaned · conformed · typed]
    end

    subgraph CATALOG["Open Catalog — Apache Polaris / AWS Glue"]
        C1[Iceberg REST Catalog\nnamespace · table registry]
        C2[Schema Evolution\ncolumn add · type promote]
        C3[Snapshot Management\ntime-travel · branching]
    end

    subgraph SNOWFLAKE["Snowflake — Query Engine + EDW"]
        SF1[Iceberg External Tables\ndirect query of lake Parquet]
        SF2[Native Snowflake Tables\nconformed dimensional layer]
        SF3[Data Marts\nfinance · sales · product]
        SF4[Dynamic Tables\nauto-refresh materializations]
    end

    subgraph TRANSFORM["Transformation — dbt Core + Airflow"]
        T1[dbt Staging Models\nread Iceberg external tables]
        T2[dbt Core Models\nintegration + conformation]
        T3[dbt Mart Models\nstar schema output]
        T4[Airflow DAGs\norchestrate dbt + compaction]
    end

    subgraph CONSUME["Consumption"]
        F1[Tableau / Power BI\nEnterprise BI]
        F2[Snowflake Worksheets\nAd-hoc SQL]
        F3[Spark / Trino\nExternal engines on Iceberg]
        F4[Notebooks\nJupyter · Hex]
    end

    SRC --> INGEST
    INGEST --> L1
    L1 --> L2
    L2 -. register .-> C1
    C1 -. govern .-> SF1
    L2 --> T1
    T1 --> T2 --> L3
    L3 -. register .-> C1
    L3 --> SF1
    SF1 --> T2 --> SF2
    SF2 --> T3 --> SF3
    T4 --> T1 & T2 & T3
    SF3 --> F1
    SF2 --> F2
    L2 --> F3
    SF2 --> F4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(OLTP DBs)]
        A2[SaaS APIs]
        A3[Object Storage\nParquet drops]
        A4[Kafka Topics]
    end

    subgraph Ingestion
        B1[Airbyte\nOSS connectors]
        B2[Fivetran\nManaged SaaS]
        B3[Kafka Connect\nS3 Sink]
    end

    subgraph Lake["Object Storage — Iceberg Tables"]
        C1[Landing\nraw files]
        C2[Raw Iceberg\nPartitioned Parquet]
        C3[Curated Iceberg\nCleaned · Typed]
    end

    subgraph Catalog["Iceberg Catalog\nPolaris / Glue"]
        D1[Table Registry]
        D2[Schema Versions]
    end

    subgraph Snowflake["Snowflake"]
        E1[External Tables\non Iceberg]
        E2[Native Tables\ndimensional layer]
        E3[Data Marts]
    end

    subgraph Orchestration["Airflow + dbt Core"]
        F1[dbt Staging\nread external tables]
        F2[dbt Core\nintegrate + conform]
        F3[dbt Mart\nstar schema]
    end

    subgraph Consume
        G1[Tableau / Power BI]
        G2[Ad-hoc SQL]
        G3[Spark / Trino\non Iceberg directly]
    end

    A1 --> B1 --> C1
    A2 --> B2 --> C1
    A3 --> C1
    A4 --> B3 --> C1

    C1 -->|Airflow compaction| C2
    C2 -->|register| D1 & D2
    C2 -->|dbt staging| F1 --> C3
    C3 -->|register| D1
    D1 -->|external table| E1
    E1 --> F2 --> E2
    E2 --> F3 --> E3

    E3 --> G1
    E2 --> G2
    C2 --> G3
```

---

## Component Breakdown

| Layer | Tool | Role |
|-------|------|------|
| Ingestion — OSS | Airbyte (self-hosted) | 300+ OSS connectors; custom connector SDK; Kubernetes-deployable |
| Ingestion — Managed | Fivetran | High-reliability SaaS connectors for critical business systems |
| Stream Ingestion | Kafka Connect S3/GCS Sink | Micro-batch event landing into object storage as Parquet |
| Object Storage | S3 / GCS / ADLS | Open lake storage; vendor-neutral; Iceberg table format on top |
| Open Table Format | Apache Iceberg | ACID transactions, schema evolution, time-travel on object storage |
| Iceberg Catalog | Apache Polaris or AWS Glue | REST Catalog API; table registry for multi-engine access |
| Query Engine | Snowflake External Tables | Query Iceberg Parquet directly without data movement or duplication |
| Native Warehouse | Snowflake Native Tables | Conformed dimensional and mart layers for BI performance |
| Transformation | dbt Core | Open-source dbt; models target both Iceberg and Snowflake native |
| Orchestration | Apache Airflow | DAGs for ingestion scheduling, dbt runs, Iceberg compaction jobs |
| BI | Tableau / Power BI | Connect to Snowflake native mart layer for governed BI |
| Alternative Engines | Apache Spark / Trino | Direct Iceberg table access for ML feature engineering or heavy transforms |

---

## Key Design Decisions

- **Iceberg as the canonical storage format.** Writing raw and curated data as Iceberg tables in object storage means any compute engine (Snowflake, Spark, Trino, Flink) can read the same data without copying or format conversion, avoiding vendor lock-in at the storage layer.
- **Two-tier query strategy.** Snowflake External Tables give analysts immediate SQL access to all Iceberg data; Native Snowflake Tables hold only the conformed dimensional layer where query performance and cost predictability matter most for BI workloads.
- **Airflow as the orchestration backbone.** Unlike the managed sf.1 stack, this pattern uses Airflow to coordinate cross-system dependencies — ingestion completion, Iceberg compaction, dbt runs, and data quality gates — giving full control over DAG logic and retry behaviour.
- **Schema evolution via Iceberg.** Airbyte and Kafka Connect land schema changes as Iceberg column additions without breaking downstream consumers, eliminating the brittle schema migration scripts common in traditional EDW pipelines.
- **Airbyte for breadth, Fivetran for reliability.** Airbyte's OSS connector library covers long-tail sources and custom APIs; Fivetran remains for tier-1 business systems (ERP, CRM) where connector reliability SLAs are non-negotiable.

---

## When to Choose This Implementation

- The organisation needs to support multiple query engines on the same data — Snowflake for BI, Spark for ML, Trino for federated analytics — and cannot afford to maintain separate copies per engine.
- Avoiding cloud vendor lock-in at the storage layer is a strategic requirement; Iceberg tables on object storage ensure portability across clouds and warehouse vendors.
- The platform team has engineering capacity to operate Airflow and Airbyte on Kubernetes and prefers OSS tooling with full customisation over fully managed SaaS.
- Data volumes or variety (semi-structured JSON, nested arrays, rapidly evolving schemas) exceed what a traditional Snowflake-only warehouse handles efficiently without high VARIANT storage costs.

---

## Trade-offs

| Strength | Limitation |
|----------|------------|
| True multi-engine access — Snowflake, Spark, Trino, and Flink all read the same Iceberg tables without data duplication | Higher operational complexity — Airflow, Airbyte, and Iceberg catalog all require platform engineering to operate and maintain |
| Vendor-neutral object storage layer prevents warehouse vendor lock-in | Iceberg External Tables in Snowflake have higher query latency than native tables; performance-critical BI still needs materialisation into native tables |
| Iceberg schema evolution and time-travel reduce data pipeline fragility significantly | Iceberg compaction and snapshot expiry must be scheduled explicitly; poorly tuned compaction leads to small-file performance degradation |
| OSS ingestion via Airbyte significantly reduces connector licensing costs at scale | Airbyte connector reliability and maintenance burden falls on the internal team rather than a vendor SLA |
| Full auditability — Iceberg snapshots provide point-in-time data reconstruction without separate backup infrastructure | Two-tier architecture (Iceberg + Snowflake native) increases data modelling complexity and requires teams to understand both layers |
