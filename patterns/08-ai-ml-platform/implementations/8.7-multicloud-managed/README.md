---
layout: default
title: "8.7 — AI/ML Data Platform · Multi-Cloud Fully Managed"
---

# 8.7 — AI/ML Data Platform · Multi-Cloud Fully Managed

**Stack:** Databricks (Feature Store · MLflow · Vector Search) · dbt Cloud · Delta Lake  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Buy (fully managed Databricks on any cloud)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Operational DBs\nPostgreSQL · MySQL · Oracle]
        S2[Cloud Object Storage\nS3 · ADLS · GCS]
        S3[Event Streams\nKafka · Kinesis · Event Hubs]
        S4[SaaS APIs\nSalesforce · Snowflake]
    end

    subgraph INGEST["Feature Engineering — Databricks"]
        I1[Databricks DLT\nBatch Feature Pipeline]
        I2[dbt Cloud\nSQL Feature Transform]
        I3[Databricks Streaming\nStructured Streaming → DLT]
    end

    subgraph FEATURE["Feature Store — Databricks Feature Store"]
        F1[Offline Store\nDelta Lake tables · Unity Catalog]
        F2[Online Store\nDatabricks Online Tables · ms lookup]
        F3[Feature Registry\nUnity Catalog lineage]
    end

    subgraph TRAIN["Training — Databricks + MLflow"]
        T1[Databricks Jobs\nOrchestrate Training]
        T2[All-Purpose / Job Cluster\nGPU training]
        T3[MLflow Tracking\nManaged on Databricks]
        T4[MLflow Model Registry\nUnity Catalog]
    end

    subgraph VECTOR["Vector / LLM Layer"]
        V1[Embedding Pipeline\nDatabricks Batch Inference]
        V2[Databricks Vector Search\nDelta Sync Index · HNSW]
        V3[DBRX / LLM Endpoint\nRAG via Mosaic AI]
    end

    subgraph SERVE["Serving — Mosaic AI Model Serving"]
        E1[Model Serving Endpoint\nReal-Time · GPU]
        E2[Batch Inference Job\nDatabricks Jobs]
        E3[AI Gateway\nRate limit · Auth · Routing]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Predictions]
        C2[BI / Analytics\nBatch Scores → Delta]
        C3[LLM App\nRAG Responses]
    end

    SRC --> INGEST
    I1 & I2 --> F1
    I3 --> F2
    F1 --> T1 --> T2 --> T3 --> T4
    T4 --> E1 & E2
    F1 --> V1 --> V2
    V2 & F2 --> V3
    E1 & E3 --> C1
    E2 --> C2
    V3 --> C3
    F2 -. online lookup .-> E3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Operational DBs)]
        A2[Cloud Storage\nS3 / ADLS / GCS]
        A3[Kafka / Event Streams]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Databricks DLT + dbt\nBatch Compute]
        B2[Structured Streaming\nStream Compute]
    end

    subgraph FeatureStore["Databricks Feature Store"]
        C1[Offline Store\nDelta Lake · Unity Catalog]
        C2[Online Tables\nReal-Time Lookup]
        C3[Registry\nUnity Catalog]
    end

    subgraph DatabricksML["Databricks MLflow"]
        D1[Jobs Orchestration]
        D2[GPU Cluster Training]
        D3[MLflow Registry\nUnity Catalog]
    end

    subgraph VectorRAG["Vector Search + LLM"]
        E1[Batch Inference\nEmbedding Pipeline]
        E2[Databricks Vector Search\nHNSW Index]
        E3[Mosaic AI LLM\nRAG endpoint]
    end

    subgraph Serving
        F1[Model Serving\nReal-Time Endpoint]
        F2[Batch Inference\nDelta output]
    end

    A1 & A2 --> B1 --> C1
    A3 --> B2 --> C2

    C1 & C3 --> D1 --> D2 --> D3
    D3 --> F1 & F2

    C1 --> E1 --> E2
    E2 & C2 --> E3
```

---

## Storage Zone Design

```
Delta Lake on Cloud Object Storage (S3 / ADLS / GCS)
Unity Catalog namespace: catalog.schema.table

catalog: ml_platform
│
├── schema: feature_store_offline
│   ├── {feature_table_name}              ← Databricks Feature Store tables
│   └── {feature_table_name}_checkpoint/
│
├── schema: training
│   ├── train_{model_name}_{run_id}
│   ├── validation_{model_name}_{run_id}
│   └── test_{model_name}_{run_id}
│
├── schema: vector_search
│   └── {collection_name}                 ← Delta table synced by Vector Search
│
└── schema: predictions
    └── {model_name}_{score_date}         ← Batch inference output

MLflow artifact store → s3|adls|gs://<company>-mlflow-artifacts/
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│        Databricks Unity Catalog + RBAC + IP Access Lists      │
│                                                              │
│  Group / Principal      Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineers           Admin               All catalogs · clusters │
│  data-scientists        USE SCHEMA + SELECT Feature Store · Experiments │
│  ml-ops                 EXECUTE + MODIFY    Model Registry · Endpoints │
│  feature-consumers      Online Tables API   Read-only online lookup │
│  llm-app                AI Gateway token    Model Serving endpoints │
│  analysts               SELECT              Predictions schema │
│                                                              │
│  Unity Catalog          → row/column security on feature tables │
│  AI Gateway             → API key per consumer; rate limits  │
│  Token-based Auth       → PAT + service principals (no passwords) │
│  Cluster Policies       → restrict cluster config per group  │
│  Customer-Managed Keys  → encryption on cloud object storage │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Databricks Job Schedule\nDaily feature pipeline]
    T2[📡 Delta Live Tables\nContinuous / triggered mode]

    T1 --> J1[DLT Pipeline\nBatch Feature Compute]
    T2 --> J2[Structured Streaming\nFeature Online Sync]

    J1 --> J3[Feature Store Write\nDelta Offline tables]
    J2 --> J4[Online Tables Sync\nAutomatic by Databricks]

    J3 --> J5[Databricks Job\nTraining Pipeline]
    J5 --> J6[GPU Cluster Job\nPyTorch / XGBoost]
    J6 --> J7[MLflow Autolog\nMetrics + Artifacts]
    J7 -->|metrics pass| J8[MLflow Register\nUnity Catalog]
    J8 -->|approval webhook| J9[Model Serving Update\nBlue/Green]
    J9 --> N1[Slack Webhook\nDeploy complete]

    J6 -->|fail| A1[Databricks Alert\n→ Slack / PagerDuty]

    V1[⏰ Nightly Job\nEmbedding refresh] --> V2[Batch Inference\nEmbedding model]
    V2 --> V3[Vector Search Sync\nDelta → HNSW index]
```

---

## Component Map

| Component | Databricks Service | Notes |
|-----------|------------------|-------|
| Object Storage | S3 / ADLS / GCS (cloud native) | Delta Lake files; cloud-agnostic |
| Table Format | Delta Lake | ACID; time travel; schema evolution |
| Metadata / Governance | Unity Catalog | Cross-workspace; column-level security |
| Batch Feature Compute | Databricks DLT | Declarative pipelines; auto data quality |
| SQL Feature Transform | dbt Cloud + Databricks SQL | Semantic layer; feature views as dbt models |
| Stream Feature Compute | Structured Streaming + DLT | Auto-upsert to online tables |
| Feature Store (Offline) | Databricks Feature Store | Delta-backed; pointInTime joins |
| Feature Store (Online) | Databricks Online Tables | Auto-sync from Delta; <10ms lookup |
| Feature Registry | Unity Catalog lineage | Automatic lineage from DLT + Feature Store |
| Training Orchestration | Databricks Jobs | DAG tasks; retry + alerting |
| Training Compute | All-Purpose / Job GPU Cluster | A10G / A100; spot instances |
| Experiment Tracking | MLflow (managed on Databricks) | Auto-logging; artifact in cloud storage |
| Model Registry | MLflow + Unity Catalog | Cross-environment promotion |
| Real-Time Serving | Mosaic AI Model Serving | GPU endpoints; shadow mode; A/B |
| Batch Scoring | Databricks Batch Inference | Pandas UDF or mlflow.pyfunc |
| Embedding Pipeline | Databricks Batch Inference | BGE / E5 / OpenAI embeddings |
| Vector Store | Databricks Vector Search | Delta Sync index; Direct Vector Access |
| LLM / RAG | Mosaic AI (DBRX / Llama) | AI Playground; RAG Studio |
| AI Gateway | Databricks AI Gateway | Unified endpoint; rate limit; audit |
| Monitoring | Databricks Lakehouse Monitoring | Drift detection on Delta tables |

---

## Comparison vs 8.8 — Multi-Cloud OSS Portable

| Dimension | 8.7 Multi-Cloud Managed | 8.8 Multi-Cloud OSS Portable |
|-----------|------------------------|------------------------------|
| Feature Store | Databricks Feature Store | Feast + Redis |
| Training Orchestration | Databricks Jobs | Apache Airflow |
| Experiment Tracking | MLflow (managed) | MLflow (self-hosted) |
| Model Registry | MLflow + Unity Catalog | MLflow (self-hosted) |
| Vector Store | Databricks Vector Search | Qdrant |
| LLM / RAG | Mosaic AI / DBRX | LangChain + any LLM provider |
| Serving | Mosaic AI Model Serving | BentoML / Seldon |
| Table Format | Delta Lake | Apache Iceberg |
| Ops Burden | Low — Databricks managed | Medium — self-manage OSS stack |
| Portability | Databricks lock-in | Fully portable OSS |
| Cost | Databricks DBU pricing | Infra cost + operational overhead |
| Best For | Enterprise scale, unified platform | Maximum portability, no vendor lock-in |
