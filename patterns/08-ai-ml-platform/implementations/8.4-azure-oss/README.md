---
layout: default
title: "8.4 — AI/ML Data Platform · Azure OSS on Cloud"
---

# 8.4 — AI/ML Data Platform · Azure OSS on Cloud

**Stack:** Feast · MLflow · Apache Airflow · Qdrant · LangChain · ADLS Gen2 · AKS · Redis  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Build (OSS on Azure-managed infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / PostgreSQL]
        S2[ADLS Gen2\nData Lake]
        S3[Event Hubs\nKafka API]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Airflow DAG\nBatch Feature Jobs]
        I2[Databricks / Spark on AKS\nLarge-scale Transform]
        I3[Flink on AKS\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Feast"]
        F1[Offline Store\nADLS + Trino]
        F2[Online Store\nRedis on AKS · ms lookup]
        F3[Feature Registry\nPostgreSQL on Azure DB]
    end

    subgraph TRAIN["Training — MLflow + Airflow"]
        T1[Airflow Pipeline\nOrchestrate Training]
        T2[Training Job\nPyTorch on AKS GPU nodes]
        T3[MLflow Tracking\nMetrics · Artifacts → ADLS]
        T4[MLflow Registry\nStaging → Production]
    end

    subgraph VECTOR["Vector / RAG Layer"]
        V1[Embedding Pipeline\nHuggingFace on AKS]
        V2[Qdrant on AKS\nVector Search · HNSW]
        V3[LangChain\nRAG Orchestration]
    end

    subgraph SERVE["Serving"]
        E1[BentoML on AKS\nReal-Time Endpoint]
        E2[Spark Batch Score\nADLS out]
        E3[NGINX Ingress\nTraffic Routing]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Predictions]
        C2[Analytics\nBatch Predictions]
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
    F2 -. online lookup .-> E1
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(Azure SQL)]
        A2[ADLS Gen2]
        A3[Event Hubs Kafka]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Airflow + Spark\nBatch Compute]
        B2[Flink on AKS\nStream Compute]
    end

    subgraph Feast["Feature Store — Feast"]
        C1[Offline Store\nADLS + Trino]
        C2[Online Store\nRedis on AKS]
        C3[Registry\nPostgreSQL]
    end

    subgraph MLflow["MLflow + Airflow"]
        D1[Airflow DAG]
        D2[GPU Training\nAKS node pool]
        D3[MLflow Registry]
    end

    subgraph VectorRAG["Qdrant + LangChain"]
        E1[Embedding Job\nHuggingFace]
        E2[Qdrant\nVector Search]
        E3[LangChain RAG]
    end

    subgraph Serving
        F1[BentoML\nReal-Time]
        F2[Batch Scores\nADLS]
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
adls://<company>-ml-oss/
│
├── raw-features/
│   └── {source}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet
│
├── feast-offline/                          ← Feast Offline Store
│   └── {feature_view}/{entity}/
│       └── year=YYYY/month=MM/day=DD/
│           └── *.parquet
│
├── training-datasets/
│   └── {model-name}/{experiment-id}/
│       ├── train.parquet
│       ├── validation.parquet
│       └── test.parquet
│
├── mlflow-artifacts/                       ← MLflow artifact store (ADLS)
│   └── {experiment-id}/{run-id}/
│       └── artifacts/
│
├── embeddings/
│   └── {collection}/{run-date}/
│       └── *.parquet (id + vector + metadata)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── predictions.parquet
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│        Azure AD + Feast RBAC + MLflow Auth + Qdrant API Keys  │
│                                                              │
│  AAD Group              Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer            Full ADLS + AKS     All zones        │
│  data-scientist         Contributor         Feast Offline · MLflow │
│  ml-ops                 Deploy              MLflow Registry · BentoML │
│  feature-consumer       Reader + Redis AUTH Online Store     │
│  llm-app                API Key             Qdrant collection │
│  analyst                Reader              Batch predictions ADLS │
│                                                              │
│  Redis TLS + AUTH       → in-transit encryption              │
│  Qdrant API Keys        → per-collection isolation           │
│  MLflow Auth            → AAD OIDC integration               │
│  Azure Key Vault        → secrets for all OSS services       │
│  AKS Network Policy     → pod-level traffic isolation        │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Daily Airflow DAG\nfeature_pipeline]
    T2[📡 Event Hub Consumer\nstream feature trigger]

    T1 --> J1[Spark on AKS\nBatch Feature Compute]
    T2 --> J2[Flink on AKS\nStream Materialize]

    J1 --> J3[Feast Materialize\nOffline → ADLS]
    J2 --> J4[Feast Online Sync\nOffline → Redis]

    J3 --> J5[Airflow DAG\ntraining_pipeline]
    J5 --> J6[AKS GPU Node\nPyTorch Training]
    J6 --> J7[MLflow Log\nMetrics + Artifacts]
    J7 -->|metrics pass| J8[MLflow Register\nStaging]
    J8 -->|manual approval| J9[BentoML Deploy\nAKS rollout]
    J9 --> N1[Teams Webhook\nDeploy alert]

    J6 -->|fail| A1[Airflow Slack Alert\nPipeline failure]

    V1[⏰ Nightly Embedding DAG] --> V2[HuggingFace Batch\nAKS Job]
    V2 --> V3[Qdrant Upsert\nCollection refresh]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | ADLS Gen2 | Hierarchical namespace; all zones |
| Batch Feature Compute | Apache Spark on AKS | Spark Operator; auto-scaling executor pods |
| Stream Feature Compute | Apache Flink on AKS | Flink Operator; Kafka source via Event Hubs |
| Feature Store (Offline) | Feast + ADLS + Trino | Trino query engine for ad-hoc feature exploration |
| Feature Store (Online) | Feast + Redis on AKS | Redis Operator; TLS + persistence |
| Feature Registry | Feast + PostgreSQL (Azure DB) | Managed PostgreSQL; HA failover |
| Training Orchestration | Apache Airflow on AKS | Helm chart; CeleryExecutor |
| Training Jobs | PyTorch on AKS GPU pool | NC/ND GPU nodes; distributed training |
| Experiment Tracking | MLflow Tracking on AKS | ADLS artifact store; PostgreSQL backend |
| Model Registry | MLflow Model Registry | Staging → Production promotion |
| Real-Time Serving | BentoML on AKS | Kubernetes deployment; HPA autoscale |
| Batch Scoring | Apache Spark on AKS | Same Spark Operator cluster |
| Embedding Pipeline | HuggingFace Transformers on AKS | GPU job; ADLS output |
| Vector Store | Qdrant on AKS | StatefulSet; persistent volume |
| RAG Orchestration | LangChain | Qdrant retriever + OpenAI / Azure OpenAI |
| Ingress | NGINX Ingress Controller | BentoML + RAG endpoint routing |
| Monitoring | Prometheus + Grafana on AKS | Scrape BentoML, Feast, MLflow metrics |
| Secrets | Azure Key Vault + CSI Driver | Mounted as env vars into pods |

---

## Comparison vs 8.3 — Azure Fully Managed

| Dimension | 8.4 Azure OSS on Cloud | 8.3 Azure Fully Managed |
|-----------|----------------------|------------------------|
| Feature Store | Feast + Redis on AKS | Azure ML Feature Store |
| Training Orchestration | Airflow on AKS | Azure ML Pipelines |
| Experiment Tracking | MLflow on AKS | Azure ML (MLflow compat) |
| Model Registry | MLflow Registry | Azure ML Registry |
| Vector Store | Qdrant on AKS | Azure AI Search |
| LLM / RAG | LangChain + OpenAI API | Azure OpenAI native |
| Serving | BentoML on AKS | Azure ML Online Endpoints |
| Ops Burden | Medium — manage AKS workloads | Low — fully managed |
| Portability | High — OSS portable | Azure lock-in |
| Cost | AKS + VM (potentially lower) | Azure ML compute pricing |
| Best For | OSS flexibility, portability | Azure-first, managed experience |
