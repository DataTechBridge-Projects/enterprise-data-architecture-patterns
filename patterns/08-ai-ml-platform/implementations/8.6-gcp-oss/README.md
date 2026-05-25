---
layout: default
title: "8.6 — AI/ML Data Platform · GCP OSS on Cloud"
---

# 8.6 — AI/ML Data Platform · GCP OSS on Cloud

**Stack:** Feast · MLflow · Kubeflow Pipelines · Weaviate · LangChain · GCS · GKE · Redis  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Build (OSS on GCP-managed infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / PostgreSQL]
        S2[GCS Data Lake]
        S3[Pub/Sub\nEvent Streams]
        S4[BigQuery]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Kubeflow Pipeline\nBatch Feature Jobs]
        I2[Spark on Dataproc\nLarge-scale Transform]
        I3[Dataflow Streaming\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Feast"]
        F1[Offline Store\nGCS + BigQuery]
        F2[Online Store\nRedis on GKE]
        F3[Feature Registry\nPostgreSQL on Cloud SQL]
    end

    subgraph TRAIN["Training — Kubeflow + MLflow"]
        T1[Kubeflow Pipeline\nOrchestrate Training]
        T2[Training Job\nGPU on GKE]
        T3[MLflow Tracking\nMetrics · Artifacts → GCS]
        T4[MLflow Registry\nStaging → Production]
    end

    subgraph VECTOR["Vector / RAG Layer"]
        V1[Embedding Pipeline\nHuggingFace on GKE]
        V2[Weaviate on GKE\nVector Search · HNSW]
        V3[LangChain\nRAG Orchestration]
    end

    subgraph SERVE["Serving"]
        E1[BentoML on GKE\nReal-Time Endpoint]
        E2[Spark Batch Score\nGCS out]
        E3[Cloud Load Balancer\nIngress]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Predictions]
        C2[BigQuery / Looker\nBatch Predictions]
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
        A1[(Cloud SQL)]
        A2[GCS Data Lake]
        A3[Pub/Sub]
        A4[BigQuery]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Kubeflow + Spark Dataproc\nBatch Compute]
        B2[Dataflow Streaming\nStream Compute]
    end

    subgraph Feast["Feature Store — Feast"]
        C1[Offline Store\nGCS + BigQuery]
        C2[Online Store\nRedis on GKE]
        C3[Registry\nCloud SQL PostgreSQL]
    end

    subgraph KFP["Kubeflow + MLflow"]
        D1[Kubeflow Pipeline]
        D2[GPU Training\nGKE A100 nodes]
        D3[MLflow Registry]
    end

    subgraph VectorRAG["Weaviate + LangChain"]
        E1[Embedding Job\nHuggingFace on GKE]
        E2[Weaviate\nVector Search]
        E3[LangChain RAG]
    end

    subgraph Serving
        F1[BentoML\nReal-Time]
        F2[Batch Scores\nGCS]
    end

    A1 & A2 & A4 --> B1 --> C1
    A3 --> B2 --> C2

    C1 & C3 --> D1 --> D2 --> D3
    D3 --> F1 & F2

    C1 --> E1 --> E2
    E2 & C2 --> E3
```

---

## Storage Zone Design

```
gs://<company>-ml-oss/
│
├── raw-features/
│   └── {domain}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet / Avro
│
├── feast-offline/                          ← Feast Offline Store (GCS)
│   └── {feature_view}/{entity}/
│       └── year=YYYY/month=MM/day=DD/
│           └── *.parquet
│
├── training-datasets/
│   └── {model-name}/{pipeline-run-id}/
│       ├── train.parquet  · validation.parquet  · test.parquet
│       └── schema.json
│
├── mlflow-artifacts/                       ← MLflow GCS artifact store
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
│      IAM + Workload Identity + Feast RBAC + Weaviate API Keys │
│                                                              │
│  IAM Role / SA          Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer@           Storage Admin + GKE All resources    │
│  data-scientist@        BigQuery Data Viewer + GCS Reader Features · Training │
│  ml-ops@                GKE Developer + GCS Reader Registry · BentoML |
│  feature-consumer@      Redis AUTH (read)   Online Store     │
│  llm-app@               Weaviate API Key    Vector collection │
│  analyst@               BigQuery Viewer     Batch predictions │
│                                                              │
│  Workload Identity      → GKE pod SA → GCP IAM (no key files) │
│  VPC Private Cluster    → GKE nodes not publicly accessible  │
│  Redis TLS + AUTH       → in-transit encryption              │
│  Weaviate API Keys      → per-class access control           │
│  Cloud KMS              → GCS + BigQuery CMEK encryption     │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Cloud Scheduler\nDaily feature pipeline]
    T2[📡 Pub/Sub Trigger\nNew data event]

    T1 --> J1[Kubeflow Pipeline\nBatch Feature DAG]
    T2 --> J2[Dataflow Streaming\nReal-Time Materialize]

    J1 --> J3[Feast Materialize\nOffline → GCS/BQ]
    J2 --> J4[Feast Online Sync\n→ Redis]

    J3 --> J5[Kubeflow Pipeline\nTraining DAG]
    J5 --> J6[GKE GPU Job\nPyTorch Training]
    J6 --> J7[MLflow Log\nMetrics + Artifacts]
    J7 -->|metrics pass| J8[MLflow Register\nStaging]
    J8 -->|approval| J9[BentoML Deploy\nGKE rollout]
    J9 --> N1[Slack Webhook\nDeploy alert]

    J6 -->|fail| A1[Cloud Monitoring\n→ PagerDuty]

    V1[⏰ Nightly Cloud Scheduler] --> V2[HuggingFace Batch\nGKE GPU Job]
    V2 --> V3[Weaviate Upsert\nCollection refresh]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | Google Cloud Storage | All zones: features, training, artifacts, predictions |
| Analytical Store | BigQuery | Feast offline tables; batch prediction output |
| Batch Feature Compute | Apache Spark on Dataproc | Managed Spark; ephemeral clusters |
| Stream Feature Compute | Dataflow Streaming (Apache Beam) | Pub/Sub → Redis materialization |
| Feature Store (Offline) | Feast + GCS + BigQuery | BigQuery source + GCS file store |
| Feature Store (Online) | Feast + Redis on GKE | Redis Operator; HA sentinel mode |
| Feature Registry | Feast + PostgreSQL (Cloud SQL) | Managed PostgreSQL; HA |
| Training Orchestration | Kubeflow Pipelines on GKE | KFP SDK; Argo Workflow backend |
| Training Jobs | PyTorch on GKE GPU nodes | A100 node pool; Gang scheduling |
| Experiment Tracking | MLflow on GKE | GCS artifact store; Cloud SQL backend |
| Model Registry | MLflow Model Registry | Staging → Production; webhook on promotion |
| Real-Time Serving | BentoML on GKE | Kubernetes deployment; HPA on RPS |
| Batch Scoring | Spark on Dataproc | GCS input; GCS/BigQuery output |
| Embedding Pipeline | HuggingFace Transformers on GKE | GPU Job; GCS output |
| Vector Store | Weaviate on GKE | StatefulSet; persistent disk |
| RAG Orchestration | LangChain | Weaviate retriever + Google PaLM / OpenAI |
| Ingress | GKE Ingress + Cloud Load Balancer | BentoML + RAG routing |
| Monitoring | Prometheus + Grafana on GKE | GKE pod metrics + custom ML metrics |
| Secrets | Secret Manager + Workload Identity | No key files; SA token binding |

---

## Comparison vs 8.5 — GCP Fully Managed

| Dimension | 8.6 GCP OSS on Cloud | 8.5 GCP Fully Managed |
|-----------|---------------------|----------------------|
| Feature Store | Feast + Redis on GKE | Vertex AI Feature Store |
| Training Orchestration | Kubeflow Pipelines on GKE | Vertex AI Pipelines (KFP managed) |
| Experiment Tracking | MLflow on GKE | Vertex AI Experiments |
| Model Registry | MLflow Registry | Vertex AI Model Registry |
| Vector Store | Weaviate on GKE | Vertex AI Vector Search |
| LLM / RAG | LangChain + OpenAI API | Gemini API native |
| Serving | BentoML on GKE | Vertex AI Online Endpoints |
| Ops Burden | Medium — manage GKE workloads | Low — fully managed |
| Portability | High — OSS portable | GCP lock-in |
| Cost | GKE + VM (potentially lower) | Vertex AI pricing |
| Best For | OSS teams, portability needs | GCP-first, managed experience |
