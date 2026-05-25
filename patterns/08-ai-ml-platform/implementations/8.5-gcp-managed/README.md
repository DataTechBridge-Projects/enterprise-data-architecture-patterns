---
layout: default
title: "8.5 — AI/ML Data Platform · GCP Fully Managed"
---

# 8.5 — AI/ML Data Platform · GCP Fully Managed

**Stack:** Vertex AI Feature Store · Vertex AI Pipelines · Vertex AI Vector Search · Gemini · GCS · BigQuery  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Cloud SQL / Spanner]
        S2[GCS Data Lake\nRaw / Curated]
        S3[Pub/Sub\nEvent Streams]
        S4[BigQuery\nAnalytical Store]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Dataflow\nBatch Feature Compute]
        I2[BigQuery SQL\nFeature Transform]
        I3[Dataflow Streaming\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Vertex AI Feature Store"]
        F1[Offline Store\nBigQuery · feature tables]
        F2[Online Store\nBigtable-backed · low-latency]
        F3[Feature Registry\nVertex AI metadata]
    end

    subgraph TRAIN["Training — Vertex AI Pipelines"]
        T1[Vertex AI Pipeline\nKubeflow-based DAG]
        T2[Vertex AI Training\nCustom / AutoML]
        T3[Vertex AI Experiments\nMetrics tracking]
        T4[Vertex AI Model Registry\nversioned artifacts]
    end

    subgraph VECTOR["Vector / LLM Layer"]
        V1[Embedding Pipeline\nVertex AI Batch Prediction]
        V2[Vertex AI Vector Search\nANN · HNSW]
        V3[Gemini API\nRAG · Grounding]
    end

    subgraph SERVE["Serving"]
        E1[Vertex AI Online Endpoint\nReal-Time Inference]
        E2[Vertex AI Batch Prediction\nBatch Scoring]
        E3[Cloud Endpoints / Apigee\nAPI Gateway]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Decisions]
        C2[Looker / BigQuery\nBatch Predictions]
        C3[Gemini App\nRAG Responses]
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
        A1[(Cloud SQL / Spanner)]
        A2[GCS Data Lake]
        A3[Pub/Sub]
        A4[BigQuery]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Dataflow + BigQuery SQL\nBatch Compute]
        B2[Dataflow Streaming\nStream Compute]
    end

    subgraph FeatureStore["Vertex AI Feature Store"]
        C1[Offline Store\nBigQuery Tables]
        C2[Online Store\nBigtable · ms latency]
        C3[Registry\nVertex AI Metadata]
    end

    subgraph VertexAI["Vertex AI Pipelines"]
        D1[Data Prep Component]
        D2[Training Job\nCustom / AutoML]
        D3[Model Registry\nVertex AI]
    end

    subgraph VectorRAG["Vector Search + Gemini"]
        E1[Batch Embeddings\nVertex AI Prediction]
        E2[Vertex Vector Search\nANN Index]
        E3[Gemini\nRAG Grounding]
    end

    subgraph Serving
        F1[Online Endpoint\nReal-Time]
        F2[Batch Prediction\nGCS out]
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
gs://<company>-ml-platform/
│
├── raw-features/
│   └── {domain}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet / Avro
│
├── training-datasets/
│   └── {model-name}/{pipeline-run-id}/
│       ├── train/  · validation/  · test/
│       └── metadata.json
│
├── model-artifacts/                        ← Vertex AI artifact store
│   └── {model-name}/{version}/
│       └── saved_model/  or  model.pkl
│
├── embeddings/
│   └── {index-name}/{run-date}/
│       └── *.json (id + embedding vector)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── predictions.jsonl

── BigQuery Datasets (separate from GCS)
   ├── feature_store_offline   ← Vertex AI Feature Store offline tables
   ├── ml_training             ← curated training views
   └── ml_predictions          ← batch prediction output tables
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│         IAM + Vertex AI RBAC + VPC Service Controls           │
│                                                              │
│  IAM Role / Principal   Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer@           Vertex AI Admin     All resources     │
│  data-scientist@        Vertex AI User      Pipelines · Features · Experiments │
│  ml-ops@                Vertex AI Deployer  Endpoints · Registry │
│  feature-consumer@      BigQuery Data Viewer + Bigtable Reader Online Store │
│  ai-app@                aiplatform.endpoints.predict Endpoint invoke │
│  analyst@               BigQuery Data Viewer Batch predictions |
│                                                              │
│  VPC Service Controls   → data exfiltration prevention       │
│  Private Service Connect → Vertex AI + BigQuery private      │
│  CMEK (Cloud KMS)       → GCS + BigQuery + Bigtable          │
│  Workload Identity      → GKE pod → service account (no key) │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Cloud Scheduler\nDaily feature pipeline]
    T2[📡 Pub/Sub Trigger\nNew data event]

    T1 --> J1[Dataflow Job\nBatch Feature Compute]
    T2 --> J2[Dataflow Streaming\nReal-Time Materialize]

    J1 --> J3[Vertex Feature Store\nImport Offline Features]
    J2 --> J4[Vertex Feature Store\nSync Online Store]

    J3 --> J5[Vertex AI Pipeline\nTraining DAG]
    J5 --> J6[Custom Training Job\nGPU · A100]
    J6 --> J7[Vertex AI Experiments\nLog Metrics]
    J7 -->|metrics pass| J8[Vertex Model Registry\nRegister]
    J8 -->|approval| J9[Online Endpoint Update\nCanary → Traffic split]
    J9 --> N1[Cloud Monitoring Alert\nDeploy success]

    J6 -->|fail| A1[Cloud Monitoring\n→ PagerDuty]

    V1[⏰ Nightly Cloud Scheduler] --> V2[Vertex Batch Prediction\nEmbedding Job]
    V2 --> V3[Vector Search\nRebuild / Upsert Index]
```

---

## Component Map

| Component | GCP Service | Notes |
|-----------|------------|-------|
| Object Storage | Google Cloud Storage | All zones: datasets, artifacts, embeddings |
| Analytical Store | BigQuery | Offline feature tables; prediction output |
| Batch Feature Compute | Dataflow (Apache Beam) | Managed runner; auto-scaling workers |
| BigQuery Feature SQL | BigQuery SQL | Feature views as BQ materialized views |
| Stream Feature Compute | Dataflow Streaming | Pub/Sub source; windowed aggregations |
| Feature Store (Offline) | Vertex AI Feature Store | BigQuery-backed; time-travel snapshots |
| Feature Store (Online) | Vertex AI Feature Store | Bigtable-backed; <10ms p99 |
| Feature Registry | Vertex AI Metadata | Feature group + version lineage |
| Training Orchestration | Vertex AI Pipelines (KFP) | KFP SDK; component-based DAGs |
| Training Jobs | Vertex AI Custom Training | TPU / GPU (A100); distributed TF/PyTorch |
| AutoML | Vertex AI AutoML | Tabular / text / vision models |
| Experiment Tracking | Vertex AI Experiments | MLflow-compatible SDK |
| Model Registry | Vertex AI Model Registry | Version + alias management |
| Real-Time Serving | Vertex AI Online Endpoints | Auto-scaling; shadow mode |
| Batch Scoring | Vertex AI Batch Prediction | GCS in → GCS/BQ out |
| Embedding Generation | Vertex AI Batch Prediction | text-embedding-gecko or custom |
| Vector Store | Vertex AI Vector Search | Managed ANN; HNSW/ScaNN index |
| LLM / RAG | Gemini API + Vertex AI Search | Grounding API; real-time retrieval |
| API Gateway | Cloud Endpoints / Apigee | Auth, rate limiting, routing |
| Monitoring | Cloud Monitoring + Vertex Model Monitoring | Prediction drift detection |
| Governance | Dataplex + Data Catalog | ML asset lineage |
| Encryption | Cloud KMS (CMEK) | GCS + BQ + Bigtable |

---

## Comparison vs 8.6 — GCP OSS on Cloud

| Dimension | 8.5 GCP Fully Managed | 8.6 GCP OSS on Cloud |
|-----------|----------------------|---------------------|
| Feature Store | Vertex AI Feature Store | Feast + Redis on GKE |
| Training Orchestration | Vertex AI Pipelines (KFP) | Kubeflow Pipelines on GKE |
| Experiment Tracking | Vertex AI Experiments | MLflow on GKE |
| Model Registry | Vertex AI Model Registry | MLflow Model Registry |
| Vector Store | Vertex AI Vector Search | Weaviate on GKE |
| LLM / RAG | Gemini API | LangChain + OpenAI / VertexAI SDK |
| Serving | Vertex AI Endpoints | BentoML / Seldon on GKE |
| Ops Burden | Low — fully managed | Medium — manage GKE workloads |
| Portability | GCP lock-in | OSS portable |
| Cost | Vertex AI pricing | GKE + VM (potentially lower) |
| Best For | GCP-committed, managed experience | OSS teams, portability |
