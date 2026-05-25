---
layout: default
title: "8.9 — AI/ML Data Platform · Hybrid OSS Self-Hosted"
---

# 8.9 — AI/ML Data Platform · Hybrid OSS Self-Hosted (On-Prem + Cloud)

**Stack:** Feast · MLflow · Kubeflow Pipelines · Qdrant · Ollama (local LLM) · MinIO · Redis · Kubernetes on-prem  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Full Build (self-hosted, on-prem + private cloud)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources — On-Prem"]
        S1[Oracle / SQL Server\nOn-Prem DBs]
        S2[MinIO\nOn-Prem Object Storage]
        S3[Kafka on K8s\nEvent Streams]
        S4[Legacy Systems\nMTF / File Drops]
    end

    subgraph INGEST["Feature Engineering — On-Prem K8s"]
        I1[Kubeflow Pipeline\nBatch Feature DAG]
        I2[Spark on K8s\nFeature Compute]
        I3[Flink on K8s\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Feast (On-Prem)"]
        F1[Offline Store\nMinIO + Parquet]
        F2[Online Store\nRedis on K8s · ms lookup]
        F3[Feature Registry\nPostgreSQL on K8s]
    end

    subgraph TRAIN["Training — Kubeflow + MLflow (On-Prem)"]
        T1[Kubeflow Pipeline\nOrchestrate Training]
        T2[GPU Training Job\nNVIDIA A100 on-prem]
        T3[MLflow Tracking\nOn-Prem Server]
        T4[MLflow Registry\nOn-Prem Registry]
    end

    subgraph CLOUD["Cloud Burst — AWS / Azure / GCP"]
        CL1[Cloud GPU Instances\nA100 · H100 for large jobs]
        CL2[Cloud Object Storage\nS3/ADLS/GCS for overflow]
    end

    subgraph VECTOR["Vector / LLM Layer — On-Prem"]
        V1[Embedding Pipeline\nHuggingFace · On-Prem GPU]
        V2[Qdrant on K8s\nVector Search · HNSW]
        V3[Ollama\nLocal LLM · Llama3 / Mistral]
    end

    subgraph SERVE["Serving — On-Prem K8s"]
        E1[BentoML on K8s\nReal-Time Endpoint]
        E2[Spark Batch Score\nMinIO output]
        E3[NGINX Ingress\nTraffic Routing]
    end

    subgraph CONSUME["Consumers"]
        C1[Internal Apps\nReal-Time Predictions]
        C2[Analytics\nBatch Predictions]
        C3[Internal Chatbot\nOllama RAG · no data egress]
    end

    SRC --> INGEST
    I1 & I2 --> F1
    I3 --> F2
    F1 --> T1 --> T2 --> T3 --> T4
    T2 -. cloud burst .-> CL1
    CL1 -. artifacts .-> CL2 -. sync back .-> F1
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
    subgraph OnPremSrc["On-Prem Sources"]
        A1[(Oracle / SQL Server)]
        A2[MinIO\nObject Storage]
        A3[Kafka on K8s]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Kubeflow + Spark K8s\nBatch Compute]
        B2[Flink K8s\nStream Compute]
    end

    subgraph Feast["Feature Store — Feast"]
        C1[Offline Store\nMinIO · Parquet]
        C2[Online Store\nRedis K8s]
        C3[Registry\nPostgreSQL K8s]
    end

    subgraph MLflow["Kubeflow + MLflow"]
        D1[Kubeflow Pipeline]
        D2[On-Prem GPU Training\nor Cloud Burst]
        D3[MLflow Registry\nOn-Prem]
    end

    subgraph VectorRAG["Qdrant + Ollama"]
        E1[Embedding Job\nHuggingFace On-Prem GPU]
        E2[Qdrant\nVector Search]
        E3[Ollama\nLocal LLM · RAG]
    end

    subgraph Serving
        F1[BentoML K8s\nReal-Time]
        F2[Batch Scores\nMinIO]
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
MinIO (on-prem) — s3-compatible API
│
├── raw-features/
│   └── {domain}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet · raw signals from on-prem sources
│
├── feast-offline/                          ← Feast Offline Store
│   └── {feature_view}/{entity}/
│       └── year=YYYY/month=MM/day=DD/
│           └── *.parquet
│
├── training-datasets/
│   └── {model-name}/{pipeline-run-id}/
│       ├── train.parquet  · validation.parquet  · test.parquet
│       └── schema.json
│
├── mlflow-artifacts/                       ← MLflow artifact store
│   └── {experiment-id}/{run-id}/
│       └── artifacts/  (model + plots + metrics)
│
├── embeddings/
│   └── {collection}/{run-date}/
│       └── *.parquet (id + vector + metadata)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── predictions.parquet

Cloud Object Storage (for burst / DR only):
  s3://<company>-ml-burst/ or adls://<company>-ml-burst/
  └── large-training-artifacts/{job-id}/
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│   K8s RBAC + MinIO IAM + Feast ACL + Ollama local-only       │
│                                                              │
│  K8s ServiceAccount    Access Level        Scope             │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer-sa        Admin               All namespaces    │
│  data-scientist-sa     Read + Write        Features · Training │
│  ml-ops-sa             Deploy              Serving namespace  │
│  feature-consumer-sa   Redis AUTH (read)   Online Store      │
│  internal-app-sa       BentoML API key     Serving endpoint  │
│  analyst-sa            MinIO read          Batch predictions bucket │
│                                                              │
│  Data Sovereignty       → all data stays on-prem            │
│  Ollama (local LLM)     → no data egress to external APIs    │
│  MinIO IAM              → bucket + path access policies      │
│  OPA / Kyverno          → admission control on GPU workloads │
│  HashiCorp Vault        → secrets management on-prem         │
│  Network Isolation      → air-gap capable; VPN for cloud burst │
│  NVIDIA MIG             → GPU partition isolation per team   │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Kubeflow Cron\nDaily feature pipeline]
    T2[📡 Kafka Consumer\nStream feature trigger]

    T1 --> J1[Spark K8s Job\nBatch Feature Compute]
    T2 --> J2[Flink K8s Job\nStream Materialize]

    J1 --> J3[Feast Materialize\nOffline → MinIO]
    J2 --> J4[Feast Online Sync\n→ Redis]

    J3 --> J5[Kubeflow Pipeline\nTraining DAG]
    J5 --> J6{GPU Available?}
    J6 -->|on-prem| J7[On-Prem A100\nTraining Job]
    J6 -->|cloud burst| J8[Cloud GPU Instance\nTraining via VPN]
    J7 & J8 --> J9[MLflow Log\nMetrics + Artifacts → MinIO]
    J9 -->|metrics pass| J10[MLflow Register\nOn-Prem Registry]
    J10 -->|approval| J11[BentoML Deploy\nK8s rolling update]
    J11 --> N1[Internal Alert\nDeploy complete]

    J7 -->|fail| A1[Kubeflow Alert\n→ Email / Slack on-prem]

    V1[⏰ Nightly Kubeflow DAG] --> V2[HuggingFace Batch\nOn-Prem GPU Job]
    V2 --> V3[Qdrant Upsert\nOn-Prem collection]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| On-Prem Object Storage | MinIO | S3-compatible; distributed HA mode |
| On-Prem K8s | Kubernetes (kubeadm / Rancher) | Bare-metal or on-prem VMs |
| GPU Hardware | NVIDIA A100 / H100 on-prem | MIG partitioning; CUDA device plugin |
| Batch Feature Compute | Apache Spark on Kubernetes | Spark Operator; MinIO S3A connector |
| Stream Feature Compute | Apache Flink on Kubernetes | Flink Operator; Kafka source |
| Feature Store (Offline) | Feast + MinIO | S3-compatible offline store |
| Feature Store (Online) | Feast + Redis on K8s | Redis Operator; persistence + AOF |
| Feature Registry | Feast + PostgreSQL on K8s | PostgreSQL Operator; HA + backups |
| Training Orchestration | Kubeflow Pipelines | KFP SDK; Argo Workflow backend |
| Training Jobs | PyTorch / XGBoost on K8s GPU | Gang scheduling; NCCL for distributed |
| Cloud Burst | AWS / Azure / GCP spot GPU | VPN tunnel; artifact sync back to MinIO |
| Experiment Tracking | MLflow (on-prem K8s) | MinIO artifact store; PostgreSQL backend |
| Model Registry | MLflow Model Registry (on-prem) | Staging → Production promotion |
| Real-Time Serving | BentoML on Kubernetes | HPA autoscale; GPU serving |
| Batch Scoring | Apache Spark on K8s | MinIO input + output |
| Embedding Pipeline | HuggingFace Transformers | On-prem GPU; BGE / E5 models |
| Vector Store | Qdrant on Kubernetes | StatefulSet; persistent volume; on-prem |
| Local LLM | Ollama (Llama3 / Mistral / Phi) | No data egress; GPU-backed inference |
| RAG Orchestration | LangChain + Ollama | Qdrant retriever → Ollama local LLM |
| Ingress | NGINX Ingress Controller | Internal DNS; no public exposure |
| Monitoring | Prometheus + Grafana (on-prem) | GPU metrics (DCGM exporter) |
| Secrets | HashiCorp Vault (on-prem) | On-prem secrets; PKI |
| Service Mesh | Istio | mTLS between all ML services |

---

## Comparison vs 8.8 — Multi-Cloud OSS Portable

| Dimension | 8.9 Hybrid OSS Self-Hosted | 8.8 Multi-Cloud OSS Portable |
|-----------|---------------------------|------------------------------|
| Deployment | On-prem K8s + optional cloud burst | Cloud K8s (any cloud) |
| Object Storage | MinIO (S3-compatible) | S3 / ADLS / GCS native |
| LLM | Ollama (local, no egress) | LangChain + external LLM API |
| Data Sovereignty | Full — no data leaves on-prem | Depends on cloud region policy |
| GPU | Own hardware (CapEx) | Cloud spot instances (OpEx) |
| Cloud Burst | Optional VPN-based burst | Native cloud elasticity |
| Ops Burden | Highest — manage hardware + K8s | High — manage K8s on cloud |
| Portability | Self-contained; no cloud dependency | Cloud portable, not cloud-free |
| Cost | High CapEx, low OpEx at scale | OpEx-only; scales to zero |
| Best For | Air-gap, regulated, sovereignty-first | OSS teams on cloud, no hardware |
