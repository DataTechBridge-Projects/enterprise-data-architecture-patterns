---
layout: default
title: "8.3 — AI/ML Data Platform · Azure Fully Managed"
---

# 8.3 — AI/ML Data Platform · Azure Fully Managed

**Stack:** Azure Machine Learning Feature Store · Azure ML Pipelines · Azure AI Search (vector) · Azure OpenAI · ADLS Gen2  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Buy (fully managed, no infra to operate)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Azure SQL / Cosmos DB]
        S2[ADLS Gen2\nData Lake]
        S3[Event Hubs\nStreaming Data]
        S4[SaaS / APIs]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Azure Data Factory\nBatch ETL]
        I2[Azure Databricks\nSpark Feature Jobs]
        I3[Azure Stream Analytics\nStream Feature Compute]
    end

    subgraph FEATURE["Feature Store — Azure ML Feature Store"]
        F1[Offline Store\nADLS · Parquet]
        F2[Online Store\nAzure Cache for Redis]
        F3[Feature Registry\nAzure ML workspace]
    end

    subgraph TRAIN["Training — Azure ML Pipelines"]
        T1[Azure ML Pipeline\nData Prep → Train → Eval]
        T2[Azure ML Compute\nGPU Cluster]
        T3[Azure ML Tracking\nMetrics · MLflow compat]
        T4[Azure ML Registry\nModel versions]
    end

    subgraph VECTOR["Vector / LLM Layer"]
        V1[Embedding Pipeline\nAzure ML Batch]
        V2[Azure AI Search\nVector Index · HNSW]
        V3[Azure OpenAI\nGPT-4 · RAG]
    end

    subgraph SERVE["Serving"]
        E1[Azure ML Online Endpoint\nReal-Time Inference]
        E2[Azure ML Batch Endpoint\nBatch Scoring]
        E3[Azure API Management\nUnified API Gateway]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Decisions]
        C2[Power BI / Synapse\nBatch Predictions]
        C3[Copilot App\nRAG Responses]
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
        A1[(Azure SQL)]
        A2[ADLS Gen2]
        A3[Event Hubs]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[ADF + Databricks\nBatch Compute]
        B2[Stream Analytics\nStream Compute]
    end

    subgraph FeatureStore["Azure ML Feature Store"]
        C1[Offline Store\nADLS · Parquet]
        C2[Online Store\nRedis Cache]
        C3[Feature Registry\nAzure ML]
    end

    subgraph AzureML["Azure ML Pipelines"]
        D1[Data Prep Component]
        D2[Training Component\nGPU Compute]
        D3[Azure ML Registry\nModel Versions]
    end

    subgraph VectorRAG["AI Search + Azure OpenAI"]
        E1[Embedding Batch\nAzure ML Job]
        E2[Azure AI Search\nVector Index]
        E3[Azure OpenAI\nGPT-4 RAG]
    end

    subgraph Serving
        F1[Online Endpoint\nReal-Time]
        F2[Batch Endpoint\nOffline Scoring]
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
adls://<company>-ml-platform/
│
├── raw-features/
│   └── {domain}/{entity}/year=YYYY/month=MM/day=DD/
│       └── Parquet · raw signals
│
├── feature-store/                          ← Azure ML Feature Store offline
│   └── {feature_set}/{version}/
│       └── year=YYYY/month=MM/day=DD/
│           └── *.parquet
│
├── training-datasets/
│   └── {model-name}/{run-id}/
│       ├── train/  · validation/  · test/
│       └── metadata.json
│
├── model-artifacts/                        ← Azure ML artifact store
│   └── {model-name}/{version}/
│       └── model/  + mlflow-model/
│
├── embeddings/
│   └── {index-name}/{run-date}/
│       └── *.parquet (id + vector + content)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── predictions.parquet
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│      Azure RBAC + Azure ML Workspace Access Control           │
│                                                              │
│  AAD Group / Role       Access Level        Scope            │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer            AzureML Contributor All workspace    │
│  data-scientist         AzureML Data Scientist Experiments · Features │
│  ml-ops                 AzureML Operator    Endpoints · Registry │
│  feature-consumer       Reader + Redis AUTH Online Store only │
│  ai-app                 Cognitive Services User OpenAI · AI Search │
│  analyst                Reader              Batch predictions ADLS │
│                                                              │
│  Azure Private Link     → AML workspace + ADLS private       │
│  Managed Identity       → no credential secrets in code      │
│  Azure Key Vault        → model secrets · API keys           │
│  Customer-Managed Keys  → CMK encryption on ADLS + Redis     │
│  Microsoft Defender     → threat protection on ADLS          │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Daily ADF Trigger\nfeature pipeline]
    T2[📡 Event Hub Consumer\nstream feature trigger]

    T1 --> J1[ADF Pipeline\nBatch Feature ETL]
    T2 --> J2[Stream Analytics\nReal-Time Materialize]

    J1 --> J3[Azure ML Feature Store\nMaterialize Offline]
    J2 --> J4[Feature Store Online\nRedis Sync]

    J3 --> J5[Azure ML Pipeline\nTrain Trigger]
    J5 --> J6[GPU Compute Cluster\nTraining Job]
    J6 --> J7[Azure ML Tracking\nLog Metrics]
    J7 -->|metrics pass| J8[Azure ML Registry\nRegister Model]
    J8 -->|approval gate| J9[Online Endpoint Update\nBlue/Green]
    J9 --> N1[Azure Monitor Alert\nDeploy complete]

    J6 -->|fail| A1[Azure Monitor Alarm\n→ Teams / PagerDuty]

    V1[⏰ Nightly Embedding Job] --> V2[Azure ML Batch\nGenerate Embeddings]
    V2 --> V3[AI Search Index\nBulk Upload]
```

---

## Component Map

| Component | Azure Service | Notes |
|-----------|--------------|-------|
| Object Storage | ADLS Gen2 | Hierarchical namespace; all zones |
| Batch Feature ETL | Azure Data Factory | Linked services to Azure SQL, ADLS |
| Spark Feature Compute | Azure Databricks | Delta Lake integration; job clusters |
| Stream Feature Compute | Azure Stream Analytics | Windowed aggregations → Redis |
| Feature Store (Offline) | Azure ML Feature Store | ADLS-backed; Spark queryable |
| Feature Store (Online) | Azure Cache for Redis | Enterprise tier; TLS + AUTH |
| Training Orchestration | Azure ML Pipelines | Component-based DAG; reusable steps |
| Compute | Azure ML Compute Cluster | GPU (NC/NDs series); autoscale 0→N |
| Experiment Tracking | Azure ML (MLflow-compatible) | Metrics, params, artifacts per run |
| Model Registry | Azure ML Registry | Cross-workspace model sharing |
| Real-Time Serving | Azure ML Online Endpoints | Managed K8s; traffic splitting |
| Batch Scoring | Azure ML Batch Endpoints | Parallelized scoring on compute cluster |
| Embedding Pipeline | Azure ML Batch Job | text-embedding-ada-002 or custom |
| Vector Store | Azure AI Search | Vector + hybrid (BM25 + kNN) |
| LLM / RAG | Azure OpenAI (GPT-4) | RAG via AI Search semantic ranking |
| API Management | Azure API Management | Rate limit, auth, routing |
| Monitoring | Azure Monitor + App Insights | Endpoint latency, drift detection |
| Governance | Azure Purview | ML asset lineage integration |
| Encryption | Azure Key Vault + CMK | Secrets + customer-managed key |

---

## Comparison vs 8.4 — Azure OSS on Cloud

| Dimension | 8.3 Azure Fully Managed | 8.4 Azure OSS on Cloud |
|-----------|------------------------|----------------------|
| Feature Store | Azure ML Feature Store | Feast + Redis on AKS |
| Training Orchestration | Azure ML Pipelines | Apache Airflow on AKS |
| Experiment Tracking | Azure ML (MLflow compat) | MLflow on AKS |
| Model Registry | Azure ML Registry | MLflow Model Registry |
| Vector Store | Azure AI Search | Qdrant on AKS |
| LLM / RAG | Azure OpenAI | LangChain + OpenAI API |
| Serving | Azure ML Online Endpoints | BentoML on AKS |
| Ops Burden | Low — fully managed | Medium — manage AKS workloads |
| Portability | Azure lock-in | OSS portable |
| Cost Model | Azure ML compute pricing | AKS + VM pricing |
| Best For | Azure-committed, enterprise scale | Cost control, OSS flexibility |
