---
layout: default
title: "8.8 — AI/ML Data Platform · Multi-Cloud OSS Portable"
---

# 8.8 — AI/ML Data Platform · Multi-Cloud OSS Portable

**Stack:** Feast · MLflow · Apache Airflow · Qdrant · LangChain · dbt Core · Apache Iceberg · Kubernetes  
**Processing:** Batch (training) + Real-Time (online serving) — Hybrid  
**Buy vs Build:** Build (fully portable OSS on any cloud)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[Operational DBs\nPostgreSQL · MySQL]
        S2[Object Storage\nS3 · ADLS · GCS]
        S3[Kafka\nEvent Streams]
        S4[APIs / SaaS]
    end

    subgraph INGEST["Feature Engineering"]
        I1[Airflow DAG\nBatch Feature Orchestration]
        I2[Spark on K8s\nLarge-scale Feature Compute]
        I3[Flink on K8s\nStream Feature Compute]
        I4[dbt Core\nSQL Feature Transforms]
    end

    subgraph FEATURE["Feature Store — Feast"]
        F1[Offline Store\nIceberg on S3/ADLS/GCS]
        F2[Online Store\nRedis on K8s · ms lookup]
        F3[Feature Registry\nPostgreSQL]
    end

    subgraph TRAIN["Training — MLflow + Airflow"]
        T1[Airflow Pipeline\nOrchestrate Training DAG]
        T2[Training Job\nPyTorch / XGBoost on K8s GPU]
        T3[MLflow Tracking\nMetrics · Params · Artifacts]
        T4[MLflow Registry\nStaging → Production]
    end

    subgraph VECTOR["Vector / RAG Layer"]
        V1[Embedding Pipeline\nHuggingFace on K8s]
        V2[Qdrant on K8s\nVector Search · HNSW]
        V3[LangChain\nRAG Orchestration · Provider Agnostic]
    end

    subgraph SERVE["Serving"]
        E1[BentoML on K8s\nReal-Time Endpoint]
        E2[Spark Batch Score\nIceberg output]
        E3[NGINX Ingress\nTraffic Routing]
    end

    subgraph CONSUME["Consumers"]
        C1[Applications\nReal-Time Predictions]
        C2[Analytics / BI\nBatch Predictions]
        C3[LLM App\nRAG Responses]
    end

    SRC --> INGEST
    I1 & I2 & I4 --> F1
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
        A1[(PostgreSQL / MySQL)]
        A2[Object Storage\nS3 · ADLS · GCS]
        A3[Kafka]
    end

    subgraph FeatureEng["Feature Engineering"]
        B1[Airflow + Spark K8s\nBatch Compute]
        B2[dbt Core\nSQL Features]
        B3[Flink K8s\nStream Compute]
    end

    subgraph Feast["Feature Store — Feast"]
        C1[Offline Store\nIceberg on Object Storage]
        C2[Online Store\nRedis on K8s]
        C3[Registry\nPostgreSQL]
    end

    subgraph MLflow["MLflow + Airflow"]
        D1[Airflow DAG]
        D2[GPU Training K8s]
        D3[MLflow Registry]
    end

    subgraph VectorRAG["Qdrant + LangChain"]
        E1[Embedding Job\nHuggingFace K8s]
        E2[Qdrant Vector Search]
        E3[LangChain RAG\nProvider Agnostic]
    end

    subgraph Serving
        F1[BentoML K8s\nReal-Time]
        F2[Batch Scores\nIceberg tables]
    end

    A1 & A2 --> B1 & B2 --> C1
    A3 --> B3 --> C2

    C1 & C3 --> D1 --> D2 --> D3
    D3 --> F1 & F2

    C1 --> E1 --> E2
    E2 & C2 --> E3
```

---

## Storage Zone Design

```
Object Storage (S3 / ADLS / GCS) — Apache Iceberg tables
│
├── raw-features/
│   └── {domain}/{entity}/
│       └── Parquet partitioned by date
│
├── iceberg-warehouse/                      ← Feast Offline + dbt outputs
│   └── {catalog}/{database}/
│       └── {table}/                        ← Iceberg table (data + metadata)
│           ├── data/   *.parquet
│           └── metadata/   *.json · *.avro
│
├── training-datasets/
│   └── {model-name}/{run-id}/
│       ├── train.parquet  · validation.parquet  · test.parquet
│       └── data-profile.json
│
├── mlflow-artifacts/
│   └── {experiment-id}/{run-id}/
│       └── artifacts/
│
├── embeddings/
│   └── {collection}/{run-date}/
│       └── *.parquet (id + vector + metadata)
│
└── batch-predictions/
    └── {model-name}/{score-date}/
        └── Iceberg table (id + prediction + score)
```

---

## Security Model

```
┌──────────────────────────────────────────────────────────────┐
│    K8s RBAC + Feast ACL + MLflow Auth + Qdrant API Keys       │
│                                                              │
│  K8s ServiceAccount    Access Level        Scope             │
│  ─────────────────────  ──────────────────  ──────────────── │
│  ml-engineer-sa        Admin               All namespaces    │
│  data-scientist-sa     Read + Write        ml-features · ml-training ns │
│  ml-ops-sa             Deploy              ml-serving ns     │
│  feature-consumer-sa   Redis AUTH (read)   Online Store      │
│  llm-app-sa            Qdrant API key      vector-search ns  │
│  analyst-sa            Read only           batch-output object prefix │
│                                                              │
│  Feast ACL             → per feature view access control    │
│  MLflow Auth           → OIDC (Keycloak or cloud IdP)       │
│  Qdrant API Keys       → per-collection granularity          │
│  Redis TLS + AUTH      → in-transit + password              │
│  OPA / Kyverno         → admission policies on K8s pods     │
│  Sealed Secrets / Vault → secret management per namespace   │
└──────────────────────────────────────────────────────────────┘
```

---

## Orchestration

```mermaid
flowchart TD
    T1[⏰ Airflow CronJob\nDaily feature pipeline]
    T2[📡 Kafka Consumer\nStream feature trigger]

    T1 --> J1[Spark K8s Job\nBatch Feature Compute]
    T2 --> J2[Flink K8s Job\nStream Materialize]

    J1 --> J3[dbt Core Run\nSQL Feature Transforms]
    J3 --> J4[Feast Materialize\nOffline → Iceberg]
    J2 --> J5[Feast Online Sync\n→ Redis]

    J4 --> J6[Airflow DAG\nTraining Pipeline]
    J6 --> J7[K8s GPU Job\nPyTorch Training]
    J7 --> J8[MLflow Log\nMetrics + Artifacts]
    J8 -->|metrics pass| J9[MLflow Register\nStaging]
    J9 -->|approval| J10[BentoML Deploy\nK8s rolling update]
    J10 --> N1[Slack Webhook\nDeploy alert]

    J7 -->|fail| A1[Airflow Alert\n→ Slack / PagerDuty]

    V1[⏰ Nightly Airflow DAG] --> V2[HuggingFace Batch K8s\nEmbedding Job]
    V2 --> V3[Qdrant Upsert\nCollection refresh]
```

---

## Component Map

| Component | Tool / Service | Notes |
|-----------|---------------|-------|
| Object Storage | S3 / ADLS / GCS (cloud native) | Any cloud; Iceberg abstraction above |
| Table Format | Apache Iceberg | ACID; time travel; cloud-agnostic |
| Iceberg Catalog | Nessie / Hive Metastore / JDBC | Git-like branching with Nessie |
| Batch Feature Compute | Apache Spark on Kubernetes | Spark Operator; Iceberg source/sink |
| SQL Feature Transforms | dbt Core | Feature views as dbt models |
| Stream Feature Compute | Apache Flink on Kubernetes | Flink Operator; Kafka source |
| Feature Store (Offline) | Feast + Iceberg | Offline store via Iceberg tables |
| Feature Store (Online) | Feast + Redis | Redis Operator; HA sentinel |
| Feature Registry | Feast + PostgreSQL | Metadata + lineage |
| Training Orchestration | Apache Airflow on K8s | CeleryExecutor or KubernetesExecutor |
| Training Jobs | PyTorch / XGBoost on K8s GPU | Gang scheduling; GPU node pools |
| Experiment Tracking | MLflow (self-hosted) | Object storage artifact backend |
| Model Registry | MLflow Model Registry | PostgreSQL backend |
| Real-Time Serving | BentoML on Kubernetes | HPA autoscale; multi-model |
| Batch Scoring | Apache Spark on K8s | Iceberg input + output |
| Embedding Pipeline | HuggingFace Transformers on K8s | BGE / E5 models; GPU batch |
| Vector Store | Qdrant on Kubernetes | StatefulSet; persistent volume |
| RAG Orchestration | LangChain | Qdrant retriever; any LLM provider |
| Ingress | NGINX Ingress Controller | BentoML + RAG API routing |
| Monitoring | Prometheus + Grafana | K8s + custom ML metrics |
| Secrets | HashiCorp Vault or Sealed Secrets | Cloud-agnostic secret management |
| Service Mesh | Istio (optional) | mTLS between ML services |

---

## Comparison vs 8.7 — Multi-Cloud Managed

| Dimension | 8.8 Multi-Cloud OSS Portable | 8.7 Multi-Cloud Managed |
|-----------|------------------------------|------------------------|
| Feature Store | Feast + Redis + Iceberg | Databricks Feature Store |
| Training Orchestration | Apache Airflow | Databricks Jobs |
| Experiment Tracking | MLflow (self-hosted) | MLflow (Databricks managed) |
| Model Registry | MLflow (self-hosted) | MLflow + Unity Catalog |
| Table Format | Apache Iceberg | Delta Lake |
| Vector Store | Qdrant | Databricks Vector Search |
| LLM / RAG | LangChain (any provider) | Mosaic AI / DBRX |
| Serving | BentoML | Mosaic AI Model Serving |
| Ops Burden | High — manage full OSS stack | Low — Databricks managed |
| Portability | Maximum — runs on any K8s | Databricks lock-in |
| Cost | Infra + operational overhead | Databricks DBU pricing |
| Best For | No vendor lock-in, OSS commitment | Enterprise scale, unified platform |
