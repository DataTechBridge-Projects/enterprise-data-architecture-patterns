---
layout: default
title: "sf.3 — Snowflake · AI / ML with Snowpark and Cortex"
---

# sf.3 — Snowflake · AI / ML with Snowpark and Cortex

**Stack:** Fivetran · Snowflake · Snowpark ML · Cortex AI · Streamlit in Snowflake
**Processing:** Batch training · Real-time inference · In-platform ML
**Buy vs Build:** Buy (fully managed in-platform ML; no external ML infrastructure)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Data Sources"]
        S1[OLTP / ERP\nSAP · Oracle · Postgres]
        S2[SaaS Applications\nSalesforce · Marketo · Zendesk]
        S3[Event Streams\nClickstream · Product telemetry]
        S4[Unstructured Data\nDocuments · PDFs · Emails]
    end

    subgraph INGEST["Ingestion — Fivetran + Snowpipe"]
        I1[Fivetran\nStructured SaaS sources]
        I2[Snowpipe\nEvent micro-batch from S3]
        I3[Snowflake Document AI\nPDF and image extraction]
    end

    subgraph STORAGE["Snowflake — Data Foundation"]
        DB1[RAW Database\nFivetran-managed schemas]
        DB2[CORE Database\ndbt-transformed features]
        DB3[FEATURE STORE\nSnowpark ML feature tables]
        DB4[UNSTRUCTURED\nSnowflake Cortex documents]
    end

    subgraph FEATUREENG["Feature Engineering — Snowpark"]
        FE1[Snowpark Python\nDataFrame API on Snowflake]
        FE2[Snowpark ML Preprocessing\nscalers · encoders · imputers]
        FE3[Feature Store API\nversioned feature sets]
    end

    subgraph MLTRAINING["Model Training — Snowpark ML"]
        MT1[Snowpark ML Modeling\nsklearn-compatible API in Snowflake]
        MT2[Snowflake Model Registry\nversion · metadata · lineage]
        MT3[Cortex Fine-tuning\nLLM fine-tune on Snowflake data]
    end

    subgraph INFERENCE["Inference — Cortex + Snowpark"]
        INF1[Snowpark UDFs\nBatch scoring via Python UDF]
        INF2[Cortex ML Functions\nFORECAST · ANOMALY DETECTION]
        INF3[Cortex LLM Functions\nCOMPLETE · CLASSIFY · SUMMARIZE]
        INF4[Model Registry Inference\ndeploy model for SQL invocation]
    end

    subgraph APPS["Applications — Streamlit in Snowflake"]
        APP1[Streamlit Dashboards\nML predictions + Cortex insights]
        APP2[Internal AI Apps\nchat on data · document Q and A]
        APP3[Analyst Notebooks\nHex · Jupyter via Snowflake Partner Connect]
    end

    SRC --> INGEST
    INGEST --> DB1
    DB1 --> DB2
    DB2 --> FE1 --> FE2 --> FE3
    FE3 --> DB3
    DB3 --> MT1 --> MT2
    DB4 --> MT3
    MT2 --> INF1 & INF4
    DB2 --> INF2 & INF3
    INF1 & INF2 & INF3 & INF4 --> APP1 & APP2
    DB2 --> APP3
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Sources
        A1[(OLTP / ERP)]
        A2[SaaS APIs]
        A3[Events\nClickstream]
        A4[Documents\nPDFs · Emails]
    end

    subgraph Ingestion
        B1[Fivetran\nStructured sync]
        B2[Snowpipe\nMicro-batch events]
        B3[Document AI\nUnstructured extract]
    end

    subgraph Foundation["Snowflake — Data Foundation"]
        C1[RAW Database]
        C2[CORE Database\ndbt transformed]
    end

    subgraph Features["Snowpark ML — Feature Engineering"]
        D1[Snowpark Python\nDataFrame transforms]
        D2[Preprocessing\nscale · encode]
        D3[Feature Store\nversioned feature sets]
    end

    subgraph Training["Snowpark ML — Training"]
        E1[Model Training\nsklearn-compatible API]
        E2[Model Registry\nversion · promote]
    end

    subgraph Inference["Inference — In-Platform"]
        F1[Cortex ML Functions\nFORECAST · ANOMALY]
        F2[Cortex LLM\nCOMPLETE · CLASSIFY]
        F3[Snowpark UDF\nBatch scoring]
    end

    subgraph Apps["Streamlit in Snowflake"]
        G1[Prediction Dashboards]
        G2[AI Data Apps]
    end

    A1 --> B1 --> C1
    A2 --> B1 --> C1
    A3 --> B2 --> C1
    A4 --> B3 --> C1

    C1 -->|dbt Core transform| C2
    C2 -->|Snowpark Python| D1 --> D2 --> D3
    D3 -->|train| E1 --> E2
    E2 -->|deploy| F3
    C2 -->|SQL functions| F1 & F2

    F1 & F2 & F3 --> G1 & G2
```

---

## Component Breakdown

| Layer | Tool | Role |
|-------|------|------|
| Ingestion — Structured | Fivetran | Managed connectors for ERP, CRM, SaaS sources into RAW database |
| Ingestion — Events | Snowpipe | Continuous micro-batch loading from S3/GCS event landing zones |
| Ingestion — Unstructured | Snowflake Document AI | Extract structured data from PDFs, images, and forms into Snowflake tables |
| Data Foundation | Snowflake + dbt | RAW → CORE transformation; conformed tables as ML input |
| Feature Engineering | Snowpark Python | Pandas-compatible DataFrame API executing natively inside Snowflake compute |
| Preprocessing | Snowpark ML Preprocessing | Scalers, encoders, and imputers that run on Snowflake warehouses |
| Feature Store | Snowpark ML Feature Store | Versioned, reusable feature sets with lineage and point-in-time lookups |
| Model Training | Snowpark ML Modeling | scikit-learn compatible estimators trained entirely inside Snowflake |
| Model Registry | Snowflake Model Registry | Version, tag, and promote models; stores metrics and signatures |
| ML Inference Functions | Cortex ML Functions | No-code FORECAST, ANOMALY DETECTION, CLASSIFICATION via SQL |
| LLM Inference | Cortex LLM Functions | COMPLETE, CLASSIFY, SUMMARIZE, TRANSLATE powered by hosted LLMs |
| Batch Scoring | Snowpark UDFs | Deploy registered models as SQL-callable Python UDFs |
| Applications | Streamlit in Snowflake | First-class Streamlit runtime inside Snowflake; no external hosting needed |

---

## Key Design Decisions

- **Zero data movement for ML.** Snowpark executes Python code inside Snowflake's compute engine directly on warehouse data, eliminating the extract-to-notebook-to-S3 pattern and the security and reproducibility problems it creates.
- **Cortex ML Functions as the default for common patterns.** FORECAST and ANOMALY DETECTION cover the majority of business ML use cases (demand forecasting, churn signals, spend anomalies) with no model training required, dramatically reducing time-to-value.
- **Snowflake Model Registry as the governance layer.** All trained models are registered with version metadata, evaluation metrics, and promotion status, creating a single auditable record of which model version is serving production predictions.
- **Streamlit in Snowflake for application delivery.** Deploying Streamlit apps directly inside Snowflake means business users access ML predictions through a governed, access-controlled application without any data leaving the platform or requiring external web infrastructure.
- **LLM access via Cortex to avoid shadow AI.** Routing document analysis, summarisation, and classification through Cortex LLM Functions keeps unstructured data inside the Snowflake security perimeter rather than sending sensitive content to external LLM APIs.

---

## When to Choose This Implementation

- The organisation wants to operationalise ML and AI on existing Snowflake data without standing up a separate MLOps platform (SageMaker, Vertex AI, Azure ML), reducing infrastructure sprawl and security surface area.
- The primary ML workloads are tabular — forecasting, classification, anomaly detection, regression — where Snowpark ML and Cortex ML Functions cover the full model lifecycle without custom deep learning infrastructure.
- Business teams need interactive AI applications (document Q&A, prediction explorers, data chatbots) built on governed enterprise data, and Streamlit in Snowflake provides a faster, more secure delivery path than external web application hosting.
- Compliance requirements make it unacceptable to exfiltrate training data to external cloud ML services; keeping all computation inside Snowflake satisfies data residency and access control requirements without additional controls.

---

## Trade-offs

| Strength | Limitation |
|----------|------------|
| No data ever leaves Snowflake — security perimeter, governance, and access control apply uniformly to ML workloads | Snowpark ML supports scikit-learn compatible algorithms only; deep learning, graph neural networks, or custom PyTorch/TensorFlow training require an external compute tier |
| Cortex ML Functions deliver production forecasting and anomaly detection with a single SQL call and no model training expertise required | Cortex LLM models are Snowflake-hosted; organisations requiring specific model versions or open-weight LLMs (Llama fine-tunes) have limited control over the underlying model |
| Snowflake Model Registry provides built-in model governance, versioning, and promotion workflows out of the box | Snowpark warehouse compute for ML training is billed in Snowflake credits, which can become expensive for large-scale iterative training jobs compared to dedicated GPU instances |
| Streamlit in Snowflake eliminates the need for external application hosting, authentication integration, and data API layers | Streamlit in Snowflake is less customisable than a standalone web application; complex UI requirements or mobile-first experiences still require external hosting |
| Feature Store with point-in-time lookups prevents training-serving skew without additional infrastructure | Snowflake is not a real-time inference server; sub-100ms online inference latency requires exporting models to an external serving layer |
