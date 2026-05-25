---
layout: default
title: "db.2 — Databricks · Streaming and Real-Time"
---

# db.2 — Databricks · Streaming and Real-Time

**Stack:** Kafka · Databricks Structured Streaming · Delta Live Tables · Delta Lake
**Processing:** Real-Time Event-Driven · Sub-Minute Latency · Continuous Pipelines
**Buy vs Build:** Buy (managed Kafka, managed Databricks) + Build (streaming DLT pipelines, custom aggregations)

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph SRC["Event Sources"]
        S1[Application Events\nmobile · web · microservices]
        S2[IoT Sensors\nMQTT · telemetry devices]
        S3[CDC Streams\nDebezium · DMS]
        S4[Clickstream\nweb analytics events]
        S5[Machine Logs\ninfrastructure · APM]
    end

    subgraph KAFKA["Message Bus — Apache Kafka"]
        K1[Kafka Topics\nraw-events · cdc-events · iot-events]
        K2[Schema Registry\nAvro · Protobuf schema enforcement]
        K3[Kafka Connect\nDebezium source connectors]
    end

    subgraph STREAMING["Databricks Structured Streaming — DLT"]
        P1[Bronze Streaming Table\nkafka readStream · append-only]
        P2[Silver Streaming Table\nDLT expectations · stateful dedup]
        P3[Gold Streaming Table\nwindowed aggregations · watermarks]
        P4[DLT Continuous Mode\nauto-recovery · checkpoint management]
    end

    subgraph STORAGE["Delta Lake — Streaming Medallion"]
        Z1[BRONZE Delta\nraw events · exact-once · ACID]
        Z2[SILVER Delta\ncleansed · entity-resolved · keyed]
        Z3[GOLD Delta\nreal-time aggregates · time windows]
    end

    subgraph SERVE["Serving Layer"]
        C1[Databricks SQL Warehouse\nnear-real-time BI queries]
        C2[Delta Live Tables\npublished streaming tables]
        C3[Downstream Kafka\nprocessed event re-publish]
        C4[REST API\nLakehouse Federation · external apps]
    end

    subgraph GOVERN["Unity Catalog"]
        G1[Streaming Table Registry\nlineage · schema history]
        G2[Data Quality Metrics\nDLT expectations dashboard]
    end

    SRC --> K3 --> K1
    K2 -. validate schema .-> K1
    K1 --> P1 --> Z1
    Z1 --> P2 --> Z2
    Z2 --> P3 --> Z3
    P4 -. manage .-> P1 & P2 & P3
    Z1 & Z2 & Z3 -. register .-> G1
    G2 -. monitor .-> P2 & P3
    Z3 --> C1
    Z3 --> C2
    Z3 --> C3
    Z2 --> C4
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Producers
        A1[Mobile App\nevents JSON]
        A2[IoT Device\ntelemetry Avro]
        A3[Postgres CDC\nDebezium]
    end

    subgraph Kafka
        B1[Topic: raw-app-events]
        B2[Topic: raw-iot-telemetry]
        B3[Topic: cdc-postgres-orders]
        B4[Schema Registry\nAvro schemas]
    end

    subgraph DLT_Pipeline["DLT Continuous Pipeline"]
        C1[Bronze Streaming Table\nreadStream from Kafka\nappend-only · checkpoint]
        C2[Silver Streaming Table\nDLT EXPECT constraints\ndedup on event-id · MERGE]
        C3[Gold Streaming Table\ntumbling window 5-min aggs\nwatermark 10 min late data]
    end

    subgraph Delta["Delta Lake"]
        D1[bronze.raw_events]
        D2[silver.clean_events]
        D3[gold.event_aggregates]
    end

    subgraph Serve
        E1[Databricks SQL\nnear-real-time dashboard]
        E2[Kafka Topic\nprocessed-events]
        E3[Operational App\nREST read via JDBC]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    B4 -. schema enforce .-> B1 & B2 & B3

    B1 & B2 & B3 --> C1
    C1 -->|expectations\n+ dedup| C2
    C2 -->|window agg\nwatermark| C3

    C1 --> D1
    C2 --> D2
    C3 --> D3

    D3 --> E1
    D3 --> E2
    D2 --> E3
```

---

## Component Breakdown

| Layer | Tool | Role |
|-------|------|------|
| Event Sources | Application services, IoT, Debezium | Publish events to Kafka topics via producers or CDC connectors |
| Message Bus | Apache Kafka (Confluent Cloud / MSK) | Durable, ordered, partitioned event log; decouples producers from consumers |
| Schema Enforcement | Confluent Schema Registry | Avro / Protobuf schema validation at produce time; prevents schema drift |
| Bronze Streaming | Databricks Structured Streaming + DLT | `readStream` from Kafka; exactly-once write to Delta Bronze with checkpointing |
| Silver Streaming | Delta Live Tables — streaming table | DLT EXPECT constraints; stateful dedup with MERGE on event key |
| Gold Streaming | Delta Live Tables — streaming table | Tumbling and sliding window aggregations with watermark for late data |
| Storage | Delta Lake | ACID streaming writes across all medallion zones; queryable while streaming |
| Pipeline Management | DLT Continuous Mode | Auto-restart on failure, checkpoint recovery, backpressure handling |
| Governance | Unity Catalog | Streaming table registration, schema history, and column-level lineage |
| Data Quality | DLT Expectations Dashboard | Real-time pass / quarantine / fail metrics per constraint per pipeline |
| BI Serving | Databricks SQL Warehouses | Query Gold Delta tables with second-level staleness for dashboards |
| Re-publish | Kafka Sink Connector | Write processed Gold events back to Kafka for downstream microservices |

---

## Key Design Decisions

- **DLT Continuous Mode over manual Structured Streaming jobs:** Delta Live Tables manages checkpointing, auto-recovery, and backpressure automatically — eliminating the operational overhead of hand-managed `StreamingQuery` objects and restart logic.
- **Exactly-once semantics via Delta Lake + idempotent Kafka offsets:** Structured Streaming writes Delta transaction log entries atomically with Kafka offset commits, guaranteeing exactly-once end-to-end without custom deduplication at the Kafka layer.
- **Watermarks for late-arriving data:** Gold aggregations define a watermark (typically 10–30 minutes) to correctly handle out-of-order events from mobile or IoT producers without holding unbounded state in memory.
- **Schema Registry enforcement at ingest:** Validating Avro/Protobuf schemas at produce time prevents malformed events from reaching the Bronze table, where schema corruption is expensive to remediate in streaming pipelines.
- **Separate DLT pipelines per domain:** Running independent DLT pipelines per event domain (user events, IoT, CDC) prevents a single slow or failed stream from stalling unrelated pipelines and allows independent scaling.

---

## When to Choose This Implementation

- Business requirements demand sub-minute data freshness — fraud detection, operational dashboards, IoT alerting, or real-time personalisation where batch-hourly latency is not acceptable.
- The organisation already operates Kafka as an enterprise event bus, and Databricks is the chosen compute platform — avoiding a second streaming engine (Flink, Spark Standalone) reduces operational surface area.
- Event volumes are high and irregular; Structured Streaming's auto-scaling on Databricks handles burst traffic without manual cluster resizing.
- You need streaming and batch in a single pipeline framework — DLT supports both `readStream` and batch reads in the same DAG, enabling a unified Bronze-to-Gold medallion without maintaining two separate codebases.

---

## Trade-offs

| Strength | Limitation |
|----------|------------|
| DLT Continuous Mode provides hands-off checkpoint management and auto-recovery | DLT pipelines are harder to debug than plain Structured Streaming; internal DAG execution is a black box |
| Exactly-once Delta writes eliminate duplicates without application-level dedup logic | Exactly-once requires idempotent Kafka consumers and careful offset management — misconfiguration causes silent data gaps |
| Watermark-based late-data handling keeps Gold aggregations correct without unbounded state | Watermark tuning is workload-specific; too tight loses late events, too loose inflates state memory and delays results |
| Unified medallion architecture works for both streaming and batch — one mental model for all engineers | Streaming Delta tables accumulate many small files; compaction jobs must run alongside continuous pipelines to avoid query performance degradation |
| DLT expectations provide built-in data quality monitoring with zero extra infrastructure | Failed expectation rows are quarantined in a separate table — consumers must be aware of and periodically reconcile quarantine data |
