# 🍙 Igloo

Igloo is a distributed SQL query engine and intelligent caching layer built in Rust. It uses Apache DataFusion for query execution and Apache Arrow for in-memory representation. Igloo caches query results and keeps them fresh using Change Data Capture (CDC) sourced from Apache Iceberg.


## 🧩 Architecture Overview

```mermaid
flowchart LR
    subgraph DataSources["Relational DB Sources"]
        PG["Postgres"]
        MY["MySQL"]
        MSSQL["SQL Server"]
        JDBC["Generic JDBC"]
    end
    PG --> IGLOO
    MY --> IGLOO
    MSSQL --> IGLOO
    JDBC --> IGLOO
    subgraph IGLOO["Igloo"]
        CACHE["Smart Cache"]
        ENGINE["Query Engine (DataFusion)"]
        CDC["CDC Listener"]
    end
    IGLOO --> CACHE
    IGLOO --> ENGINE
    IGLOO --> CDC
    CACHE -- "Fresh/Hot Data" --> ANALYTICS["Real-time Analytics"]
    ENGINE -- "Query Results" --> ANALYTICS
    CDC -- "Change Events" --> CACHE
```

- 🧠 **Query Engine:** Apache DataFusion — Arrow-native SQL execution
- 💾 **Cache Layer:** In-memory cache (MVP), pluggable (e.g., Sled/RocksDB)
- 🔄 **CDC Integration:** Monitors Iceberg CDC streams and invalidates/updates cache entries
- 📦 **Data Format:** Apache Arrow in memory, Parquet on disk (from Iceberg)

## 🏗️ Example Code

```rust
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
pub mod postgres_table;

use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion_engine::DataFusionEngine;
use tokio::runtime::Runtime;

fn main() {
    let mut cache = Cache::new();
    let cdc = CdcListener::new("s3://my-bucket/iceberg-cdc");

    let parquet_path = "./dummy_iceberg_cdc/";
    let postgres_conn = "host=localhost user=postgres password=postgres dbname=mydb";
    let rt = Runtime::new().unwrap();
    let engine = rt.block_on(DataFusionEngine::new(parquet_path, postgres_conn));

    let query = "SELECT i.user_id, i.data, p.extra_info \
                 FROM iceberg i \
                 JOIN pg_table p ON i.user_id = p.user_id \
                 WHERE i.user_id = 42";

    if let Some(result) = cache.get(query) {
        println!("Cache hit:\n{:?}", result);
    } else {
        let result = rt.block_on(engine.query(query));
        cache.set(query, &result);
        println!("Cache miss. Executed with DataFusion:\n{:?}", result);
    }

    // (In production, CDC sync should run asynchronously)
    cdc.sync(&mut cache);
}
```

## 🚀 Quick Start

```sh
# Build and run Igloo
cargo run
```

**Environment Setup**

Ensure your local Parquet/Iceberg and Postgres paths are accessible in your config or .env.

## ✅ Features

- ⚡ Fast SQL Execution with Apache DataFusion
- 🧊 Smart Result Caching using query fingerprints
- 🔄 CDC-Driven Invalidation from Iceberg logs
- 🔌 Join Support for Postgres + Arrow datasets
- 🧪 Designed for extensibility (remote cache, metrics, etc.)

## 🔮 Roadmap

- ⏱️ Async CDC updates & live cache refresh
- 🌐 REST or gRPC query API
- 🧠 Query planner-aware caching
- 📊 Metrics (e.g., Prometheus, OpenTelemetry)
- 📦 Optional persistent cache backend (e.g., RocksDB, Redis)


## 🤝 Contributing

Contributions, suggestions, and PRs are welcome! See CONTRIBUTING.md for more details.