# 🍙 Igloo

Igloo is a Rust-based data caching layer that works with Trino and Iceberg. It queries data from Trino, caches the results locally, and keeps the cache up-to-date using Change Data Capture (CDC) stored in Apache Iceberg format.

## 🧩 Architecture

- ✅ **Query Engine**: Trino (SQL layer)
- 💾 **Cache Layer**: Embedded (Sled / RocksDB) or remote
- 🔄 **CDC Sync**: Listens for changes and updates cache
- 📦 **Data Format**: Apache Iceberg (on S3/HDFS)

## 🦀 Simple Rust Project Structure

```
igloo/
├── src/
│   ├── main.rs         # Entry point
│   ├── query_engine.rs # Trino client
│   ├── cache_layer.rs  # Cache logic
│   └── cdc_sync.rs     # CDC listener (Iceberg)
├── Cargo.toml          # Rust dependencies
└── README.md
```

## 🏗️ Example Code

```rust
// main.rs
mod query_engine;
mod cache_layer;
mod cdc_sync;

use query_engine::TrinoClient;
use cache_layer::Cache;
use cdc_sync::CdcListener;

fn main() {
    let trino = TrinoClient::new("http://localhost:8080");
    let mut cache = Cache::new();
    let cdc = CdcListener::new("s3://my-bucket/iceberg-cdc");

    let query = "SELECT * FROM my_table WHERE user_id = 42";
    if let Some(result) = cache.get(query) {
        println!("Cache hit: {:?}", result);
    } else {
        let result = trino.query(query);
        cache.set(query, &result);
        println!("Cache miss, fetched from Trino: {:?}", result);
    }

    cdc.sync(&mut cache);
}
```

---

## 🚀 Usage

```bash
# Run igloo (example)
cargo run -- query "SELECT * FROM my_table WHERE user_id = 42"
```

---

- **Trino** is used as the query engine.
- **Cache** stores query results for fast access.
- **CDC** (Change Data Capture) in **Iceberg** format keeps the cache up-to-date.

This is a minimal structure. Extend each module for real-world use (networking, async, error handling, etc.).