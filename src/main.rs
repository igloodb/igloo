// src/main.rs

mod query_engine;
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
mod adbc_postgres;

pub mod postgres_table;

use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion_engine::DataFusionEngine;

#[tokio::main]
async fn main() {
    // Initialize components
    let mut cache = Cache::new();
    let cdc = CdcListener::new("./dummy_iceberg_cdc");

    // DataFusion setup (async)
    let parquet_path = "./dummy_iceberg_cdc/"; // Iceberg/Parquet dir
    let postgres_conn = "host=localhost user=postgres password=postgres dbname=mydb";
    let engine = DataFusionEngine::new(parquet_path, postgres_conn).await;

    // Example query: join between iceberg and postgres
    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";
    if let Some(result) = cache.get(query) {
        println!("Cache hit: {:?}", result);
    } else {
        let result = engine.query(query).await;
        cache.set(query, &result);
        println!("Cache miss, executed with DataFusion: {:?}", result);
    }

    // Connect to Postgres using ADBC and run a test query (using Rust-native adbc_core)
    let uri = "postgres://postgres:postgres@localhost:5432/mydb";
    let sql = "SELECT 1 AS test_col";
    match adbc_postgres::adbc_postgres_query_example(uri, sql).await {
        Ok(_) => println!("ADBC test query succeeded!"),
        Err(e) => eprintln!("ADBC test query failed: {}", e),
    }

    // Start CDC sync (in real app, this would be async/threaded)
    cdc.sync(&mut cache);
}
