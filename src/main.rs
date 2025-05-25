// src/main.rs

mod query_engine;
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;

pub mod postgres_table;

use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion_engine::DataFusionEngine;
use tokio::runtime::Runtime;

fn main() {
    // Initialize components
    let mut cache = Cache::new();
    let cdc = CdcListener::new("./dummy_iceberg_cdc");

    // DataFusion setup (sync wrapper for async)
    let parquet_path = "./dummy_iceberg_cdc/"; // Iceberg/Parquet dir
    let postgres_conn = "host=localhost user=postgres password=postgres dbname=mydb";
    let rt = Runtime::new().unwrap();
    let engine = rt.block_on(DataFusionEngine::new(parquet_path, postgres_conn));

    // Example query: join between iceberg and postgres
    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";
    if let Some(result) = cache.get(query) {
        println!("Cache hit: {:?}", result);
    } else {
        let result = rt.block_on(engine.query(query));
        cache.set(query, &result);
        println!("Cache miss, executed with DataFusion: {:?}", result);
    }

    // Start CDC sync (in real app, this would be async/threaded)
    cdc.sync(&mut cache);
}
