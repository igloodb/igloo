// src/main.rs

mod query_engine;
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
mod adbc_postgres;

pub mod postgres_table;

use std::env;
use std::error::Error;
use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion_engine::DataFusionEngine;
use tracing::{info, error, Level};
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    // Initialize components
    info!("Initializing Igloo components...");
    let mut cache = Cache::new();
    let cdc_path = env::var("IGLOO_CDC_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc".to_string());
    let cdc = CdcListener::new(&cdc_path);

    // DataFusion setup (async)
    let parquet_path = env::var("IGLOO_PARQUET_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc/".to_string());
    let postgres_conn_str = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "host=localhost user=postgres password=postgres dbname=mydb".to_string());
    let engine = DataFusionEngine::new(&parquet_path, &postgres_conn_str).await
        .map_err(|e| {
            error!(parquet_path = %parquet_path, postgres_conn = %postgres_conn_str, error = %e, "Failed to initialize DataFusionEngine");
            e
        })?;
    info!("DataFusionEngine initialized successfully.");

    // Example query: join between iceberg and postgres
    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";
    if let Some(_result) = cache.get(query) { // _result is &String, not logging its content for brevity
        info!(query = query, "Cache hit. Result retrieved from cache.");
    } else {
        match engine.query(query).await {
            Ok(result_str) => {
                cache.set(query, &result_str);
                info!(query = query, "Cache miss. Executed with DataFusion.");
                // Optionally log a snippet of result_str if needed, e.g.:
                // info!(query = query, result_preview = %result_str.chars().take(100).collect::<String>(), "Cache miss. Executed with DataFusion.");
            }
            Err(e) => {
                error!(query = query, error = %e, "Failed to execute DataFusion query");
                // This error is not returned from main, program continues.
            }
        }
    }

    // Connect to Postgres using ADBC and run a test query (using Rust-native adbc_core)
    let adbc_uri = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/mydb".to_string());
    let sql = "SELECT 1 AS test_col";
    adbc_postgres::adbc_postgres_query_example(&adbc_uri, sql).await
        .map_err(|e| {
            error!(uri = %adbc_uri, sql = sql, error = %e, "ADBC test query failed");
            e
        })?;
    info!(uri = %adbc_uri, sql = sql, "ADBC test query succeeded!");

    // Start CDC sync (in real app, this would be async/threaded)
    info!("Starting CDC sync...");
    cdc.sync(&mut cache);
    info!("CDC sync completed.");

    info!("Igloo application finished successfully.");
    Ok(())
}
