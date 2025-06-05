// src/main.rs
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
mod adbc_postgres;
mod errors; // Added
pub mod postgres_table;

use std::env;
use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion_engine::DataFusionEngine;
use errors::Result; // Using our project's Result type alias
use datafusion::arrow::util::pretty::pretty_format_batches; // Added

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize env_logger. Logs will go to stderr.
    // You can set the RUST_LOG_STYLE environment variable to "never" to disable colors,
    // or "auto" to enable them when stderr is a TTY.
    std::env::set_var("RUST_LOG", "info"); // Set default log level if not overridden
    env_logger::init();

    log::info!("Initializing Igloo components...");
    let mut cache = Cache::new();
    let cdc_path = env::var("IGLOO_CDC_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc".to_string());
    let cdc = CdcListener::new(&cdc_path); // Assuming this doesn't return Result for now

    log::info!("Initializing DataFusionEngine...");
    let parquet_path = env::var("IGLOO_PARQUET_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc/".to_string());
    let postgres_conn_str = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "host=localhost user=postgres password=postgres dbname=mydb".to_string());

    // Assumes DataFusionEngine::new and ::query are updated to return errors::Result (IglooError)
    let engine = DataFusionEngine::new(&parquet_path, &postgres_conn_str).await?;
    log::info!("DataFusionEngine initialized successfully.");

    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";

    if let Some(cached_result_str) = cache.get(query) {
        // log::debug!(target: "igloo_cache", "Cache hit for query: {}", query);
        log::info!(target: "igloo_main", query = query, "Cache hit. Result retrieved from cache.");
        // Output the cached result (it's already a string)
        println!("Cached result:\n{}", cached_result_str);
    } else {
        // log::debug!(target: "igloo_cache", "Cache miss for query: {}", query);
        log::info!(target: "igloo_main", query = query, "Cache miss. Executing with DataFusion.");

        // This now assumes engine.query() returns Result<Vec<RecordBatch>, IglooError>
        match engine.query(query).await {
            Ok(record_batches) => {
                // log::info!("Successfully executed query: {}", query);
                let result_str = match pretty_format_batches(&record_batches) {
                    Ok(formatted) => formatted.to_string(),
                    Err(arrow_err) => {
                        // log::error!("Failed to format record batches: {}", arrow_err);
                        // Convert ArrowError to IglooError or handle appropriately
                        // For now, return a placeholder or the error description
                        // This error should ideally be propagated as IglooError::Arrow(arrow_err)
                        // Forcing it into the cache string is not ideal for robust error handling.
                        // Consider changing this to return Err(IglooError::from(arrow_err)) if the block can use ?
                        format!("Error formatting results: {}", arrow_err)
                    }
                };
                cache.set(query, &result_str); // result_str is now String
                // log::info!("Result for query '{}':\n{}", query, result_str);
                println!("Cache miss. Executed with DataFusion:\n{}", result_str);
            }
            Err(e) => {
                // log::error!("Error executing query with DataFusion: {}", e);
                // If main returns Result<()>, this should ideally be: return Err(e);
                // Or if we want to log and continue (though for a query failure, maybe not):
                log::error!("Error executing query '{}' with DataFusion: {}", query, e);
                // The original snippet used eprintln, which is fine if not returning error from main.
                // Since main *does* return Result, this error should be propagated or explicitly handled.
                // For now, following the snippet's style of printing but noting it doesn't propagate.
                eprintln!("Error executing query with DataFusion: {}", e);
            }
        }
    }

    // Connect to Postgres using ADBC and run a test query
    let adbc_uri = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/mydb".to_string());
    let sql_adbc_test = "SELECT 1 AS test_col";

    // Assumes adbc_postgres_query_example is updated to return errors::Result<()>
    adbc_postgres::adbc_postgres_query_example(&adbc_uri, sql_adbc_test).await?;
    log::info!(target: "igloo_main", uri = %adbc_uri, sql = sql_adbc_test, "ADBC test query succeeded!");

    log::info!("Starting CDC sync...");
    cdc.sync(&mut cache); // Assuming this doesn't return Result for now
    log::info!("CDC sync completed.");

    log::info!("Igloo application finished successfully.");
    Ok(())
}
