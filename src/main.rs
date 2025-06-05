// src/main.rs
mod adbc_postgres;
mod cache_layer;
mod cdc_sync;
mod datafusion_engine;
mod errors; // Added
pub mod postgres_table;

use cache_layer::Cache;
use cdc_sync::CdcListener;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion_engine::DataFusionEngine;
use errors::Result; // Using our project's Result type alias
use std::env; // Added

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
    // Configure paths and connection strings for DataFusionEngine.
    // IGLOO_PARQUET_PATH: Path to the directory containing Parquet files for the 'iceberg' table.
    // DATABASE_URL or IGLOO_POSTGRES_URI: ADBC URI for the PostgreSQL connection for the 'pg_table'.
    let parquet_path =
        env::var("IGLOO_PARQUET_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc/".to_string());
    let postgres_conn_str = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| {
            // Default basic libpq-style connection string if not set via environment variables.
            // Note: For ADBC, a URI like "postgresql://user:pass@host:port/dbname" is typically expected.
            // This default might need adjustment if a pure ADBC URI is strictly required by the driver
            // and it doesn't fall back to parsing libpq strings.
            "host=localhost user=postgres password=postgres dbname=mydb".to_string()
        });

    // Initialize the DataFusionEngine. This sets up the 'iceberg' table (from Parquet)
    // and 'pg_table' (connected to PostgreSQL via ADBC).
    let engine = DataFusionEngine::new(&parquet_path, &postgres_conn_str).await?;
    log::info!("DataFusionEngine initialized successfully.");

    // Example query: Joins the Parquet-based 'iceberg' table with the ADBC-backed 'pg_table'.
    // The 'pg_table' now transparently uses ADBC for its data access through DataFusionEngine.
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

    // Direct ADBC Driver Health Check (PostgreSQL)
    // This section demonstrates a direct call to the ADBC PostgreSQL driver,
    // bypassing the DataFusionEngine. It's useful as a standalone test to ensure
    // the ADBC driver itself is correctly configured and can connect to PostgreSQL.
    // Uses the same environment variables (DATABASE_URL or IGLOO_POSTGRES_URI) for the ADBC URI.
    let adbc_uri = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/mydb".to_string()); // Default ADBC URI
    let sql_adbc_test = "SELECT 1 AS test_col";

    // Execute the direct ADBC query example.
    adbc_postgres::adbc_postgres_query_example(&adbc_uri, sql_adbc_test).await?;
    log::info!(target: "igloo_main", uri = %adbc_uri, sql = sql_adbc_test, "Direct ADBC PostgreSQL health check query succeeded!");

    log::info!("Starting CDC sync...");
    cdc.sync(&mut cache); // Assuming this doesn't return Result for now
    log::info!("CDC sync completed.");

    log::info!("Igloo application finished successfully.");
    Ok(())
}
