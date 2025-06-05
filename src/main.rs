// src/main.rs
mod adbc_postgres;
mod cache_layer;
mod cdc_sync;
mod compute; // Ensure this is present
mod datafusion_engine;
mod errors; // Added
pub mod postgres_table;

use crate::errors::Result; // Using our project's Result type alias
use cache_layer::Cache;
use cdc_sync::CdcListener;
use compute::{ComputeService, FunctionRegistry, LocalTaskScheduler};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion_engine::DataFusionEngine;
use serde::{Deserialize, Serialize}; // For our sample function args/ret
use std::env; // Added
use std::time::Duration; // For simulating work
use tokio::time::sleep; // For simulating work

// Define sample functions for demonstration
#[derive(Serialize, Deserialize, Debug)]
struct AddArgs {
    a: i32,
    b: i32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)] // Added PartialEq for assertion
struct AddResult {
    value: i32,
}

fn actual_add_function(args: AddArgs) -> AddResult {
    log::info!("actual_add_function called with a={}, b={}", args.a, args.b);
    AddResult {
        value: args.a + args.b,
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ComplexTaskArgs {
    name: String,
    delay_ms: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct ComplexTaskResult {
    greeting: String,
}

// Synchronous version of actual_complex_task for the demo wrapper:
fn actual_complex_task_sync(
    args: ComplexTaskArgs,
) -> std::result::Result<ComplexTaskResult, compute::ComputeError> {
    log::info!(
        "actual_complex_task_sync '{}' called, will 'sleep' for {}ms",
        args.name,
        args.delay_ms
    );
    // Simulate blocking sleep for sync demo
    std::thread::sleep(Duration::from_millis(args.delay_ms));
    if args.name == "fail_me" {
        log::warn!("actual_complex_task_sync '{}' is about to fail.", args.name);
        Err(compute::ComputeError::ExecutionFailed(
            "Task failed as per 'fail_me' input".to_string(),
        ))
    } else {
        log::info!("actual_complex_task_sync '{}' completed.", args.name);
        Ok(ComplexTaskResult {
            greeting: format!("Hello, {} from sync task!", args.name),
        })
    }
}

async fn run_compute_layer_demo() -> std::result::Result<(), compute::ComputeError> {
    log::info!("--- Starting Compute Layer Demo ---");

    // 1. Instantiate LocalTaskScheduler
    let scheduler = LocalTaskScheduler::new(4, 100);
    log::info!("LocalTaskScheduler initialized with 4 workers.");

    // 2. Define and wrap functions
    let add_task_wrapper = FunctionRegistry::wrap_infallible_fn(actual_add_function);
    let complex_task_wrapper = FunctionRegistry::wrap_fn(actual_complex_task_sync);

    // 3. Register functions
    scheduler
        .register_function("add".to_string(), add_task_wrapper)
        .await?;
    log::info!("'add' function registered.");
    scheduler
        .register_function("complex_task".to_string(), complex_task_wrapper)
        .await?;
    log::info!("'complex_task' function registered.");

    // 4. Submit tasks
    log::info!("Submitting 'add_task_1' (5 + 10)...");
    let add_args1 = AddArgs { a: 5, b: 10 };
    let future1: compute::TaskFuture<AddResult> =
        scheduler.submit("add".to_string(), add_args1).await?;
    log::info!("'add_task_1' submitted with ID: {}", future1.task_id());

    log::info!("Submitting 'complex_task_1' (Alice, 500ms)...");
    let complex_args1 = ComplexTaskArgs {
        name: "Alice".to_string(),
        delay_ms: 500,
    };
    let future2: compute::TaskFuture<ComplexTaskResult> = scheduler
        .submit("complex_task".to_string(), complex_args1)
        .await?;
    log::info!("'complex_task_1' submitted with ID: {}", future2.task_id());

    log::info!("Submitting 'add_task_2' (33 + 77)...");
    let add_args2 = AddArgs { a: 33, b: 77 };
    let future3: compute::TaskFuture<AddResult> =
        scheduler.submit("add".to_string(), add_args2).await?;
    log::info!("'add_task_2' submitted with ID: {}", future3.task_id());

    log::info!("Submitting 'complex_task_fail' (fail_me, 100ms)...");
    let complex_args_fail = ComplexTaskArgs {
        name: "fail_me".to_string(),
        delay_ms: 100,
    };
    let future_fail: compute::TaskFuture<ComplexTaskResult> = scheduler
        .submit("complex_task".to_string(), complex_args_fail)
        .await?;
    log::info!(
        "'complex_task_fail' submitted with ID: {}",
        future_fail.task_id()
    );

    // 5. Await results and verify
    log::info!("Awaiting results...");

    let result1 = future1.await?;
    log::info!(
        "Result for 'add_task_1': {:?}. Expected: AddResult {{ value: 15 }}",
        result1
    );
    assert_eq!(result1, AddResult { value: 15 });

    let result2 = future2.await?;
    log::info!("Result for 'complex_task_1': {:?}", result2);
    assert_eq!(
        result2,
        ComplexTaskResult {
            greeting: "Hello, Alice from sync task!".to_string()
        }
    );

    let result3 = future3.await?;
    log::info!(
        "Result for 'add_task_2': {:?}. Expected: AddResult {{ value: 110 }}",
        result3
    );
    assert_eq!(result3, AddResult { value: 110 });

    log::info!("Awaiting 'complex_task_fail' (expected to fail)...");
    match future_fail.await {
        Ok(res) => {
            log::error!("'complex_task_fail' succeeded unexpectedly: {:?}", res);
            return Err(compute::ComputeError::Unknown); // Should have failed
        }
        Err(e) => {
            log::info!("'complex_task_fail' failed as expected: {}", e);
            if let compute::ComputeError::ExecutionFailed(msg) = e {
                assert!(msg.contains("Task failed as per 'fail_me' input"));
            } else {
                log::error!(
                    "'complex_task_fail' failed with unexpected error type: {}",
                    e
                );
                return Err(e);
            }
        }
    }

    log::info!("--- Compute Layer Demo Finished Successfully ---");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();

    log::info!("Initializing Igloo components...");
    let mut cache = Cache::new();
    let cdc_path = env::var("IGLOO_CDC_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc".to_string());
    let cdc = CdcListener::new(&cdc_path);

    log::info!("Initializing DataFusionEngine...");
    let parquet_path =
        env::var("IGLOO_PARQUET_PATH").unwrap_or_else(|_| "./dummy_iceberg_cdc/".to_string());
    let postgres_conn_str = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| {
            "host=localhost user=postgres password=postgres dbname=mydb".to_string()
        });

    let engine = DataFusionEngine::new(&parquet_path, &postgres_conn_str).await?;
    log::info!("DataFusionEngine initialized successfully.");

    let query = "SELECT i.user_id, i.data, p.extra_info FROM iceberg i JOIN pg_table p ON i.user_id = p.user_id WHERE i.user_id = 42";

    if let Some(cached_result_str) = cache.get(query) {
        log::info!(target: "igloo_main", query = query, "Cache hit. Result retrieved from cache.");
        println!("Cached result:\n{}", cached_result_str);
    } else {
        log::info!(target: "igloo_main", query = query, "Cache miss. Executing with DataFusion.");
        match engine.query(query).await {
            Ok(record_batches) => {
                let result_str = match pretty_format_batches(&record_batches) {
                    Ok(formatted) => formatted.to_string(),
                    Err(arrow_err) => {
                        format!("Error formatting results: {}", arrow_err)
                    }
                };
                cache.set(query, &result_str);
                println!("Cache miss. Executed with DataFusion:\n{}", result_str);
            }
            Err(e) => {
                log::error!("Error executing query '{}' with DataFusion: {}", query, e);
                eprintln!("Error executing query with DataFusion: {}", e);
            }
        }
    }

    let adbc_uri = env::var("DATABASE_URL")
        .or_else(|_| env::var("IGLOO_POSTGRES_URI"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/mydb".to_string());
    let sql_adbc_test = "SELECT 1 AS test_col";

    adbc_postgres::adbc_postgres_query_example(&adbc_uri, sql_adbc_test).await?;
    log::info!(target: "igloo_main", uri = %adbc_uri, sql = sql_adbc_test, "ADBC test query succeeded!");

    log::info!("Starting CDC sync...");
    cdc.sync(&mut cache);
    log::info!("CDC sync completed.");

    // Run the compute layer demo
    run_compute_layer_demo().await?;

    log::info!("Igloo application finished successfully.");
    Ok(())
}
