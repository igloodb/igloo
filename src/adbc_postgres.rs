// Arrow ADBC Postgres connection and query MVP in Rust
// NOTE: The Rust ADBC implementation does not provide a native Postgres driver crate.
// This is a placeholder showing how you might use the generic ADBC API if/when a driver is available.
// For now, this will not work out-of-the-box for Postgres, but demonstrates the intended structure.

// Enable the driver_manager feature for adbc_core in Cargo.toml:
// adbc_core = { version = "0.18.0", features = ["driver_manager"] }

use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::Connection as AdbcConnectionTrait;
use adbc_core::Database as AdbcDatabaseTrait;
use adbc_core::Statement as AdbcStatementTrait;
use adbc_core::Driver;
use arrow::array::{Array, StringArray};
use arrow::record_batch::{RecordBatch, RecordBatchReader};

pub async fn adbc_postgres_query_example(uri: &str, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Load the Postgres ADBC driver dynamically (ensure the .so/.dll is in your library path)
    let mut driver = ManagedDriver::load_dynamic_from_name("adbc_driver_postgresql", None, AdbcVersion::V100)?;
    let opts = [(OptionDatabase::Uri, uri.into())];
    let mut database = driver.new_database_with_opts(opts)?;
    let mut connection = database.new_connection()?;
    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;
    let reader = statement.execute()?;
    let batches: Result<Vec<RecordBatch>, _> = reader.collect();
    for batch in batches? {
        print_arrow_batch(&batch);
    }
    Ok(())
}

fn print_arrow_batch(batch: &RecordBatch) {
    for col in 0..batch.num_columns() {
        let array = batch.column(col);
        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            for i in 0..string_array.len() {
                let value = string_array.value(i);
                print!("{}\t", value);
            }
            println!();
        }
    }
}
