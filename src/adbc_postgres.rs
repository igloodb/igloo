// src/adbc_postgres.rs
use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::{AdbcVersion, OptionDatabase};
use adbc_core::{Connection as AdbcConnectionTrait, Database as AdbcDatabaseTrait, Statement as AdbcStatementTrait, Driver};
use arrow::array::{
    Array, Int32Array, Float64Array, StringArray, TimestampNanosecondArray,
    BooleanArray, Date32Array, GenericBinaryArray
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::{RecordBatch, RecordBatchReader}; // RecordBatchReader is a trait now

// Using our project's error types
use crate::errors::{Result, IglooError};

pub async fn adbc_postgres_query_example(uri: &str, sql: &str) -> Result<()> {
    log::info!(target: "adbc_example", "Attempting ADBC query. URI: {}, SQL: {}", uri, sql);

    // Load the Postgres ADBC driver dynamically
    // Ensure the .so/.dylib/.dll is in your LD_LIBRARY_PATH/DYLD_LIBRARY_PATH/PATH
    // Using V110 which is common for newer drivers.
    let mut driver = ManagedDriver::load_dynamic_from_name("adbc_driver_postgresql", None, AdbcVersion::V110)?;

    let opts = [(OptionDatabase::Uri, uri.into())];
    let mut database = driver.new_database_with_opts(opts)?;

    let mut connection = database.new_connection()?;
    log::info!(target: "adbc_example", "ADBC Connection established to URI: {}", uri);

    let mut statement = connection.new_statement()?;
    statement.set_sql_query(sql)?;

    // For adbc_core 0.6.0 and later, execute_query returns a tuple
    let (_schema, mut reader, _rows_affected) = statement.execute_query()?;
    // If using an older version (e.g. 0.3.0), it might be:
    // let mut reader = statement.execute()?;

    log::info!(target: "adbc_example", "ADBC statement executed. Reading results for SQL: {}", sql);

    // The reader is an AdbcRecordBatchReader which itself is a RecordBatchReader (iterator)
    let collected_batches_result: std::result::Result<Vec<RecordBatch>, arrow::error::ArrowError> = reader.collect();

    match collected_batches_result {
        Ok(collected_batches) => {
            if collected_batches.is_empty() {
                log::info!(target: "adbc_example", "Query returned no record batches.");
            } else {
                log::info!(target: "adbc_example", "Query returned {} record batch(es).", collected_batches.len());
                for (i, batch) in collected_batches.iter().enumerate() {
                    log::info!(target: "adbc_example", "Printing Batch {}:", i + 1);
                    print_arrow_batch(batch)?;
                }
            }
        }
        Err(arrow_error) => {
            log::error!(target: "adbc_example", "Failed to collect record batches: {}", arrow_error);
            return Err(IglooError::Arrow(arrow_error));
        }
    }

    // According to ADBC spec, statement, connection, and database should be closed.
    // This happens when they go out of scope due to RAII (drop trait implementation).
    // Explicit close calls are available if needed for more precise resource management.
    // statement.close()?;
    // connection.close()?;
    // database.close()?;

    log::info!(target: "adbc_example", "ADBC query example completed successfully for SQL: {}", sql);
    Ok(())
}

fn print_arrow_batch(batch: &RecordBatch) -> Result<()> {
    if batch.num_rows() == 0 {
        log::info!(target: "adbc_print", "Batch is empty."); // Changed from debug to info for empty batch
        return Ok(());
    }
    let schema = batch.schema();
    log::debug!(target: "adbc_print", "--- Batch ({} rows) ---", batch.num_rows());

    // Print column headers
    let header_line: String = schema.fields().iter()
        .map(|field| format!("{} ({})", field.name(), field.data_type()))
        .collect::<Vec<String>>()
        .join(" | ");
    log::debug!(target: "adbc_print", "{}", header_line);

    // Print rows
    for row_idx in 0..batch.num_rows() {
        let mut row_str = String::new();
        for col_idx in 0..batch.num_columns() {
            let array = batch.column(col_idx);
            if array.is_null(row_idx) {
                row_str.push_str("NULL");
            } else {
                let data_type = array.data_type(); // Get data_type from array itself
                match data_type {
                    DataType::Int32 => row_str.push_str(&format!("{}", array.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx))),
                    DataType::Float64 => row_str.push_str(&format!("{}", array.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx))),
                    DataType::Utf8 => row_str.push_str(&format!("'{}'", array.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx))),
                    DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                        let val = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row_idx);
                        row_str.push_str(&format!("{}ns{}", val, tz_opt.as_ref().map_or("".to_string(), |s| format!(" ({})", s))));
                    }
                    DataType::Boolean => row_str.push_str(&format!("{}", array.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_idx))),
                    DataType::Date32 => row_str.push_str(&format!("{}d", array.as_any().downcast_ref::<Date32Array>().unwrap().value(row_idx))),
                    DataType::Binary => {
                        let val = array.as_any().downcast_ref::<GenericBinaryArray<i32>>().unwrap().value(row_idx);
                        row_str.push_str(&format!("[binary data: {} bytes]", val.len()));
                    }
                    other => {
                        // This addresses the "unsupported data types" part of the plan step
                        log::warn!(target: "adbc_print", "Unsupported data type for printing: {:?}. Displaying as [unsupported]", other);
                        row_str.push_str(&format!("[unsupported: {:?}]", other));
                    }
                }
            }
            if col_idx < batch.num_columns() - 1 {
                row_str.push_str(" | ");
            }
        }
        log::debug!(target: "adbc_print", "{}", row_str);
    }
    log::debug!(target: "adbc_print", "--- End Batch ---");
    Ok(())
}
