// Arrow ADBC Postgres connection and query MVP in Rust
use adbc_driver_postgresql::Connection;
use adbc_core::{ConnectionOptions, Statement};
use arrow::record_batch::RecordBatch;
use arrow::array::{StringArray, Array};
use std::sync::Arc;

pub async fn adbc_postgres_query_example(uri: &str, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Set up connection options
    let mut options = ConnectionOptions::default();
    options.set_uri(uri);
    let mut conn = Connection::new();
    conn.connect(&options).await?;
    println!("Connected to Postgres via Arrow ADBC!");

    // Prepare and execute query
    let mut stmt = Statement::new(&mut conn);
    stmt.set_sql_query(sql);
    let mut reader = stmt.execute_query().await?;

    // Fetch and print results
    while let Some(batch) = reader.next().await? {
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
