// src/datafusion_engine.rs
use datafusion::arrow::datatypes::Schema as ArrowSchema; // Alias
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*; // Includes SessionContext, DataFrame, etc. // For query return type

use std::sync::Arc;

use crate::errors::{IglooError, Result}; // Using project's error types
use crate::postgres_table::PostgresTable; // Assuming path is correct

/// Provides a DataFusion query execution engine.
///
/// `DataFusionEngine` is the main entry point for setting up tables (e.g., from Parquet files, PostgreSQL)
/// and executing SQL queries against them using the DataFusion query engine.
pub struct DataFusionEngine {
    pub ctx: SessionContext,
}

impl DataFusionEngine {
    /// Creates a new `DataFusionEngine` and registers tables.
    ///
    /// This constructor initializes a DataFusion `SessionContext` and registers two tables:
    /// 1.  An 'iceberg' table: Reads Parquet files from the specified `parquet_path`.
    ///     It assumes a schema with `user_id` (Int64) and `data` (Utf8).
    /// 2.  A 'pg_table': Connects to a PostgreSQL database using the ADBC driver
    ///     via the `postgres_conn_str` (ADBC URI). It assumes a schema with
    ///     `user_id` (Int64) and `extra_info` (Utf8).
    ///
    /// # Arguments
    ///
    /// * `parquet_path`: Filesystem path to the directory containing Parquet files for the 'iceberg' table.
    /// * `postgres_conn_str`: ADBC connection string (URI) for the PostgreSQL database.
    ///
    /// # Errors
    ///
    /// Returns `IglooError` if there's an issue setting up the tables, such as
    /// path parsing errors, DataFusion errors during table registration, or issues
    /// initializing the `PostgresTable` (e.g., invalid connection string format, though
    /// actual connection attempt is deferred to query time).
    pub async fn new(parquet_path: &str, postgres_conn_str: &str) -> Result<Self> {
        let ctx = SessionContext::new();

        // Define the schema for the Parquet files (Iceberg table)
        // This should match the actual schema of your Parquet files.
        let iceberg_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
        ]));

        // Configure listing options for Parquet
        // Adjust file extension and target partition count as needed.
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_target_partitions(num_cpus::get()); // Use number of CPU cores for partitions

        let table_url = ListingTableUrl::parse(parquet_path)?; // DFError -> IglooError::DataFusion via From trait

        let listing_table_config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(iceberg_schema);

        let iceberg_table = Arc::new(ListingTable::try_new(listing_table_config)?); // DFError -> IglooError::DataFusion
        ctx.register_table("iceberg", iceberg_table)?; // DFError -> IglooError::DataFusion

        // Register PostgresTable
        let pg_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("extra_info", DataType::Utf8, true),
        ]));
        // Ensure PostgresTable::new is compatible with error handling or map its error.
        // Assuming PostgresTable::new does not return a Result for now, or its errors are not handled here.
        // If PostgresTable::new can fail in a way that needs to be an IglooError, it should return Result.
        // Corrected call to use asynchronous try_new:
        let pg_provider = Arc::new(
            PostgresTable::try_new(postgres_conn_str, "my_pg_table", pg_schema.clone())?,
        );
        ctx.register_table("pg_table", pg_provider)?; // DFError -> IglooError::DataFusion

        // log::info!("DataFusion context initialized with Iceberg and Postgres tables.");
        Ok(Self { ctx })
    }

    /// Executes a SQL query against the registered tables.
    ///
    /// # Arguments
    ///
    /// * `sql`: The SQL query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `Vec<RecordBatch>` if the query is successful.
    /// Each `RecordBatch` represents a chunk of the query result in Arrow format.
    ///
    /// # Errors
    ///
    /// Returns `IglooError` if any error occurs during query parsing, planning,
    /// execution, or if there are issues with the underlying data sources (e.g.,
    /// ADBC communication errors with PostgreSQL, file I/O errors for Parquet).
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // log::debug!("Executing SQL query in DataFusion: {}", sql);
        let df = self.ctx.sql(sql).await?; // DFError -> IglooError::DataFusion
        let results = df.collect().await?; // DFError -> IglooError::DataFusion
                                           // log::debug!("Query executed successfully. Number of batches: {}", results.len());
        Ok(results)
    }
}
