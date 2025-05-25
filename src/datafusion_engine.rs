// src/datafusion_engine.rs
use datafusion::prelude::*;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::listing::ListingTableConfig;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::arrow::util::pretty::pretty_format_batches;
use std::sync::Arc;
use super::postgres_table::PostgresTable;
use datafusion::arrow::datatypes::{Field, DataType, Schema};

pub struct DataFusionEngine {
    pub ctx: SessionContext,
}

impl DataFusionEngine {
    pub async fn new(parquet_path: &str, postgres_conn: &str) -> Self {
        let ctx = SessionContext::new();
        // Register Parquet (Iceberg) as a table
        let url = ListingTableUrl::parse(parquet_path).unwrap();
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()));
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(Arc::new(
                datafusion::arrow::datatypes::Schema::new(vec![
                    datafusion::arrow::datatypes::Field::new("user_id", datafusion::arrow::datatypes::DataType::Int64, false),
                    datafusion::arrow::datatypes::Field::new("data", datafusion::arrow::datatypes::DataType::Utf8, true),
                ])
            ));
        let table = datafusion::datasource::listing::ListingTable::try_new(config).unwrap();
        ctx.register_table("iceberg", Arc::new(table)).unwrap();

        // Register Postgres as a custom TableProvider
        let pg_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("extra_info", DataType::Utf8, true),
        ]));
        let pg_provider = Arc::new(PostgresTable::new(postgres_conn, "my_pg_table", pg_schema));
        ctx.register_table("pg_table", pg_provider).unwrap();

        Self { ctx }
    }

    pub async fn query(&self, sql: &str) -> String {
        let df = self.ctx.sql(sql).await.unwrap();
        let results = df.collect().await.unwrap();
        pretty_format_batches(&results).unwrap().to_string()
    }
}
