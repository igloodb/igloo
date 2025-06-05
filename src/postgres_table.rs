// src/postgres_table.rs
use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, GenericBinaryArray,
    Int16Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
}; // Removed ArrayBuilder
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::sync::Arc;
use adbc_core::driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver};
use adbc_core::options::{AdbcVersion, OptionDatabase};
use arrow::record_batch::RecordBatchReader; // For collect()
use log; // Ensure log is imported for log::debug, log::warn

use crate::errors::{IglooError, Result as IglooResult, AdbcCore}; // Project error types

// Represents a table physically stored in PostgreSQL
#[derive(Debug)]
pub struct PostgresTable {
    adbc_uri: String,
    table_name: String,
    schema: SchemaRef,
}

impl PostgresTable {
    // Constructor that attempts to connect and stores the client
    pub fn try_new(conn_str: &str, table_name: &str, schema: SchemaRef) -> IglooResult<Self> {
        Ok(Self {
            adbc_uri: conn_str.to_string(),
            table_name: table_name.to_string(),
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for PostgresTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionContext,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };

        let selected_field_names: Vec<String> = projected_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let sql_select_cols = if selected_field_names.is_empty() {
            "*".to_string()
        } else {
            selected_field_names
                .iter()
                .map(|name| format!("\"{}\"", name))
                .collect::<Vec<String>>()
                .join(", ")
        };

        let query = format!("SELECT {} FROM \"{}\"", sql_select_cols, self.table_name);
        log::debug!("Executing ADBC scan query on Postgres: {}", query);

        let mut driver = ManagedDriver::load_dynamic_from_name(
            "adbc_driver_postgresql",
            None,
            AdbcVersion::V110,
        ).map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        let mut db = driver.new_database_with_opts(vec![(OptionDatabase::Uri, self.adbc_uri.as_str().into())])
            .map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        let mut conn = db.new_connection()
            .map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        let mut statement = conn.new_statement()
            .map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        statement.set_sql_query(&query)
            .map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        let (_arrow_schema_from_adbc, mut reader, _rows_affected) = statement.execute_query()
            .map_err(|e| DataFusionError::External(Box::new(IglooError::AdbcCore(e))))?;

        let batches: Vec<RecordBatch> = reader.collect()
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        Ok(Arc::new(MemoryExec::try_new(&[batches], projected_schema, None)?))
    }
}
