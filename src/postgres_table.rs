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
use tokio_postgres::{Client, NoTls}; // Assuming NoTls for simplicity

use crate::errors::{IglooError, Result as IglooResult}; // Project error types

// Represents a table physically stored in PostgreSQL
#[derive(Debug, Clone)]
pub struct PostgresTable {
    client: Arc<Client>, // Use Arc for shared ownership if needed, or just Client
    table_name: String,
    schema: SchemaRef,
}

impl PostgresTable {
    // Constructor that attempts to connect and stores the client
    pub async fn try_new(conn_str: &str, table_name: &str, schema: SchemaRef) -> IglooResult<Self> {
        let (client, connection) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .map_err(IglooError::Postgres)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self {
            client: Arc::new(client), // Wrap client in Arc if it's to be shared or if PostgresTable is cloned often
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
        _filters: &[Expr],     // Filters not handled in this iteration
        _limit: Option<usize>, // Limit not handled in this iteration
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
        let sql_select_cols = if selected_field_names.is_empty()
            || selected_field_names.len() == self.schema.fields().len()
        {
            "*".to_string() // Should ideally list all original schema cols if projection is None but schema is selected
        } else {
            selected_field_names.join(", ")
        };

        let query = format!("SELECT {} FROM \"{}\"", sql_select_cols, self.table_name);
        // log::debug!("Executing scan query on Postgres: {}", query);

        let rows =
            self.client.query(&query, &[]).await.map_err(|pg_err| {
                DataFusionError::External(Box::new(IglooError::Postgres(pg_err)))
            })?;

        if rows.is_empty() {
            let batch = RecordBatch::new_empty(projected_schema.clone());
            return Ok(Arc::new(MemoryExec::try_new(
                &[vec![batch]],
                self.schema(),
                projection.cloned(),
            )?));
        }

        let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());

        for (col_idx, field) in projected_schema.fields().iter().enumerate() {
            // It's crucial that `col_idx` here correctly maps to the column index in the `row` from `tokio_postgres`.
            // If `sql_select_cols` is "*" this is simple, but with projected columns, ensure the order matches.
            // The current `sql_select_cols` generation based on `projected_schema` field names ensures this.

            macro_rules! append_col_data {
                ($builder:expr, $pg_type:ty, $arrow_builder_type:ty) => {{
                    let mut builder = $builder;
                    for row in &rows {
                        match row.try_get::<usize, Option<$pg_type>>(col_idx) {
                            // Corrected: $usize -> usize
                            Ok(Some(val)) => builder.append_value(val),
                            Ok(None) => builder.append_null(),
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(
                                    IglooError::Postgres(e),
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }};
                // Variant for string types that need `&str`
                ($builder:expr, String, $arrow_builder_type:ty) => {{
                    let mut builder = $builder;
                    for row in &rows {
                        match row.try_get::<usize, Option<String>>(col_idx) {
                            // Corrected: $usize -> usize
                            Ok(Some(val)) => builder.append_value(&val),
                            Ok(None) => builder.append_null(),
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(
                                    IglooError::Postgres(e),
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }};
                // Variant for chrono NaiveDateTime -> TimestampNanosecond
                ($builder:expr, chrono::NaiveDateTime, $arrow_builder_type:ty) => {{
                    let mut builder = $builder;
                    for row in &rows {
                        match row.try_get::<usize, Option<chrono::NaiveDateTime>>(col_idx) {
                            // Corrected: $usize -> usize
                            Ok(Some(val)) => {
                                builder.append_value(val.timestamp_nanos_opt().unwrap_or_default())
                            }
                            Ok(None) => builder.append_null(),
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(
                                    IglooError::Postgres(e),
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }};
                // Variant for chrono NaiveDate -> Date32
                ($builder:expr, chrono::NaiveDate, $arrow_builder_type:ty) => {{
                    let mut builder = $builder;
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    for row in &rows {
                        match row.try_get::<usize, Option<chrono::NaiveDate>>(col_idx) {
                            // Corrected: $usize -> usize
                            Ok(Some(val)) => builder
                                .append_value(val.signed_duration_since(epoch).num_days() as i32),
                            Ok(None) => builder.append_null(),
                            Err(e) => {
                                return Err(DataFusionError::External(Box::new(
                                    IglooError::Postgres(e),
                                )))
                            }
                        }
                    }
                    Arc::new(builder.finish()) as ArrayRef
                }};
            }

            let array_ref: ArrayRef = match field.data_type() {
                DataType::Int16 => {
                    append_col_data!(Int16Array::builder(rows.len()), i16, Int16Array)
                }
                DataType::Int32 => {
                    append_col_data!(Int32Array::builder(rows.len()), i32, Int32Array)
                }
                DataType::Int64 => {
                    append_col_data!(Int64Array::builder(rows.len()), i64, Int64Array)
                }
                DataType::Float32 => {
                    append_col_data!(Float32Array::builder(rows.len()), f32, Float32Array)
                }
                DataType::Float64 => {
                    append_col_data!(Float64Array::builder(rows.len()), f64, Float64Array)
                }
                DataType::Utf8 => {
                    append_col_data!(StringArray::builder(rows.len()), String, StringArray)
                }
                DataType::Boolean => {
                    append_col_data!(BooleanArray::builder(rows.len()), bool, BooleanArray)
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    // Assuming UTC if no timezone specified in Arrow schema
                    append_col_data!(
                        TimestampNanosecondArray::builder(rows.len()),
                        chrono::NaiveDateTime,
                        TimestampNanosecondArray
                    )
                }
                DataType::Date32 => append_col_data!(
                    Date32Array::builder(rows.len()),
                    chrono::NaiveDate,
                    Date32Array
                ),
                DataType::Binary => append_col_data!(
                    GenericBinaryArray::<i32>::builder(rows.len()),
                    Vec<u8>,
                    GenericBinaryArray<i32>
                ),
                dt => {
                    return Err(DataFusionError::External(Box::new(
                        IglooError::UnsupportedArrowType(dt.clone()),
                    )));
                }
            };
            arrow_columns.push(array_ref);
        }

        let batch = RecordBatch::try_new(projected_schema.clone(), arrow_columns)
            .map_err(|e| DataFusionError::ArrowError(e, None))?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
