// src/postgres_table.rs
// Minimal TableProvider for Postgres integration with DataFusion

use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, MemTable};
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::logical_expr::TableType;
use tokio_postgres::NoTls;

#[derive(Debug)]
pub struct PostgresTable {
    schema: SchemaRef,
    conn_str: String,
    table: String,
}

impl PostgresTable {
    pub fn new(conn_str: &str, table: &str, schema: SchemaRef) -> Self {
        Self {
            schema,
            conn_str: conn_str.to_string(),
            table: table.to_string(),
        }
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
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Connect to Postgres and fetch all rows from the table
        let (client, connection) = tokio_postgres::connect(&self.conn_str, NoTls).await.unwrap();
        tokio::spawn(connection);
        let query = format!("SELECT * FROM {}", self.table);
        let rows = client.query(query.as_str(), &[]).await.unwrap();
        // For MVP: assume two columns: user_id (i64), extra_info (String)
        let user_ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
        let extra_infos: Vec<String> = rows.iter().map(|r| r.get(1)).collect();
        use datafusion::arrow::array::{Int64Array, StringArray, ArrayRef};
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(user_ids)) as ArrayRef,
                Arc::new(StringArray::from(extra_infos)) as ArrayRef,
            ],
        ).unwrap();
        let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(_state, projection, filters, limit).await
    }
}
