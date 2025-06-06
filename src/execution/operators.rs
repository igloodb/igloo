use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Use the central EngineError
use crate::execution::error::EngineError;

/// An ExecutionOperator produces a stream of RecordBatches.
pub trait ExecutionOperator: std::fmt::Debug {
    /// Returns the schema of the RecordBatches produced by this operator.
    fn schema(&self) -> Result<SchemaRef, EngineError>;

    /// Returns the next RecordBatch from this operator.
    fn next(&mut self) -> Result<Option<RecordBatch>, EngineError>;
}

// --- ADBC Mocking (still local to this file) ---
#[derive(Debug, Clone)]
pub enum AdbcError {
    ConnectionError,
    FetchError(String),
    Other(String),
}

impl std::fmt::Display for AdbcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdbcError::ConnectionError => write!(f, "ADBC Connection Error"),
            AdbcError::FetchError(s) => write!(f, "ADBC Fetch Error: {}", s),
            AdbcError::Other(s) => write!(f, "ADBC Error: {}", s),
        }
    }
}
// --- End ADBC Mocking ---

pub fn fetch_data_mock(
    table_name: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), AdbcError> {
    if table_name == "test_table" {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![0, 1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["sys", "foo", "bar", "baz"])),
                Arc::new(arrow::array::Int32Array::from(vec![0, 10, 5, 20])),
            ],
        )
        .map_err(|e| AdbcError::Other(e.to_string()))?;
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![4, 5])),
                Arc::new(arrow::array::StringArray::from(vec!["qux", "quux"])),
                Arc::new(arrow::array::Int32Array::from(vec![15, 25])),
            ],
        )
        .map_err(|e| AdbcError::Other(e.to_string()))?;
        Ok((schema, vec![batch1, batch2]))
    } else if table_name == "single_col_table" {
         let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![100, 200])),
            ],
        )
        .map_err(|e| AdbcError::Other(e.to_string()))?;
        Ok((schema, vec![batch]))
    } else if table_name == "empty_table" {
        let schema = Arc::new(Schema::new(vec![
            Field::new("data", DataType::Int64, true),
        ]));
        Ok((schema, vec![]))
    } else if table_name == "truly_empty_schema_table" {
         Ok((Arc::new(Schema::empty()), vec![]))
    }
     else {
        Err(AdbcError::FetchError(format!(
            "Table {} not found",
            table_name
        )))
    }
}

#[derive(Debug)]
pub struct ScanOperator {
    table_name: String,
    schema: Option<SchemaRef>,
    batches: std::vec::IntoIter<RecordBatch>,
    fetched: bool,
    fetch_error: Option<AdbcError>,
}

impl ScanOperator {
    pub fn new(table_name: &str) -> Self {
        ScanOperator {
            table_name: table_name.to_string(),
            schema: None,
            batches: Vec::new().into_iter(),
            fetched: false,
            fetch_error: None,
        }
    }

    fn ensure_schema_and_data_fetched(&mut self) -> Result<(), EngineError> {
        if self.fetched {
            if let Some(ref err) = self.fetch_error {
                return Err(EngineError::Adbc { message: err.to_string() });
            }
            return Ok(());
        }

        self.fetched = true;
        match fetch_data_mock(&self.table_name) {
            Ok((schema, batches)) => {
                self.schema = Some(schema);
                self.batches = batches.into_iter();
                Ok(())
            }
            Err(adbc_err) => {
                self.fetch_error = Some(adbc_err.clone());
                self.schema = Some(Arc::new(Schema::empty()));
                Err(EngineError::Adbc { message: adbc_err.to_string() })
            }
        }
    }
}

impl ExecutionOperator for ScanOperator {
    fn schema(&self) -> Result<SchemaRef, EngineError> {
        if let Some(ref schema) = self.schema {
            Ok(schema.clone())
        } else {
            Ok(Arc::new(Schema::empty()))
        }
    }

    fn next(&mut self) -> Result<Option<RecordBatch>, EngineError> {
        if let Err(e) = self.ensure_schema_and_data_fetched() {
            return Err(e);
        }
        Ok(self.batches.next())
    }
}

pub type FilterPredicate = Box<dyn Fn(&RecordBatch) -> Result<BooleanArray, ArrowError>>;

#[derive(Debug)]
pub struct FilterOperator {
    input: Box<dyn ExecutionOperator>,
    predicate: FilterPredicate,
    schema: SchemaRef,
}

impl FilterOperator {
    pub fn new(mut input: Box<dyn ExecutionOperator>, predicate: FilterPredicate) -> Result<Self, EngineError> {
        let schema = input.schema()?;
        Ok(FilterOperator { input, predicate, schema })
    }
}

impl ExecutionOperator for FilterOperator {
    fn schema(&self) -> Result<SchemaRef, EngineError> {
        Ok(self.schema.clone())
    }

    fn next(&mut self) -> Result<Option<RecordBatch>, EngineError> {
        loop {
            match self.input.next() {
                Ok(Some(batch)) => {
                    let filter_array = (self.predicate)(&batch)?;
                    let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;
                    if filtered_batch.num_rows() > 0 {
                        return Ok(Some(filtered_batch));
                    }
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }
}

#[derive(Debug)]
pub struct ProjectionOperator {
    input: Box<dyn ExecutionOperator>,
    projection_indices: Vec<usize>,
    projected_schema: SchemaRef,
}

impl ProjectionOperator {
    pub fn new(mut input: Box<dyn ExecutionOperator>, projection_indices: Vec<usize>) -> Result<Self, EngineError> {
        let input_schema = input.schema()?;

        if input_schema.fields().is_empty() && !projection_indices.is_empty() {
             return Err(EngineError::Projection {
                message: "Cannot project specific columns (non-empty projection_indices) from an empty input schema.".to_string(),
             });
        }

        let mut projected_fields = Vec::new();
        for &index in &projection_indices {
            if index >= input_schema.fields().len() {
                return Err(EngineError::Projection {
                    message: format!(
                        "Projection index {} is out of bounds for input schema with {} fields.",
                        index,
                        input_schema.fields().len()
                    ),
                });
            }
            projected_fields.push(input_schema.field(index).clone());
        }
        let projected_schema = Arc::new(Schema::new(projected_fields));
        Ok(ProjectionOperator {
            input,
            projection_indices,
            projected_schema,
        })
    }
}

impl ExecutionOperator for ProjectionOperator {
    fn schema(&self) -> Result<SchemaRef, EngineError> {
        Ok(self.projected_schema.clone())
    }

    fn next(&mut self) -> Result<Option<RecordBatch>, EngineError> {
        match self.input.next() {
            Ok(Some(batch)) => {
                let projected_batch = arrow::compute::project(&batch, &self.projection_indices)?;
                Ok(Some(projected_batch))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, BooleanArray};
    use crate::execution::error::EngineError as ActualEngineError;

    #[derive(Debug)]
    struct MockInputOperator {
        schema: SchemaRef,
    }
    impl ExecutionOperator for MockInputOperator {
        fn schema(&self) -> Result<SchemaRef, ActualEngineError> {
            Ok(self.schema.clone())
        }
        fn next(&mut self) -> Result<Option<RecordBatch>, ActualEngineError> {
            Ok(None)
        }
    }

    fn prime_scan_op_schema(scan_op: &mut ScanOperator) -> Result<SchemaRef, ActualEngineError> {
        let _ = scan_op.next()?;
        scan_op.schema()
    }

    #[test]
    fn scan_op_schema_fetch_error() {
        let mut scan_op = ScanOperator::new("non_existent_table");
        let res = scan_op.next();
        let actual_error = res.err().expect("Expected an error from scan_op.next()");

        match actual_error {
            ActualEngineError::Adbc { message } => {
                let expected_substring = "ADBC Fetch Error: Table non_existent_table not found";
                assert!(
                    message.contains(expected_substring),
                    "Error message '{}' did not contain expected substring '{}'",
                    message,
                    expected_substring
                );
            }
            other_err => panic!("Expected Adbc error, but got a different error: {:?}", other_err),
        }
    }

    #[test]
    fn filter_op_predicate_arrow_error() {
        let mut scan_op = ScanOperator::new("test_table");
        let faulty_predicate: FilterPredicate = Box::new(|_batch: &RecordBatch| {
            Err(ArrowError::ComputeError("Predicate failure".to_string()))
        });
        let _ = prime_scan_op_schema(&mut scan_op).expect("Priming failed");
        let mut filter_op = FilterOperator::new(Box::new(scan_op), faulty_predicate).unwrap();
        let res = filter_op.next();
        let actual_error = res.err().expect("Expected an error from filter_op.next()");

        match actual_error {
            ActualEngineError::Arrow { source } => {
                assert!(
                    matches!(
                        &source,
                        ArrowError::ComputeError(msg) if msg == "Predicate failure"
                    ),
                    "Unexpected ArrowError variant or message. Expected ComputeError with 'Predicate failure', got: {:?}",
                    source
                );
            }
            other_err => panic!("Expected EngineError::Arrow with ComputeError, but got a different error: {:?}", other_err),
        }
    }

    // --- ProjectionOperator Tests ---
    #[test]
    fn projection_operator_new_from_empty_schema_non_empty_indices_error() {
        let empty_schema = Arc::new(Schema::empty());
        let input_op = Box::new(MockInputOperator { schema: empty_schema });

        let proj_op_res = ProjectionOperator::new(input_op, vec![0, 2]);
        let actual_error = proj_op_res.err().expect("Expected ProjectionOperator::new to fail");

        match actual_error {
            ActualEngineError::Projection { message } => {
                assert_eq!(
                    message,
                    "Cannot project specific columns (non-empty projection_indices) from an empty input schema.",
                    "Unexpected error message for projection from empty schema."
                );
            }
            other_err => panic!("Expected Projection error, but got a different error: {:?}", other_err),
        }
    }

    #[test]
    fn projection_operator_new_from_unprimed_scan_op_fails_as_expected() {
        let scan_op = ScanOperator::new("test_table");
        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 2]);
        let actual_error = proj_op_res.err().expect("Expected ProjectionOperator::new to fail");

        match actual_error {
            ActualEngineError::Projection { message } => {
                assert_eq!(
                    message,
                    "Cannot project specific columns (non-empty projection_indices) from an empty input schema.",
                    "Unexpected error message for projection from unprimed scan."
                );
            }
            other_err => panic!("Expected Projection error, but got a different error: {:?}", other_err),
        }
    }

    #[test]
    fn projection_operator_invalid_index_error() {
        let mut scan_op = ScanOperator::new("single_col_table");
        let _ = prime_scan_op_schema(&mut scan_op).unwrap();

        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 1]);
        let actual_error = proj_op_res.err().expect("Expected ProjectionOperator::new to fail");

        match actual_error {
            ActualEngineError::Projection { message } => {
                let expected_message = "Projection index 1 is out of bounds for input schema with 1 fields.";
                assert_eq!(
                    message,
                    expected_message,
                    "Unexpected error message for out of bounds index."
                );
            }
            other_err => panic!("Expected Projection error, but got a different error: {:?}", other_err),
        }
    }

    #[test]
    fn projection_operator_empty_indices_from_empty_schema() {
        let empty_schema = Arc::new(Schema::empty());
        let input_op = Box::new(MockInputOperator { schema: empty_schema });

        let proj_op_res = ProjectionOperator::new(input_op, vec![]);
        assert!(proj_op_res.is_ok(), "Projection with empty indices from empty schema should succeed.");
        let proj_op = proj_op_res.unwrap();
        assert!(proj_op.schema().unwrap().fields().is_empty());
        assert!(proj_op.projection_indices.is_empty());
    }

    #[test]
    fn projection_operator_empty_indices_from_non_empty_schema() {
        let mut scan_op = ScanOperator::new("test_table");
        let _ = prime_scan_op_schema(&mut scan_op).unwrap();

        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![]);
        assert!(proj_op_res.is_ok(), "Projection with empty indices from non-empty schema should succeed.");
        let proj_op = proj_op_res.unwrap();
        assert!(proj_op.schema().unwrap().fields().is_empty());
        assert!(proj_op.projection_indices.is_empty());
    }
}
