use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Use the central EngineError
use crate::execution::error::EngineError;

/// An ExecutionOperator produces a stream of RecordBatches.
pub trait ExecutionOperator: std::fmt::Debug { // Added Debug constraint for Box<dyn ExecutionOperator>
    /// Returns the schema of the RecordBatches produced by this operator.
    fn schema(&self) -> Result<SchemaRef, EngineError>;

    /// Returns the next RecordBatch from this operator.
    fn next(&mut self) -> Result<Option<RecordBatch>, EngineError>;
}

// --- ADBC Mocking (still local to this file) ---
#[derive(Debug, Clone)] // AdbcError itself needs to be Debug and Clone if EngineError::Adbc stores it directly
                        // Since EngineError::Adbc stores String, AdbcError only needs Debug for format!
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
        .map_err(|e| AdbcError::Other(e.to_string()))?; // Keep AdbcError internal mapping
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
    } else {
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
    fetch_error: Option<AdbcError>, // Store the original AdbcError
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
                // Convert stored AdbcError to EngineError::Adbc
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
                self.fetch_error = Some(adbc_err.clone()); // Store original AdbcError
                self.schema = Some(Arc::new(Schema::empty()));
                // Convert AdbcError to EngineError::Adbc
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
            // If schema is not yet determined (e.g., next() not called), return empty.
            // Downstream operators might fail if they require a non-empty schema here.
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
        let schema = input.schema()?; // This can fail, or return empty schema from unprimed ScanOp
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
                    // ArrowError from predicate is converted via From trait into EngineError::Arrow
                    let filter_array = (self.predicate)(&batch)?;
                    // ArrowError from filter_record_batch is converted via From trait
                    let filtered_batch = arrow::compute::filter_record_batch(&batch, &filter_array)?;

                    if filtered_batch.num_rows() > 0 {
                        return Ok(Some(filtered_batch));
                    }
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(e), // Propagate EngineError from input
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
                message: "Cannot project fields from an empty input schema.".to_string(),
             });
        }

        let mut projected_fields = Vec::new();
        for &index in &projection_indices {
            if index >= input_schema.fields().len() {
                return Err(EngineError::Projection {
                    message: format!(
                        "Projection index {} out of bounds for schema with {} fields",
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
                // ArrowError from project is converted via From trait
                let projected_batch = arrow::compute::project(&batch, &self.projection_indices)?;
                Ok(Some(projected_batch))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e), // Propagate EngineError from input
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // No longer need local TestEngineError. Tests will use the actual EngineError.
    // Need to adjust assertions if error variants or messages changed.
    use arrow::array::{Int32Array, BooleanArray}; // StringArray no longer used here
    use arrow::compute::compare_scalar;
    use crate::execution::error::EngineError as ActualEngineError; // Alias for clarity in tests

    fn prime_scan_op_schema(scan_op: &mut ScanOperator) -> Result<SchemaRef, ActualEngineError> {
        let _ = scan_op.next()?;
        scan_op.schema()
    }

    #[test]
    fn scan_op_schema_unfetched() {
        let scan_op = ScanOperator::new("test_table");
        assert_eq!(scan_op.schema().unwrap(), Arc::new(Schema::empty()));
    }

    #[test]
    fn scan_op_schema_after_next() {
        let mut scan_op = ScanOperator::new("test_table");
        let _ = scan_op.next().unwrap();
        let schema = scan_op.schema().unwrap();
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn scan_op_schema_fetch_error() {
        let mut scan_op = ScanOperator::new("non_existent_table");
        let res = scan_op.next();
        assert!(res.is_err());
        match res.err().unwrap() {
            ActualEngineError::Adbc { message } => {
                assert!(message.contains("Table non_existent_table not found"));
            }
            other_err => panic!("Expected Adbc error, got {:?}", other_err),
        }
        let schema = scan_op.schema().unwrap();
        assert!(schema.fields().is_empty());
    }

    #[test]
    fn filter_op_new_with_primed_scan_op() {
        let mut scan_op = ScanOperator::new("test_table");
        let _ = prime_scan_op_schema(&mut scan_op).unwrap();
        let predicate: FilterPredicate = Box::new(|rb| Ok(BooleanArray::new_scalar(true, rb.num_rows())));
        let filter_op_res = FilterOperator::new(Box::new(scan_op), predicate);
        assert!(filter_op_res.is_ok());
        assert_eq!(filter_op_res.unwrap().schema().unwrap().fields().len(), 3);
    }

    #[test]
    fn filter_op_new_with_unprimed_scan_op() {
        let scan_op = ScanOperator::new("test_table");
        let predicate: FilterPredicate = Box::new(|rb| Ok(BooleanArray::new_scalar(true, rb.num_rows())));
        let filter_op_res = FilterOperator::new(Box::new(scan_op), predicate);
        assert!(filter_op_res.is_ok());
        assert!(filter_op_res.unwrap().schema().unwrap().fields().is_empty());
    }

    #[test]
    fn filter_op_predicate_arrow_error() {
        let mut scan_op = ScanOperator::new("test_table"); // Will produce batches with 3 columns
        let faulty_predicate: FilterPredicate = Box::new(|_batch: &RecordBatch| {
            Err(ArrowError::ComputeError("Predicate failure".to_string()))
        });
        let mut filter_op = FilterOperator::new(Box::new(scan_op), faulty_predicate).unwrap();
        let res = filter_op.next();
        assert!(res.is_err());
        match res.err().unwrap() {
            ActualEngineError::Arrow { source: ArrowError::ComputeError(msg) } => {
                assert_eq!(msg, "Predicate failure");
            }
            other_err => panic!("Expected Arrow ComputeError, got {:?}", other_err),
        }
    }


    #[test]
    fn projection_operator_new_with_primed_scan_op() {
        let mut scan_op = ScanOperator::new("test_table");
        let _ = prime_scan_op_schema(&mut scan_op).unwrap();
        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 2]);
        assert!(proj_op_res.is_ok());
        let proj_op = proj_op_res.unwrap();
        let projected_schema = proj_op.schema().unwrap();
        assert_eq!(projected_schema.fields().len(), 2);
    }

    #[test]
    fn projection_operator_new_with_unprimed_scan_op_fails() {
        let scan_op = ScanOperator::new("test_table");
        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 2]);
        assert!(proj_op_res.is_err());
        match proj_op_res.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert!(message.contains("Cannot project fields from an empty input schema."));
            }
            other_err => panic!("Unexpected error: {:?}", other_err),
        }
    }

    #[test]
    fn projection_operator_invalid_index_error() {
        let mut scan_op = ScanOperator::new("single_col_table");
        let _ = prime_scan_op_schema(&mut scan_op).unwrap(); // Prime to get actual schema

        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 1]); // Index 1 is out of bounds
        assert!(proj_op_res.is_err());
        match proj_op_res.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert!(message.contains("Projection index 1 out of bounds for schema with 1 fields"));
            }
            other_err => panic!("Unexpected error: {:?}", other_err),
        }
    }
}
