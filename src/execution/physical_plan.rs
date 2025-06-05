use std::sync::Arc;
use arrow::array::{BooleanArray, Int32Array};
use arrow::record_batch::RecordBatch;
use arrow::error::ArrowError;
use arrow::compute;
use arrow::datatypes::DataType;

use crate::execution::error::EngineError;
use crate::execution::operators::{
    ExecutionOperator, ScanOperator, FilterOperator, FilterPredicate, ProjectionOperator,
};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate_expr: String,
    },
    Projection {
        input: Box<LogicalPlan>,
        projection_indices: Vec<usize>,
    },
}

pub fn build_physical_plan(
    logical_plan: LogicalPlan,
) -> Result<Box<dyn ExecutionOperator>, EngineError> {
    match logical_plan {
        LogicalPlan::Scan { table_name } => {
            let scan_op = ScanOperator::new(&table_name);
            Ok(Box::new(scan_op))
        }
        LogicalPlan::Filter {
            input,
            predicate_expr,
        } => {
            let input_physical_plan = build_physical_plan(*input)?;

            let predicate_fn: FilterPredicate = if predicate_expr == "id_gt_1" {
                Box::new(|batch: &RecordBatch| {
                    const EXPECTED_COL_INDEX: usize = 0;
                    const EXPECTED_COL_NAME: &str = "id";
                    const PREDICATE_NAME: &str = "id_gt_1";

                    if batch.num_columns() <= EXPECTED_COL_INDEX {
                        return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Batch has only {} columns, expected column '{}' at index {}.",
                            PREDICATE_NAME, batch.num_columns(), EXPECTED_COL_NAME, EXPECTED_COL_INDEX
                        )));
                    }

                    let field = batch.schema().field(EXPECTED_COL_INDEX);
                    if field.name() != EXPECTED_COL_NAME {
                        return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Expected column #{} to be named '{}', but found '{}'.",
                            PREDICATE_NAME, EXPECTED_COL_INDEX, EXPECTED_COL_NAME, field.name()
                        )));
                    }

                    let ids = batch
                        .column(EXPECTED_COL_INDEX)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            ArrowError::CastError(format!(
                                "Predicate '{}': Failed to downcast column #{} ('{}') to Int32Array. Actual type: {:?}, Expected: Int32.",
                                PREDICATE_NAME, EXPECTED_COL_INDEX, field.name(), field.data_type()
                            ))
                        })?;
                    compute::gt_scalar(ids, 1_i32)
                })
            } else if predicate_expr == "value_lt_10" {
                 Box::new(|batch: &RecordBatch| {
                    const EXPECTED_COL_INDEX: usize = 2;
                    const EXPECTED_COL_NAME: &str = "value";
                    const PREDICATE_NAME: &str = "value_lt_10";

                    if batch.num_columns() <= EXPECTED_COL_INDEX {
                         return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Batch has only {} columns, expected column '{}' at index {}.",
                            PREDICATE_NAME, batch.num_columns(), EXPECTED_COL_NAME, EXPECTED_COL_INDEX
                        )));
                    }

                    let field = batch.schema().field(EXPECTED_COL_INDEX);
                    if field.name() != EXPECTED_COL_NAME {
                        return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Expected column #{} to be named '{}', but found '{}'.",
                            PREDICATE_NAME, EXPECTED_COL_INDEX, EXPECTED_COL_NAME, field.name()
                        )));
                    }

                    let values = batch
                        .column(EXPECTED_COL_INDEX)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| ArrowError::CastError(format!(
                            "Predicate '{}': Failed to downcast column #{} ('{}') to Int32Array. Actual type: {:?}, Expected: Int32.",
                            PREDICATE_NAME, EXPECTED_COL_INDEX, field.name(), field.data_type()
                        )))?;
                    compute::lt_scalar(values, 10_i32)
                })
            }
            else if predicate_expr == "value_gt_100" {
                 Box::new(|batch: &RecordBatch| {
                    const EXPECTED_COL_INDEX: usize = 2;
                    const EXPECTED_COL_NAME: &str = "value";
                    const PREDICATE_NAME: &str = "value_gt_100";

                    if batch.num_columns() <= EXPECTED_COL_INDEX {
                         return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Batch has only {} columns, expected column '{}' at index {}.",
                            PREDICATE_NAME, batch.num_columns(), EXPECTED_COL_NAME, EXPECTED_COL_INDEX
                        )));
                    }

                    let field = batch.schema().field(EXPECTED_COL_INDEX);
                    if field.name() != EXPECTED_COL_NAME {
                        return Err(ArrowError::SchemaError(format!(
                            "Predicate '{}': Expected column #{} to be named '{}', but found '{}'.",
                            PREDICATE_NAME, EXPECTED_COL_INDEX, EXPECTED_COL_NAME, field.name()
                        )));
                    }

                    let values = batch.column(EXPECTED_COL_INDEX).as_any().downcast_ref::<Int32Array>()
                        .ok_or_else(|| ArrowError::CastError(format!(
                            "Predicate '{}': Failed to downcast column #{} ('{}') to Int32Array. Actual type: {:?}, Expected: Int32.",
                            PREDICATE_NAME, EXPECTED_COL_INDEX, field.name(), field.data_type()
                        )))?;
                    compute::gt_scalar(values, 100_i32)
                 })
            }
            else {
                Box::new(|batch: &RecordBatch| {
                    Ok(BooleanArray::new_scalar(true, batch.num_rows()))
                })
            };
            let filter_op = FilterOperator::new(input_physical_plan, predicate_fn)?;
            Ok(Box::new(filter_op))
        }
        LogicalPlan::Projection {
            input,
            projection_indices,
        } => {
            let input_physical_plan = build_physical_plan(*input)?;
            let projection_op =
                ProjectionOperator::new(input_physical_plan, projection_indices)?;
            Ok(Box::new(projection_op))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::error::EngineError as ActualEngineError;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    #[derive(Debug)]
    struct MockScanOp {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        idx: usize,
    }

    impl MockScanOp {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self { batches, schema, idx: 0 }
        }
    }
    impl ExecutionOperator for MockScanOp {
        fn schema(&self) -> Result<SchemaRef, ActualEngineError> { Ok(self.schema.clone()) }
        fn next(&mut self) -> Result<Option<RecordBatch>, ActualEngineError> {
            if self.idx < self.batches.len() {
                self.idx += 1;
                Ok(Some(self.batches[self.idx - 1].clone()))
            } else {
                Ok(None)
            }
        }
    }

    // Helper to extract the predicate function for "id_gt_1" for direct testing
    // This is somewhat fragile as it duplicates the predicate construction logic.
    // A better design might involve a predicate registry or factory.
    fn get_id_gt_1_predicate_fn() -> FilterPredicate {
        Box::new(|batch: &RecordBatch| {
            const EXPECTED_COL_INDEX: usize = 0;
            const EXPECTED_COL_NAME: &str = "id";
            const PREDICATE_NAME: &str = "id_gt_1";

            if batch.num_columns() <= EXPECTED_COL_INDEX {
                return Err(ArrowError::SchemaError(format!(
                    "Predicate '{}': Batch has only {} columns, expected column '{}' at index {}.",
                    PREDICATE_NAME, batch.num_columns(), EXPECTED_COL_NAME, EXPECTED_COL_INDEX
                )));
            }
            let field = batch.schema().field(EXPECTED_COL_INDEX);
            if field.name() != EXPECTED_COL_NAME {
                return Err(ArrowError::SchemaError(format!(
                    "Predicate '{}': Expected column #{} to be named '{}', but found '{}'.",
                    PREDICATE_NAME, EXPECTED_COL_INDEX, EXPECTED_COL_NAME, field.name()
                )));
            }
            let ids = batch.column(EXPECTED_COL_INDEX).as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| ArrowError::CastError(format!(
                    "Predicate '{}': Failed to downcast column #{} ('{}') to Int32Array. Actual type: {:?}, Expected: Int32.",
                    PREDICATE_NAME, EXPECTED_COL_INDEX, field.name(), field.data_type()
                )))?;
            compute::gt_scalar(ids, 1_i32)
        })
    }


    #[test]
    fn test_filter_predicate_cast_error_handling() {
        let schema_wrong_type = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false), // Correct name "id", but wrong type
        ]));
        let batch_wrong_type = RecordBatch::try_new(
            schema_wrong_type.clone(),
            vec![Arc::new(StringArray::from(vec!["0", "1", "2"]))],
        ).unwrap();
        let scan_op_wrong_type = Box::new(MockScanOp::new(schema_wrong_type.clone(), vec![batch_wrong_type]));

        let predicate_fn = get_id_gt_1_predicate_fn();
        let mut filter_op = FilterOperator::new(scan_op_wrong_type, predicate_fn)
            .expect("FilterOperator creation failed");

        let result = filter_op.next();
        assert!(result.is_err());
        match result.err().unwrap() {
            ActualEngineError::Arrow { source } => {
                assert!(matches!(&source, ArrowError::CastError(msg) if msg == "Predicate 'id_gt_1': Failed to downcast column #0 ('id') to Int32Array. Actual type: Utf8, Expected: Int32."), "Unexpected error: {:?}", source);
            }
            e => panic!("Expected EngineError::Arrow with CastError, got {:?}", e),
        }
    }

    #[test]
    fn test_filter_predicate_name_mismatch_error() {
        let schema_wrong_name = Arc::new(Schema::new(vec![
            Field::new("identifier", DataType::Int32, false),
        ]));
        let batch_wrong_name = RecordBatch::try_new(
            schema_wrong_name.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        let scan_op_wrong_name = Box::new(MockScanOp::new(schema_wrong_name.clone(), vec![batch_wrong_name]));

        let predicate_fn = get_id_gt_1_predicate_fn();
        let mut filter_op = FilterOperator::new(scan_op_wrong_name, predicate_fn)
            .expect("FilterOperator creation failed");

        let result = filter_op.next();
        assert!(result.is_err());
        match result.err().unwrap() {
            ActualEngineError::Arrow { source } => {
                assert!(matches!(&source, ArrowError::SchemaError(msg) if msg == "Predicate 'id_gt_1': Expected column #0 to be named 'id', but found 'identifier'."), "Unexpected error: {:?}", source);
            }
            e => panic!("Expected EngineError::Arrow with SchemaError, got {:?}", e),
        }
    }

    #[test]
    fn test_filter_predicate_insufficient_columns_error() {
        let schema_no_cols = Arc::new(Schema::new(vec![]));
        let batch_no_cols = RecordBatch::try_new(schema_no_cols.clone(), vec![]).unwrap();
        let scan_op_no_cols = Box::new(MockScanOp::new(schema_no_cols, vec![batch_no_cols]));

        let predicate_fn = get_id_gt_1_predicate_fn();
        let mut filter_op = FilterOperator::new(scan_op_no_cols, predicate_fn)
            .expect("FilterOperator creation failed");

        let result = filter_op.next();
        assert!(result.is_err());
        match result.err().unwrap() {
            ActualEngineError::Arrow { source } => {
                assert!(matches!(&source, ArrowError::SchemaError(msg) if msg == "Predicate 'id_gt_1': Batch has only 0 columns, expected column 'id' at index 0."), "Unexpected error: {:?}", source);
            }
            e => panic!("Expected EngineError::Arrow with SchemaError, got {:?}", e),
        }
    }

    #[test]
    fn test_build_projection_filter_scan_plan_fail_unprimed() {
        let logical_plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table_name: "test_table".to_string(),
                }),
                predicate_expr: "id_gt_1".to_string(),
            }),
            projection_indices: vec![2, 0],
        };
        let build_result = build_physical_plan(logical_plan);
        assert!(build_result.is_err());
        match build_result.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert_eq!(message, "Cannot project specific columns (non-empty projection_indices) from an empty input schema.");
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }

    #[test]
    fn test_build_invalid_projection_indices_on_unprimed_scan() {
        let logical_plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            projection_indices: vec![0, 10],
        };
        let build_result = build_physical_plan(logical_plan);
        assert!(build_result.is_err());
        match build_result.err().unwrap() {
            ActualEngineError::Projection { message } => {
                // This error occurs because the input schema (from unprimed scan) is empty.
                assert_eq!(message, "Cannot project specific columns (non-empty projection_indices) from an empty input schema.");
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }

    #[test]
    fn test_build_invalid_projection_indices_on_primed_scan() {
        // This test is about ProjectionOperator::new directly, not build_physical_plan
        let mut scan_op = ScanOperator::new("test_table");
        let _ = scan_op.next().unwrap(); // Prime it, so it has a schema (3 fields)

        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 10]); // Index 10 is out of bounds
        assert!(proj_op_res.is_err());
        match proj_op_res.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert_eq!(message, "Projection index 10 is out of bounds for input schema with 3 fields.");
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }
}
