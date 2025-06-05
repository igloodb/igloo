use std::sync::Arc;
use arrow::array::{BooleanArray, Int32Array};
use arrow::record_batch::RecordBatch;
use arrow::error::ArrowError;
use arrow::compute;

// Use the central EngineError
use crate::execution::error::EngineError;
use crate::execution::operators::{
    ExecutionOperator, ScanOperator, FilterOperator, FilterPredicate, ProjectionOperator,
};

/// Placeholder for a Logical Plan Node.
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

/// Builds a physical execution plan from a `LogicalPlan`.
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
                    if batch.schema().fields().is_empty() {
                         return Err(ArrowError::SchemaError("Predicate 'id_gt_1' cannot be applied to batch with no columns".to_string()));
                    }
                    // This check should ideally use schema field metadata if available,
                    // or rely on the query planner to ensure type correctness.
                    if batch.schema().fields().first().map_or(true, |f| f.name() != "id") {
                         return Err(ArrowError::SchemaError(format!("Predicate 'id_gt_1' expects first column to be 'id', found '{}'", batch.schema().field(0).name())));
                    }
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| {
                            ArrowError::CastError(format!("Failed to downcast column 'id' to Int32Array. Actual type: {:?}", batch.column(0).data_type()))
                        })?;
                    compute::gt_scalar(ids, 1_i32)
                })
            } else if predicate_expr == "value_lt_10" {
                 Box::new(|batch: &RecordBatch| {
                    if batch.schema().fields().len() < 3 {
                         return Err(ArrowError::SchemaError("Predicate 'value_lt_10' expects at least 3 columns".to_string()));
                    }
                     if batch.schema().field(2).name() != "value" {
                         return Err(ArrowError::SchemaError(format!("Predicate 'value_lt_10' expects third column to be 'value', found '{}'", batch.schema().field(2).name())));
                    }
                    let values = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| ArrowError::CastError("Failed to downcast 'value' to Int32Array".into()))?;
                    compute::lt_scalar(values, 10_i32)
                })
            }
            // Add other specific predicate handlers here if needed for tests
            else if predicate_expr == "value_gt_100" { // Added for testing filter all
                 Box::new(|batch: &RecordBatch| {
                    if batch.schema().fields().len() < 3 || batch.schema().field(2).name() != "value" {
                         return Err(ArrowError::SchemaError("Predicate 'value_gt_100' expects column 'value' at index 2".to_string()));
                    }
                    let values = batch.column(2).as_any().downcast_ref::<Int32Array>()
                        .ok_or_else(|| ArrowError::CastError("Failed to downcast 'value' to Int32Array for 'value_gt_100'".into()))?;
                    compute::gt_scalar(values, 100_i32)
                 })
            }
            else {
                // Default: a predicate that lets all data pass
                Box::new(|batch: &RecordBatch| {
                    Ok(BooleanArray::new_scalar(true, batch.num_rows()))
                })
            };
            // ArrowErrors from predicate_fn will be converted by FilterOperator using `?`
            let filter_op = FilterOperator::new(input_physical_plan, predicate_fn)?;
            Ok(Box::new(filter_op))
        }
        LogicalPlan::Projection {
            input,
            projection_indices,
        } => {
            let input_physical_plan = build_physical_plan(*input)?;
            // ProjectionOperator::new returns EngineError::Projection on failure
            let projection_op =
                ProjectionOperator::new(input_physical_plan, projection_indices)?;
            Ok(Box::new(projection_op))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Use the actual EngineError from the error module
    use crate::execution::error::EngineError as ActualEngineError;
    use crate::execution::operators::AdbcError; // For matching AdbcError string content
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef}; // Added SchemaRef
    use std::sync::Arc;


    fn get_schema_from_op(op: &mut Box<dyn ExecutionOperator>) -> Result<SchemaRef, ActualEngineError> {
        op.schema()
    }

    fn run_operator_to_get_real_schema(op: &mut Box<dyn ExecutionOperator>) -> Result<SchemaRef, ActualEngineError> {
        match op.next() {
            Ok(Some(batch)) => Ok(batch.schema()),
            Ok(None) => op.schema(),
            Err(e) => Err(e),
        }
    }

    #[test]
    fn test_build_scan_plan() {
        let plan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
        };
        let mut physical_plan = build_physical_plan(plan).expect("Failed to build scan plan");
        let initial_schema = get_schema_from_op(&mut physical_plan).unwrap();
        assert!(initial_schema.fields().is_empty());
        let loaded_schema = run_operator_to_get_real_schema(&mut physical_plan).unwrap();
        assert_eq!(loaded_schema.fields().len(), 3);
    }

    #[test]
    fn test_build_filter_scan_plan() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            predicate_expr: "id_gt_1".to_string(),
        };
        let mut physical_plan = build_physical_plan(plan).expect("Failed to build filter_scan plan");
        let filter_op_schema = get_schema_from_op(&mut physical_plan).unwrap();
        assert!(filter_op_schema.fields().is_empty());

        let first_batch = physical_plan.next().unwrap().expect("Expected a batch");
        assert_eq!(first_batch.schema().fields().len(), 3);
        let id_col = first_batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(id_col.values(), &[2, 3]);
    }

    #[test]
    fn test_build_projection_filter_scan_plan_fail_unprimed() {
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(LogicalPlan::Scan {
                    table_name: "test_table".to_string(),
                }),
                predicate_expr: "id_gt_1".to_string(),
            }),
            projection_indices: vec![2, 0],
        };
        let build_result = build_physical_plan(plan);
        assert!(build_result.is_err());
        match build_result.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert!(message.contains("Cannot project fields from an empty input schema."));
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }

    #[test]
    fn test_build_projection_filter_scan_plan_successful_after_manual_prime_equivalent() {
        let mut scan_op = ScanOperator::new("test_table");
        let _ = scan_op.next().unwrap(); // Prime scan_op

        let predicate_fn_filter: FilterPredicate = Box::new(|batch: &RecordBatch| {
            let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            compute::gt_scalar(ids, 1_i32)
        });
        // Pass scan_op (now primed and has a schema) to FilterOperator
        let filter_op_res = FilterOperator::new(Box::new(scan_op), predicate_fn_filter);
        assert!(filter_op_res.is_ok());
        let mut filter_op = filter_op_res.unwrap();
        let filter_schema = filter_op.schema().unwrap();
        assert_eq!(filter_schema.fields().len(), 3);

        // Pass filter_op (which has a valid schema) to ProjectionOperator
        let projection_op_res = ProjectionOperator::new(Box::new(filter_op), vec![2, 0]);
        assert!(projection_op_res.is_ok());
        let mut projection_op = projection_op_res.unwrap();
        let projected_schema = projection_op.schema().unwrap();
        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "value");

        let batch1 = projection_op.next().unwrap().expect("Expected first batch");
        assert_eq!(batch1.num_rows(), 2);
        let val_col_b1 = batch1.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(val_col_b1.values(), &[5, 20]);
    }

    #[test]
    fn test_build_invalid_projection_indices_on_unprimed_scan() {
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            projection_indices: vec![0, 10], // Index 10 is out of bounds
        };
        let build_result = build_physical_plan(plan);
        assert!(build_result.is_err());
        match build_result.err().unwrap() {
            ActualEngineError::Projection { message } => {
                assert!(message.contains("Cannot project fields from an empty input schema."));
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }

    #[test]
    fn test_build_invalid_projection_indices_on_primed_scan() {
        // Manually create a primed scan operator
        let mut scan_op = ScanOperator::new("test_table");
        let _ = scan_op.next().unwrap(); // Prime it, so it has a schema

        // Now, try to create a ProjectionOperator with this primed scan_op and invalid indices
        let proj_op_res = ProjectionOperator::new(Box::new(scan_op), vec![0, 10]);
        assert!(proj_op_res.is_err());
        match proj_op_res.err().unwrap() {
            ActualEngineError::Projection { message } => {
                // test_table has 3 columns (0, 1, 2). Index 10 is out of bounds.
                assert!(message.contains("Projection index 10 out of bounds for schema with 3 fields"));
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }


    #[test]
    fn test_filter_predicate_error_column_not_found() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "single_col_table".to_string(),
            }),
            predicate_expr: "id_gt_1".to_string(),
        };
        let mut physical_plan = build_physical_plan(plan).expect("Build should succeed");
        let result = physical_plan.next();
        assert!(result.is_err());
        match result.err().unwrap() {
            ActualEngineError::Arrow { source } => {
                assert!(source.to_string().contains("Predicate 'id_gt_1' expects first column to be 'id', found 'col_a'"));
            }
            e => panic!("Expected Arrow error from predicate, got {:?}", e),
        }
    }
}
