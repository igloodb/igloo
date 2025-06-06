use arrow::record_batch::RecordBatch;
use crate::execution::error::EngineError;
use crate::execution::operators::ExecutionOperator;

pub fn execute_query(
    mut root_operator: Box<dyn ExecutionOperator>,
) -> Result<Vec<RecordBatch>, EngineError> {
    let mut results = Vec::new();
    loop {
        match root_operator.next() {
            Ok(Some(batch)) => {
                if batch.num_rows() > 0 {
                    results.push(batch);
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::physical_plan::{build_physical_plan, LogicalPlan};
    use crate::execution::operators::{ScanOperator, FilterOperator, FilterPredicate, ProjectionOperator};
    use arrow::array::Int32Array;
    use arrow::compute;
    use arrow::array::BooleanArray;
    use arrow::error::ArrowError;
    use std::sync::Arc;
    use crate::execution::error::EngineError as ActualEngineError;


    #[test]
    fn test_execute_scan_multiple_batches() {
        let logical_plan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
        };
        let physical_plan = build_physical_plan(logical_plan).expect("Failed to build scan plan");
        let results = execute_query(physical_plan).expect("Query execution failed");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].num_rows(), 4);
        assert_eq!(results[1].num_rows(), 2);
    }

    #[test]
    fn test_execute_empty_table_scan() {
        let logical_plan = LogicalPlan::Scan {
            table_name: "empty_table".to_string(),
        };
        let physical_plan = build_physical_plan(logical_plan).expect("Failed to build scan plan for empty table");
        let results = execute_query(physical_plan).expect("Query execution for empty table failed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_execute_filter_all_manual_construction() {
        let scan_op_manual = ScanOperator::new("test_table");
        let filter_op_manual_res = FilterOperator::new(
            Box::new(scan_op_manual),
            Box::new(|batch: &RecordBatch| {
                if batch.schema().fields().is_empty() {
                    return Ok(BooleanArray::new_scalar(false, batch.num_rows()));
                }
                let ids = batch.column(0).as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| ArrowError::SchemaError("Test predicate expected Int32 for column 0".into()))?;
                compute::gt_scalar(ids, 100_i32)
            })
        );
        assert!(filter_op_manual_res.is_ok());
        let filter_op_manual = filter_op_manual_res.unwrap();

        let results_filter_all = execute_query(Box::new(filter_op_manual)).expect("Query execution failed");
        assert_eq!(results_filter_all.len(), 0, "Expected 0 batches when all rows are filtered");
    }


    #[test]
    fn test_execute_scan_error_non_existent_table() {
        let logical_plan = LogicalPlan::Scan {
            table_name: "non_existent_table".to_string(),
        };
        let physical_plan = build_physical_plan(logical_plan).expect("Build should succeed (ScanOp::new is not fallible)");
        let result = execute_query(physical_plan);
        let err = result.err().unwrap();

        assert!(
            matches!(
                err,
                ActualEngineError::Adbc { ref message } if message == "ADBC Fetch Error: Table non_existent_table not found"
            ),
            "Unexpected error variant or message. Expected Adbc error with specific message, got: {:?}",
            err
        );
    }

    #[test]
    fn test_execute_filter_predicate_error() {
        let logical_plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "single_col_table".to_string(),
            }),
            predicate_expr: "id_gt_1".to_string(),
        };
        let physical_plan = build_physical_plan(logical_plan).expect("Build should succeed");
        let result = execute_query(physical_plan);
        let err = result.err().unwrap();

        match err {
            ActualEngineError::Arrow { source } => {
                assert!(
                    matches!(
                        &source,
                        ArrowError::SchemaError(msg) if msg == "Predicate 'id_gt_1': Expected column #0 to be named 'id', but found 'col_a'."
                    ),
                    "Unexpected ArrowError variant or message. Expected SchemaError with specific message, got: {:?}",
                    source
                );
            }
            _ => panic!("Expected EngineError::Arrow, but got different error variant: {:?}", err),
        }
    }

    #[test]
    fn test_execute_projection_build_error_due_to_unprimed_schema() {
        let logical_plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            projection_indices: vec![2, 0, 1],
        };
        let build_result = build_physical_plan(logical_plan);
        let err = build_result.err().unwrap();

        assert!(
            matches!(
                err,
                ActualEngineError::Projection{ ref message } if message == "Cannot project specific columns (non-empty projection_indices) from an empty input schema."
            ),
            "Unexpected error variant or message. Expected Projection error with specific message, got: {:?}",
            err
        );
    }

    #[test]
    fn test_execute_projection_project_nothing_successfully() {
        let logical_plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan { table_name: "test_table".to_string() }),
            projection_indices: vec![],
        };
        let physical_plan = build_physical_plan(logical_plan)
            .expect("Build for project nothing should succeed");

        let results = execute_query(physical_plan).expect("Exec failed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].num_rows(), 4);
        assert_eq!(results[1].num_rows(), 2);
        assert_eq!(results[0].schema().fields().len(), 0);
    }
}
