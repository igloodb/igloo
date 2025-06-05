use arrow::record_batch::RecordBatch;
// Update to use the central EngineError
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
    // AdbcError from operators is used to check the content of EngineError::Adbc message
    use crate::execution::operators::{ScanOperator, FilterOperator, FilterPredicate, ProjectionOperator};
    use arrow::array::Int32Array;
    use arrow::compute;
    use arrow::array::BooleanArray;
    use arrow::error::ArrowError;
    use std::sync::Arc;
    // No longer need to import AdbcError specifically for matching EngineError variant name
    // use crate::execution::operators::AdbcError;


    #[test]
    fn test_execute_scan_multiple_batches() {
        let plan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
        };
        let physical_plan = build_physical_plan(plan).expect("Failed to build scan plan");
        let results = execute_query(physical_plan).expect("Query execution failed");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].num_rows(), 4);
        assert_eq!(results[1].num_rows(), 2);
    }

    #[test]
    fn test_execute_empty_table_scan() {
        let plan = LogicalPlan::Scan {
            table_name: "empty_table".to_string(),
        };
        let physical_plan = build_physical_plan(plan).expect("Failed to build scan plan for empty table");
        let results = execute_query(physical_plan).expect("Query execution for empty table failed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_execute_filter_all_manual_construction() {
        // Manually construct a FilterOperator with a predicate that filters all rows.
        // This test ensures execute_query correctly handles scenarios where all batches
        // might be filtered out or result in empty batches that are not collected.
        let scan_op_manual = ScanOperator::new("test_table");
        let filter_op_manual_res = FilterOperator::new(
            Box::new(scan_op_manual),
            Box::new(|batch: &RecordBatch| {
                // Predicate: id > 100 (filters all from test_table)
                // This predicate needs to be robust for unprimed scan schema.
                if batch.schema().fields().is_empty() {
                    // If no columns (e.g. unprimed scan), effectively filters all for this batch.
                    return Ok(BooleanArray::new_scalar(false, batch.num_rows()));
                }
                let ids = batch.column(0).as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| ArrowError::SchemaError("Expected id column for filter_all test".into()))?;
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
        let plan = LogicalPlan::Scan {
            table_name: "non_existent_table".to_string(),
        };
        let physical_plan = build_physical_plan(plan).expect("Build should succeed");
        let result = execute_query(physical_plan);

        assert!(result.is_err());
        match result.err().unwrap() {
            // Updated to match EngineError::Adbc { message: String }
            EngineError::Adbc { message } => {
                assert!(message.contains("ADBC Fetch Error: Table non_existent_table not found"));
            }
            e => panic!("Expected Adbc error, got {:?}", e),
        }
    }

    #[test]
    fn test_execute_filter_predicate_error() {
        let plan = LogicalPlan::Filter {
            input: Box::new(LogicalPlan::Scan {
                table_name: "single_col_table".to_string(),
            }),
            predicate_expr: "id_gt_1".to_string(),
        };
        let physical_plan = build_physical_plan(plan).expect("Build should succeed");
        let result = execute_query(physical_plan);

        assert!(result.is_err());
        match result.err().unwrap() {
            // Updated to match EngineError::Arrow { source: ArrowError }
            EngineError::Arrow { source } => {
                assert!(source.to_string().contains("Predicate 'id_gt_1' expects first column to be 'id', found 'col_a'"));
            }
            e => panic!("Expected Arrow error from predicate, got {:?}", e),
        }
    }

    #[test]
    fn test_execute_projection_build_error_due_to_unprimed_schema() {
        let plan = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan {
                table_name: "test_table".to_string(),
            }),
            projection_indices: vec![2, 0, 1],
        };
        let build_result = build_physical_plan(plan);
        assert!(build_result.is_err());
        match build_result.err().unwrap() {
            // Updated to match EngineError::Projection { message: String }
            EngineError::Projection{ message } => {
                assert!(message.contains("Cannot project fields from an empty input schema."));
            }
            e => panic!("Expected Projection error, got {:?}", e),
        }
    }

    #[test]
    fn test_execute_projection_project_nothing_successfully() {
        let plan_project_nothing = LogicalPlan::Projection {
            input: Box::new(LogicalPlan::Scan { table_name: "test_table".to_string() }),
            projection_indices: vec![],
        };
        let physical_plan_project_nothing = build_physical_plan(plan_project_nothing)
            .expect("Build for project nothing should succeed");

        let results_project_nothing = execute_query(physical_plan_project_nothing).expect("Exec failed");
        assert_eq!(results_project_nothing.len(), 2);
        assert_eq!(results_project_nothing[0].num_rows(), 4);
        assert_eq!(results_project_nothing[1].num_rows(), 2);
        assert_eq!(results_project_nothing[0].schema().fields().len(), 0);
    }
}
