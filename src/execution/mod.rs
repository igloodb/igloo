// src/execution/mod.rs

// Module declarations for the `execution` crate component.

/// Defines `EngineError`, the primary error type for the execution engine.
pub mod error;

/// Defines the `ExecutionOperator` trait and its concrete implementations
/// like `ScanOperator`, `FilterOperator`, and `ProjectionOperator`.
pub mod operators;

/// Defines `LogicalPlan` (a placeholder for query language representation)
/// and the `build_physical_plan` function which translates a `LogicalPlan`
/// into a tree of `ExecutionOperator`s (a physical plan).
pub mod physical_plan;

/// Defines the `execute_query` function, which takes the root of a physical plan
/// (an `ExecutionOperator`) and executes it, collecting the resulting RecordBatches.
pub mod execution; // This refers to the file src/execution/execution.rs

// Re-export key components for easier access from outside the `execution` module
// (e.g., `crate::execution::EngineError`).

pub use self::error::EngineError;
pub use self::operators::ExecutionOperator;
pub use self::physical_plan::{build_physical_plan, LogicalPlan};
pub use self::execution::execute_query;
