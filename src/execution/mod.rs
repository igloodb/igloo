// src/execution/mod.rs
pub mod error;
pub mod operators;
pub mod physical_plan;
pub mod execution; // This refers to src/execution/execution.rs

// Re-export key components for easier access
pub use self::error::EngineError;
pub use self::operators::ExecutionOperator;
pub use self::physical_plan::{build_physical_plan, LogicalPlan};
pub use self::execution::execute_query;
