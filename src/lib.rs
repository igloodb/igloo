// src/lib.rs

// Declare the top-level modules of the library
pub mod errors;      // For the existing src/errors.rs
pub mod execution;   // For the new src/execution/mod.rs

// Potentially re-export items from errors if needed, e.g.:
// pub use errors::SomeTopLevelError;

// Re-export key components from the execution module for easier top-level access
// This makes them available as `igloo_engine::EngineError` etc. if `igloo_engine` is the crate name.
pub use execution::{
    EngineError,
    ExecutionOperator,
    LogicalPlan,
    build_physical_plan,
    execute_query,
};
