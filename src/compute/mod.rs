// src/compute/mod.rs
pub mod api;
pub mod errors;
pub mod registry;
pub mod rpc;
pub mod scheduler; // Added

pub use api::{ComputeService, TaskDefinition, TaskFuture, TaskResult};
pub use errors::ComputeError;
pub use registry::{FunctionRegistry, RegisteredFunction};
pub use scheduler::LocalTaskScheduler; // Added
