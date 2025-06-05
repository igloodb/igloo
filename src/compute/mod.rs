// src/compute/mod.rs
pub mod api;
pub mod errors;
pub mod registry;
pub mod scheduler;
pub mod rpc; // Added

pub use api::{ComputeService, TaskDefinition, TaskFuture, TaskResult};
pub use errors::ComputeError;
pub use registry::{FunctionRegistry, RegisteredFunction};
pub use scheduler::LocalTaskScheduler; // Added
