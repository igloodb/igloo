// src/compute/rpc.rs

// This will include the Rust code generated from compute.proto
// The name `compute_rpc` corresponds to the `package compute_rpc;` line in the .proto file.
// Adjust if your package name is different.
pub mod generated {
    tonic::include_proto!("compute_rpc");
}

// Optional: Re-export commonly used types for convenience
pub use generated::{
    compute_executor_client::ComputeExecutorClient, // Client
    compute_executor_server::{ComputeExecutor, ComputeExecutorServer}, // Server traits
    TaskOutcome, TaskRequest, SubmitTaskResponse, GetResultRequest // Messages
};
