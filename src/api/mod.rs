// This will look for a file named `igloo.worker.rs` in the OUT_DIR,
// which `tonic-build` places in `src/api` as configured in build.rs.
pub mod worker {
    tonic::include_proto!("igloo.worker"); // Matches the package name in .proto
}

// Optional: re-export for convenience. Adjust as necessary once build.rs runs.
// It's good practice to re-export these from a higher level module if they are broadly used.
pub use worker::{TaskDefinition, TaskResult};
// pub use worker::worker_client::WorkerClient; // Re-export client
// pub use worker::worker_server::{WorkerServer, Worker as WorkerServiceTrait}; // Re-export server traits
