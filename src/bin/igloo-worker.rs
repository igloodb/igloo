// Assuming the crate name in Cargo.toml is "igloo"
use igloo::worker::executor::WorkerServiceImpl;
use igloo::api::worker::worker_server::WorkerServer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging. You can customize this further.
    // RUST_LOG=info can be set in the environment.
    // If not set, defaults to a basic level (e.g., error).
    // For explicit control:
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let addr_str = "0.0.0.0:50051"; // Default worker address
    let addr = addr_str.parse()?;

    let worker_service = WorkerServiceImpl::default();

    log::info!("Igloo Worker gRPC server starting on {}", addr_str);

    Server::builder()
        .add_service(WorkerServer::new(worker_service))
        .serve(addr)
        .await?;

    log::info!("Igloo Worker gRPC server shut down.");
    Ok(())
}
