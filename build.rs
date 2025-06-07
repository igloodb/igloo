fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // Generate client code
        .out_dir("src/api") // Output directory for generated Rust code
        .compile(
            &["src/api/proto/worker.proto"], // Path to .proto file
            &["src/api/proto"], // Include path for proto imports
        )?;
    Ok(())
}
