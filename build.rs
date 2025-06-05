// build.rs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/compute.proto"); // Rerun if .proto file changes
    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // Generate client code
        .compile(
            &["proto/compute.proto"], // Source .proto files
            &["proto"],               // Include path for .proto files
        )?;
    Ok(())
}
