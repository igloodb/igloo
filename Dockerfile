# Use specific Rust version with build dependencies
FROM rust:1.87 as builder

# Install minimal system dependencies for build
RUN apt-get update && apt-get install -y pkg-config libssl-dev build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Caching dependencies: copy only Cargo files first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Now copy the real source code
COPY . .
RUN cargo build

# Create minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Only copy what’s needed
COPY --from=builder /app/target/debug/igloo /app/igloo
COPY dummy_iceberg_cdc ./dummy_iceberg_cdc

CMD ["/app/igloo"]