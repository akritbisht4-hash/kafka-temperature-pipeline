# Multi-stage Docker build: compiles Rust binaries in a compatible
# Bookworm environment, eliminating host-vs-container glibc version mismatches.
FROM rust:1.80-slim-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

# Minimal runtime image
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/producer /app/producer
COPY --from=builder /app/target/release/consumer /app/consumer
RUN mkdir -p /app/output
CMD ["./producer"]
