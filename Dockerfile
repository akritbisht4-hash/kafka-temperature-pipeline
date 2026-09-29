# Runtime-only: binaries are built locally via `cargo build --release`
# (avoids pulling the large rust image on slow/flaky networks).
FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY target/release/producer /app/producer
COPY target/release/consumer /app/consumer
RUN mkdir -p /app/output
CMD ["./producer"]
