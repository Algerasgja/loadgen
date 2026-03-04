# Multi-stage build for LoadGen
FROM 192.168.31.96:5000/base/rust:1.79 as builder

WORKDIR /usr/src/app

# Install build dependencies if needed (e.g. pkg-config, libssl-dev for reqwest)
RUN apt-get update && apt-get install -y pkg-config libssl-dev
# COPY Cargo.toml Cargo.lock ./
# COPY src ./src


# RUN cargo build --release --bin loadgen

# Runtime image
FROM 192.168.31.96:5000/base/debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y libssl3 ca-certificates && rm -rf /var/lib/apt/lists/*

# COPY --from=builder /usr/src/app/target/release/loadgen /app/loadgen
COPY  ./target/release/loadgen /app/loadgen
COPY ./dags /app/dags
COPY ./demos /app/demos
COPY ./real-world-emulation /app/real-world-emulation
COPY ./loadgen.yaml /app/loadgen.yaml

# Set default env vars
ENV RUST_LOG=info
ENV LOADGEN_CONFIG=loadgen.yaml

CMD ["./loadgen"]
