# syntax=docker/dockerfile:1
# Library crate: this image only proves the crate builds and tests in CI. It is not deployed.
FROM rust:1.99-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/libkraken_messaging.rlib /app/
USER nonroot
CMD ["/bin/true"]
