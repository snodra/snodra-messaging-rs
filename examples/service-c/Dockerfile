# syntax=docker/dockerfile:1
# Context = the kraken-messaging-rs repo root (the crate is compiled from source).
#   docker build -f examples/service-c/Dockerfile -t kraken-examples-service-c:dev .
FROM rust:1.99.1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY crates ./crates
COPY examples ./examples
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p service-c && cp target/release/service-c /service-c

FROM debian:bookworm-slim
# No OpenSSL: rdkafka is built without `ssl`, mongodb and reqwest use rustls. CA certificates for a JWKS over https.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /service-c /app/service-c
ENV RUST_LOG=info
ENTRYPOINT ["/app/service-c"]
