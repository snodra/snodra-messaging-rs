# syntax=docker/dockerfile:1
# Library crate: this image only proves the crate builds in CI. It is not deployed.
# The workspace names the add-ons under crates/ and the examples as members, so the manifest needs
# them; the lock file pins what is built (--locked), as rust-toolchain.toml pins the image's
# toolchain. Only the core crate is built.
FROM rust:1.99.1-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY crates ./crates
COPY examples ./examples
RUN cargo build --release --locked -p snodra-messaging-rs

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/libsnodra_messaging.rlib /app/
USER nonroot
CMD ["/bin/true"]
