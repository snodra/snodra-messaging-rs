fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/hub.proto");
    // The server side is generated too: integration tests run an in-process fake hub.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The token and the JWT travel in `Credentials`: a derived `Debug` would print them in any
        // `{:?}` of a `Register` or `ClientMessage`. `src/proto.rs` writes it by hand, with `"***"`.
        .skip_debug([".snodra.hub.v1.Credentials"])
        .build_transport(false) // the `Connect` rpc would clash with the generated transport `connect`; channels are built via Endpoint
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
