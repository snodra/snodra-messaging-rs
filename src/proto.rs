//! gRPC types generated from `proto/hub.proto` (package `kraken.hub.v1`).
tonic::include_proto!("kraken.hub.v1");

// `Credentials` and its `kind` oneof are generated without `Debug` (see `build.rs`): the token and
// the JWT must never reach a log through a `{:?}` of a `Register` or a `ClientMessage`.
impl std::fmt::Debug for credentials::Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            credentials::Kind::Token(_) => "Token",
            credentials::Kind::Jwt(_) => "Jwt",
        };
        f.debug_tuple(kind).field(&"***").finish()
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("kind", &self.kind)
            .finish()
    }
}
