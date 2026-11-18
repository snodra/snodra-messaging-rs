//! The vendored proto must stay byte-identical to the server's `proto/hub.proto`.
use std::path::Path;

#[test]
fn vendored_proto_matches_server() {
    let candidates = [
        "../snodra-server/proto/hub.proto",
        "../../../snodra-server/proto/hub.proto",
    ];
    let Some(server) = candidates.iter().map(Path::new).find(|p| p.exists()) else {
        eprintln!("server checkout not found next to this repo, skipping proto drift check");
        return;
    };
    let vendored = std::fs::read("proto/hub.proto").expect("vendored proto");
    let source = std::fs::read(server).expect("server proto");
    assert!(
        vendored == source,
        "proto/hub.proto drifted from {}: run `cp {} proto/hub.proto`",
        server.display(),
        server.display()
    );
}
