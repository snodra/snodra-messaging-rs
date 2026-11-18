# proto/

`hub.proto` is a verbatim copy of `proto/hub.proto` in the [Snodra Server](https://github.com/snodra/snodra-server)
repository, which owns the protocol.

The server repository is the source of truth: do not edit the copy here. Change the protocol there, then re-sync this file.
`scripts/check-proto-sync.sh` (the `proto-sync` CI job) compares the two byte for byte, cloning the public server repository
anonymously; locally it compares with `../snodra-server/proto/hub.proto` when that checkout exists.

```sh
cp ../snodra-server/proto/hub.proto proto/hub.proto
```
