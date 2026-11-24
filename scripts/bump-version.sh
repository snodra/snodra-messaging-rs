#!/bin/sh
# Writes a version into every file of this repository that carries it. Run from the repository root:
#
#   scripts/bump-version.sh 0.7.0
#
# Every published crate has the same version: the core and each add-on under crates/. The
# examples keep their own version and are never published.
#
# Fails, changing nothing further, when a file no longer has the line it expects, so a moved
# version is caught by the tests (scripts/bump-version.test.sh) and not by a release.
set -eu

core=snodra-messaging

version=${1:-}
# Strictly N.N.N without leading zeros, as next-version.sh computes it.
if ! printf '%s\n' "$version" | grep -Eqx '(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)' \
  || [ "$(printf '%s\n' "$version" | wc -l)" -ne 1 ]; then
  echo "usage: $0 X.Y.Z" >&2
  exit 2
fi
minor=${version%.*}

# rewrite FILE AWK-PROGRAM [NAME]: the program sets done=1 when it replaced the version.
rewrite() {
  if awk -v v="$version" -v minor="$minor" -v core="$core" -v name="${3:-}" \
    "$2"' END { exit !done }' "$1" >"$1.tmp"; then
    mv "$1.tmp" "$1"
  else
    rm -f "$1.tmp"
    echo "bump-version: no version to replace in $1${3:+ for $3}" >&2
    exit 1
  fi
}

# The workspace version, which every published crate inherits (version.workspace = true).
rewrite Cargo.toml '
  /^\[/ { ws = ($0 == "[workspace.package]") }
  ws && !done && /^version = "/ { $0 = "version = \"" v "\""; done = 1 }
  { print }'

# The add-ons depend on the core through the workspace dependency, at the same version.
rewrite Cargo.toml '
  /^\[/ { deps = ($0 == "[workspace.dependencies]") }
  deps && !done && index($0, core " = {") == 1 && sub(/version = "[^"]*"/, "version = \"" v "\"") { done = 1 }
  { print }'

# The lock file entry of every published crate (the release publishes with --locked).
crates=$core
for manifest in crates/*/Cargo.toml; do
  name=$(awk '/^\[/ { p = ($0 == "[package]") } p && /^name = "/ { gsub(/^name = "|"$/, ""); print; exit }' "$manifest")
  [ -n "$name" ] || { echo "bump-version: no package name in $manifest" >&2; exit 1; }
  crates="$crates $name"
done
for name in $crates; do
  rewrite Cargo.lock '
    prev == "name = \"" name "\"" && /^version = "/ { $0 = "version = \"" v "\""; done = 1 }
    { print; prev = $0 }' "$name"
done

# The README's install lines name the minor ("0.7"): a patch leaves them as they are.
rewrite README.md '
  /^snodra-messaging[a-z-]* *= "[0-9]+\.[0-9]+"/ { sub(/"[0-9]+\.[0-9]+"/, "\"" minor "\""); done = 1 }
  { print }'
