#!/bin/sh
# Tests for bump-version.sh against copies of this repository's files: sh scripts/bump-version.test.sh
set -u
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
failed=0

fail() {
  echo "FAIL $*"
  failed=1
}

# The published crates: the core and every add-on.
crates=snodra-messaging-rs
for m in "$root"/crates/*/Cargo.toml; do
  crates="$crates $(awk '/^\[/ { p = ($0 == "[package]") } p && /^name = "/ { gsub(/^name = "|"$/, ""); print; exit }' "$m")"
done
ncrates=$(echo $crates | wc -w | tr -d ' ')

mkdir -p "$work/scripts" "$work/crates"
cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/README.md" "$work/"
cp "$root/scripts/bump-version.sh" "$work/scripts/"
for d in "$root"/crates/*/; do
  d=${d%/}
  mkdir "$work/crates/${d##*/}"
  cp "$d/Cargo.toml" "$work/crates/${d##*/}/"
done
cd "$work" || exit 1

if sh scripts/bump-version.sh 9.8.7; then echo "ok   it runs"; else fail "it runs"; fi

ws=$(awk '/^\[/ { p = ($0 == "[workspace.package]") } p && /^version = / { print; exit }' Cargo.toml)
[ "$ws" = 'version = "9.8.7"' ] && echo "ok   the workspace version" || fail "the workspace version: $ws"

dep=$(awk '/^\[/ { p = ($0 == "[workspace.dependencies]") } p && /^snodra-messaging-rs = / { print; exit }' Cargo.toml)
[ "$dep" = 'snodra-messaging-rs = { path = ".", version = "9.8.7" }' ] && echo "ok   the core dependency" \
  || fail "the core dependency: $dep"

# Every published crate inherits the workspace version, so no crate manifest carries its own.
for m in Cargo.toml crates/*/Cargo.toml; do
  pkg=$(awk '/^\[/ { p = ($0 == "[package]") } p && /^version/ { print; exit }' "$m")
  [ "$pkg" = 'version.workspace = true' ] && echo "ok   $m inherits the version" || fail "$m: $pkg"
done
# And the add-ons name the core through the workspace, never with a version of their own.
for m in crates/*/Cargo.toml; do
  if grep -E '^snodra-messaging-rs = ' "$m" | grep -v 'workspace = true' | grep -q .; then
    fail "$m names the core without the workspace"
  else
    echo "ok   $m names the core through the workspace"
  fi
done
[ "$ncrates" -ge 5 ] && echo "ok   $ncrates published crates" || fail "only $ncrates published crates: $crates"

for c in $crates; do
  lock=$(awk -v n="name = \"$c\"" 'prev == n { print; exit } { prev = $0 }' Cargo.lock)
  [ "$lock" = 'version = "9.8.7"' ] && echo "ok   Cargo.lock $c" || fail "Cargo.lock $c: $lock"
done

n=$(grep -cE '^snodra-messaging[a-z-]* *= "9\.8"' README.md)
[ "$n" -ge 1 ] && echo "ok   README.md install lines" || fail "README.md install lines"

# Only those lines change: two in Cargo.toml, one per published crate in Cargo.lock, the README's
# install lines, none in the crate manifests.
changed() { diff "$root/$1" "$1" | grep -c '^>'; }
[ "$(changed Cargo.toml)" -eq 2 ] && echo "ok   two lines in Cargo.toml" || fail "$(changed Cargo.toml) lines changed in Cargo.toml"
[ "$(changed Cargo.lock)" -eq "$ncrates" ] && echo "ok   $ncrates lines in Cargo.lock" || fail "$(changed Cargo.lock) lines changed in Cargo.lock"
readme=$(grep -cE '^snodra-messaging[a-z-]* *= "[0-9]+\.[0-9]+"' "$root/README.md")
[ "$(changed README.md)" -eq "$readme" ] && echo "ok   $readme lines in README.md" || fail "$(changed README.md) lines changed in README.md"
for m in crates/*/Cargo.toml; do
  diff -q "$root/$m" "$m" >/dev/null && echo "ok   $m unchanged" || fail "$m changed"
done

# A patch leaves the README's minor as it is, and still succeeds.
cp README.md README.before
if sh scripts/bump-version.sh 9.8.8; then echo "ok   a patch runs"; else fail "a patch runs"; fi
diff -q README.before README.md >/dev/null && echo "ok   a patch keeps the README" || fail "a patch changed the README"

# A file without the expected line fails the bump.
cp Cargo.lock Cargo.lock.good
grep -v '^name = "snodra-messaging-mongo"$' Cargo.lock.good >Cargo.lock
if sh scripts/bump-version.sh 9.9.0 2>/dev/null; then fail "a crate missing from the lock fails"; else echo "ok   a crate missing from the lock fails"; fi
cp Cargo.lock.good Cargo.lock

sed 's/^snodra-messaging-rs = { path = "\.", version = "[^"]*" }$/snodra-messaging-rs = { path = "." }/' Cargo.toml >Cargo.toml.new
mv Cargo.toml.new Cargo.toml
if sh scripts/bump-version.sh 9.9.0 2>/dev/null; then fail "a core dependency without a version fails"; else echo "ok   a core dependency without a version fails"; fi

printf '[package]\nname = "x"\nversion = "1.0.0"\n' >Cargo.toml
if sh scripts/bump-version.sh 9.9.0 2>/dev/null; then fail "a missing workspace version fails"; else echo "ok   a missing workspace version fails"; fi

if sh scripts/bump-version.sh 1.x 2>/dev/null; then fail "a malformed version fails"; else echo "ok   a malformed version fails"; fi

exit $failed
