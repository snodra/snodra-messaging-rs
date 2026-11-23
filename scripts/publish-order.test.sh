#!/bin/sh
# Tests for publish-order.sh: sh scripts/publish-order.test.sh (needs jq)
set -u
cd "$(dirname "$0")"
failed=0

# pkg NAME PUBLISH DEPS: one package of `cargo metadata` output. PUBLISH is null or [] (publish =
# false); DEPS is a JSON list of dependencies.
pkg() { printf '{"name":"%s","publish":%s,"dependencies":%s}' "$1" "$2" "$3"; }
# dep NAME KIND [PATH]: KIND is null (normal), "dev" or "build"; no PATH = a registry dependency.
dep() { printf '{"name":"%s","kind":%s,"path":%s}' "$1" "$2" "${3:-null}"; }
p='"/w"'

# check NAME EXPECTED METADATA: EXPECTED is the order joined by spaces, or "exit N".
check() {
  got=$(printf '%s' "$3" | sh ./publish-order.sh 2>/dev/null) || got="exit $?"
  got=$(echo $got)
  if [ "$got" = "$2" ]; then
    echo "ok   $1"
  else
    echo "FAIL $1: want '$2', got '$got'"
    failed=1
  fi
}

core=$(pkg core null "[$(dep tokio null)]")
check "the core before the add-ons" "core kafka mongo" \
  "{\"packages\":[$(pkg kafka null "[$(dep core null $p)]"),$(pkg mongo null "[$(dep core null $p),$(dep mongodb null)]"),$core]}"

check "examples are not published" "core" \
  "{\"packages\":[$core,$(pkg example '[]' "[$(dep core null $p)]")]}"

check "a dev-dependency does not order" "core kafka mongo" \
  "{\"packages\":[$core,$(pkg kafka null "[$(dep core null $p),$(dep mongo '"dev"' $p)]"),$(pkg mongo null "[$(dep core null $p),$(dep kafka '"dev"' $p)]")]}"

check "a build dependency orders" "core gen kafka" \
  "{\"packages\":[$(pkg kafka null "[$(dep core null $p),$(dep gen '"build"' $p)]"),$core,$(pkg gen null "[$(dep core null $p)]")]}"

check "a chain of three" "a b c" \
  "{\"packages\":[$(pkg c null "[$(dep b null $p)]"),$(pkg b null "[$(dep a null $p)]"),$(pkg a null '[]')]}"

check "a registry crate of a published name does not order" "kafka core" \
  "{\"packages\":[$(pkg kafka null "[$(dep core null)]"),$core]}"

check "a cycle fails" "exit 2" \
  "{\"packages\":[$(pkg a null "[$(dep b null $p)]"),$(pkg b null "[$(dep a null $p)]")]}"

check "nothing published fails" "exit 2" \
  "{\"packages\":[$(pkg example '[]' '[]')]}"

exit $failed
