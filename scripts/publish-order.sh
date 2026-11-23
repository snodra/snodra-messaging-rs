#!/bin/sh
# The crates this workspace publishes, in publishing order, one per line:
#
#   cargo metadata --no-deps --format-version 1 | scripts/publish-order.sh
#
# A published crate is a workspace member without `publish = false`. Each comes after the published
# crates it depends on through a path (normal and build dependencies: a dev-dependency is not
# needed to publish). Fails when no crate is published or the dependencies form a cycle.
set -eu

# One line per published crate: its name, then the published crates it needs first.
pairs=$(jq -r '
  [.packages[] | select(.publish == null) | .name] as $published
  | .packages[] | select(.publish == null)
  | [.dependencies[] | select(.kind != "dev" and .path != null)
     | .name | select(. as $d | $published | index($d))] as $needs
  | ([.name] + ($needs | unique)) | join(" ")')

[ -n "$pairs" ] || { echo "publish-order: no published crate" >&2; exit 2; }

ordered=" "
remaining=$pairs
while [ -n "$remaining" ]; do
  left=""
  progress=0
  while read -r name needs; do
    [ -n "$name" ] || continue
    ready=1
    for d in $needs; do
      case $ordered in *" $d "*) ;; *) ready=0 ;; esac
    done
    if [ "$ready" -eq 1 ]; then
      ordered="$ordered$name "
      echo "$name"
      progress=1
    else
      left="$left$name $needs
"
    fi
  done <<EOF
$remaining
EOF
  if [ "$progress" -eq 0 ]; then
    echo "publish-order: the published crates depend on each other in a cycle:" $left >&2
    exit 2
  fi
  remaining=$left
done
