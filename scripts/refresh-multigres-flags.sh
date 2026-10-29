#!/usr/bin/env bash
# Regenerates pkg/resource-handler/controller/shard/testdata/multigres-flags/*.txt
# from the flag registrations of a multigres checkout, by building the four
# binaries the operator runs in its pods and reading each one's --help.
#
# usage: scripts/refresh-multigres-flags.sh /path/to/multigres
set -euo pipefail

src=${1:?usage: $0 /path/to/multigres}
out="$(cd "$(dirname "$0")/.." && pwd)/pkg/resource-handler/controller/shard/testdata/multigres-flags"
bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT

# name:subcommand the operator invokes ("-" for none)
for spec in pgctld:server multipooler:- minigres:- pgbackrestd:serve; do
  name=${spec%%:*}
  sub=${spec##*:}
  (cd "$src" && go build -o "$bin/$name" "./go/cmd/$name")
  args=()
  [ "$sub" = "-" ] || args+=("$sub")
  "$bin/$name" ${args[@]+"${args[@]}"} --help 2>&1 |
    grep -oE '^ +(-[A-Za-z], )?--[a-z0-9][a-z0-9-]*' |
    grep -oE -- '--[a-z0-9][a-z0-9-]*' | sort -u >"$out/$name.txt"

  # cobra hides deprecated flags from --help, but they still parse and the
  # operator still passes them. Probe each one the operator uses.
  for flag in grpc-cert grpc-key grpc-ca; do
    probe=$("$bin/$name" ${args[@]+"${args[@]}"} "--$flag=x" --help 2>&1 || true)
    case $probe in
      *"unknown flag"*) ;;
      *) echo "--$flag" >>"$out/$name.txt" ;;
    esac
  done
  sort -u -o "$out/$name.txt" "$out/$name.txt"
done
