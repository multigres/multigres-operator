#!/usr/bin/env bash
# Fails when a test run left envtest control planes behind.
#
# Usage: scripts/check-envtest-orphans.sh <dir>
#
# <dir> is the TMPDIR the run used. envtest creates every control plane's
# certificate and data directories under TMPDIR, so the kube-apiserver and
# etcd processes of that run, and of no other, name <dir> in their
# arguments. One whose parent is PID 1 outlived the test binary that started
# it. Such processes are listed, killed, and make the script fail.
set -euo pipefail

dir=${1:?usage: $0 <TMPDIR of the test run>}

orphans=$(ps -axo pid=,ppid=,command= | awk -v dir="$dir" '
  $2 == 1 && ($3 ~ /\/kube-apiserver$/ || $3 ~ /\/etcd$/) && index($0, dir) { print $1 }
')
if [[ -z "$orphans" ]]; then
  exit 0
fi

echo "envtest control plane processes outlived their test binary:" >&2
for pid in $orphans; do
  ps -o pid=,command= -p "$pid" | cut -c1-160 >&2 || true
done
kill $orphans 2>/dev/null || true
sleep 3
kill -9 $orphans 2>/dev/null || true
exit 1
