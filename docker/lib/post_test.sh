#!/bin/bash
# Invoked by Complement per spawned homeserver after each test, before the
# container is removed. Args: container_id, test_name, test_failed. Extracts
# /var/log/tuwunel/metrics from the stopped testee via the docker engine
# unix-socket API and lands the dump files under
# /runtime_metrics/by_test/<test_name>/<container_id>/ for the runner to tar.
set -eo pipefail

cid="$1"
test_name="${2:-_unknown}"
test_name_safe="${test_name//\//__}"

dst="/runtime_metrics/by_test/$test_name_safe/$cid"
mkdir -p "$dst"

curl --silent --fail --unix-socket /var/run/docker.sock \
    --output - \
    "http://localhost/containers/$cid/archive?path=/var/log/tuwunel/metrics" \
    | tar -x --strip-components=1 -C "$dst" 2>/dev/null \
    || rmdir "$dst" 2>/dev/null || true
