#!/bin/bash
set -eo pipefail

BASEDIR=$(dirname "$0")

default_cargo_profile="bench"
default_feat_set="logging"
default_rust_toolchain="nightly"
default_rust_target="x86_64-unknown-linux-gnu"
default_sys_name="debian"
default_sys_target="x86_64-v3-linux-gnu"
default_sys_version="testing-slim"

default_complement_crypto_verbose=0
default_complement_crypto_count=1
default_complement_crypto_parallel=1
default_complement_crypto_shuffle=1337
default_complement_crypto_timeout="1h"
default_complement_crypto_run=".*"
default_complement_crypto_client_matrix="rr"

# mitmproxy is pulled by complement-crypto's deploy code via testcontainers-go
# the first time a test calls Deploy. Pre-pull here to avoid mid-run dockerhub
# rate-limit hits; pinned to the same version upstream's CI uses.
mitmproxy_image="mitmproxy/mitmproxy:10.1.5"

run="${1:-$default_complement_crypto_run}"

# Three upstream tests are nondeterministic against the exact-match results
# gate. TestOnRejoinBob races the rust SDK timeline (the backpaginated event
# is intermittently absent at lookup; upstream marks the spot with a TODO).
# TestDelayedInviteResponse self-skips on a decrypt miss (matrix-rust-sdk#3622)
# and otherwise passes, so it flips between skip and pass run to run.
# TestChangingDeviceAfterInviteReEncrypts intermittently fails the gate run to
# run.
skip="TestOnRejoinBobCanSeeButNotDecryptHistoryInPublicRoom|TestDelayedInviteResponse|TestChangingDeviceAfterInviteReEncrypts"

set -a
cargo_profile="${cargo_profile:-$default_cargo_profile}"
feat_set="${feat_set:-$default_feat_set}"
rust_target="${rust_target:-$default_rust_target}"
rust_toolchain="${rust_toolchain:-$default_rust_toolchain}"
sys_name="${sys_name:-$default_sys_name}"
sys_target="${sys_target:-$default_sys_target}"
sys_version="${sys_version:-$default_sys_version}"

runner_name=$(echo $RUNNER_NAME | cut -d"." -f1)
runner_num=$(echo $RUNNER_NAME | cut -d"." -f2)
set +a

# Outer env-var names retain the `_crypto_` infix (as referenced from
# .github/workflows/test.yml). Inside the tester container the names drop
# that infix so uwu.sh is shared verbatim with the non-crypto flavour, so
# the docker `-e` flags translate the names at the container boundary.
envs=""
envs="$envs -e complement_count=${complement_crypto_count:-$default_complement_crypto_count}"
envs="$envs -e complement_parallel=${complement_crypto_parallel:-$default_complement_crypto_parallel}"
envs="$envs -e complement_shuffle=${complement_crypto_shuffle:-$default_complement_crypto_shuffle}"
envs="$envs -e complement_timeout=${complement_crypto_timeout:-$default_complement_crypto_timeout}"
envs="$envs -e complement_skip=${complement_crypto_skip:-$skip}"
envs="$envs -e complement_run=${1:-$default_complement_crypto_run}"
envs="$envs -e COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=${complement_crypto_client_matrix:-$default_complement_crypto_client_matrix}"
envs="$envs -e COMPLEMENT_ALWAYS_PRINT_SERVER_LOGS=1"
envs="$envs -e COMPLEMENT_DESTROY_HS_TIMEOUT_SECS=10"

flavor="complement-crypto"
tester_image_prefix="complement-crypto-tester"
container_name_prefix="complement_crypto_tester"
src_root="/usr/src/complement-crypto"
results_dir="tests/complement-crypto"
addons_path="/usr/src/complement-crypto/tests/mitmproxy_addons"
export flavor tester_image_prefix container_name_prefix src_root results_dir
export envs run mitmproxy_image addons_path

exec "$BASEDIR/lib/complement-runner.sh"
