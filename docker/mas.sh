#!/bin/bash
set -eo pipefail

# Wrapper for the MAS provisioning smoke test, invoked by compliance.yml the way
# docker/complement.sh is. The matrix cell (cargo_profile, rust_toolchain, ...)
# arrives via the environment and selects the mas-testee image in the runner.

BASEDIR=$(dirname "$0")

set -a
cargo_profile="${cargo_profile:-test}"
feat_set="${feat_set:-all}"
rust_toolchain="${rust_toolchain:-nightly}"
rust_target="${rust_target:-x86_64-unknown-linux-gnu}"
sys_name="${sys_name:-debian}"
sys_target="${sys_target:-x86_64-v1-linux-gnu}"
sys_version="${sys_version:-testing-slim}"
results_dir="${results_dir:-tests/mas}"
set +a

if test "${CI_VERBOSE_ENV:-false}" = "true"; then
	date
	env
fi

exec "$BASEDIR/lib/mas-runner.sh"
