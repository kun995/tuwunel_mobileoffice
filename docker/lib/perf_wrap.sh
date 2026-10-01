#!/bin/bash
# Optional perf-stat wrapper for the Complement testee. It sits unconditionally
# in the entrypoint chain (like sched_wrap.sh) and is a transparent exec unless
# TESTEE_PERF=1 is set in the environment, so an ordinary testee carries it at
# no cost. Complement delivers the toggle into the spawned testee through its
# COMPLEMENT_SHARE_ENV_PREFIX passthrough; the runner sets it for a perf run only.
# The name is deliberately not TUWUNEL_-prefixed: tuwunel reads every
# CONDUIT_/CONDUWUIT_/TUWUNEL_ env var as a config key, so a prefixed toggle
# would abort the testee under its error_on_unknown_config_opts.
#
# When enabled it runs the server under `perf stat` as a background child and
# forwards the container stop signal to the server (perf's child) so the server
# shuts down gracefully: its runtime-metrics dump fires on Drop, and perf then
# flushes its counter report once the workload exits. Running the server directly
# under perf as PID 1 loses both, because perf does not propagate the stop signal
# to its workload: the server is killed ungracefully on the grace timeout and
# perf writes an empty report.
#
# The counter report lands in the runtime-metrics directory so the per-test
# extraction in post_test.sh collects it alongside the JSON dumps.
set -eo pipefail

# Opt-in. With the toggle unset this is a plain exec, matching sched_wrap.sh.
if test "${TESTEE_PERF:-0}" != "1"; then
	exec "$@"
fi

# Degrade to an unwrapped run when perf is missing or perf_event_open is denied
# (no CAP_PERFMON, or a restrictive perf_event_paranoid), so a misconfigured perf
# run still exercises the server instead of failing to boot it.
if ! command -v perf >/dev/null 2>&1 \
|| ! perf stat -x, -e instructions -- true >/dev/null 2>&1; then
	echo "perf_wrap: perf unavailable, running without it" >&2
	exec "$@"
fi

out="${TESTEE_PERF_STAT_OUT:-/var/log/tuwunel/metrics/tuwunel.perf_stat.csv}"

perf stat -ddd -x, --output "$out" -- "$@" &
perf_pid=$!

forward() {
	local kids
	kids=$(cat "/proc/$perf_pid/task/$perf_pid/children" 2>/dev/null) || kids=""

	for kid in $kids; do
		kill -TERM "$kid" 2>/dev/null || true
	done
}
trap forward TERM INT

# wait returns early when a trapped signal arrives; loop until perf truly exits
# so its counter report is fully flushed before this PID exits.
ec=0
wait "$perf_pid" 2>/dev/null || ec=$?
while kill -0 "$perf_pid" 2>/dev/null; do
	wait "$perf_pid" 2>/dev/null || ec=$?
done

exit "$ec"
