#!/bin/bash
set -eo pipefail

# MAS provisioning smoke test (Target A). Stands up postgres + MAS + tuwunel on a
# private bridge network, drives the real mas-cli against tuwunel's
# /_synapse/mas/* endpoints, and asserts the provisioning round-trip end to end.
#
# Three processes reach each other by network alias: postgres (MAS's store), mas
# (the mas-cli driver plus a background worker), tuwunel (the system under test).
# A per-run bridge network gives container DNS and isolates host ports so
# concurrent jobs on the shared box do not collide. The build phase still uses
# host networking; only this run phase uses the bridge.

# Pinned images (explicit versions, never floating tags).
MAS_IMAGE="${MAS_IMAGE:-ghcr.io/element-hq/matrix-authentication-service:1.19.0-debug}"
PG_IMAGE="${PG_IMAGE:-postgres:16}"
CURL_IMAGE="${CURL_IMAGE:-curlimages/curl:8.11.1}"

# Shared secret: verbatim-compared against MAS's bearer in the handler (no HMAC).
# MUST equal the mas_secret baked into docker/Dockerfile.mas.
MAS_SECRET="${MAS_SECRET:-tuwunel-mas-smoke-secret-0123456789ab}"
SERVER_NAME="tuwunel.test"
ALICE="alice"
ALICE_PW="smoke-test-password-9f3c1a"
ALICE_DEVICE="MASSMOKEDEVICE"

# Matrix cell -> testee image name (defaults mirror docker/complement.sh so the
# name matches what `docker buildx bake mas-testee` produced).
cargo_profile="${cargo_profile:-test}"
feat_set="${feat_set:-all}"
rust_toolchain="${rust_toolchain:-nightly}"
rust_target="${rust_target:-x86_64-unknown-linux-gnu}"
sys_name="${sys_name:-debian}"
sys_target="${sys_target:-x86_64-v1-linux-gnu}"
sys_version="${sys_version:-testing-slim}"
testee_image="${MAS_TESTEE_IMAGE:-mas-testee--${cargo_profile}--${rust_toolchain}--${rust_target}--${feat_set}--${sys_name}--${sys_version}--${sys_target}}"

CI="${CI:-false}"
results_dir="${results_dir:-tests/mas}"

# Run-id: unique per runner registration, stable across a re-run on the same
# registration (so the prior attempt's resources self-reclaim).
run_seed="${RUNNER_NAME:-local-$$}"
run_id=$(printf '%s' "${run_seed}-mas" | sha1sum | cut -c1-12)
net="mas-net-${run_id}"
pg="pg-${run_id}"
hs="tuwunel-${run_id}"
mas="mas-${run_id}"

mkdir -p "$results_dir"

log()  { printf '\033[1;36m[mas]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;41;37mERROR\033[0m %s\n' "$*" >&2; exit 1; }

capture_logs() {
	docker logs "$hs"  >"$results_dir/tuwunel.log" 2>&1 || true
	docker logs "$mas" >"$results_dir/mas.log"     2>&1 || true
}

teardown() {
	docker rm -f "$pg" "$hs" "$mas" >/dev/null 2>&1 || true
	docker network rm "$net"        >/dev/null 2>&1 || true
}

trap 'rc=$?; capture_logs; teardown; exit $rc' EXIT
trap 'fail "interrupted"' INT

# Pre-reclaim any prior attempt on this registration.
teardown

# Pull pinned externals (best-effort; a cache hit on re-run).
docker pull -q "$PG_IMAGE"   || true
docker pull -q "$MAS_IMAGE"  || true
docker pull -q "$CURL_IMAGE" || true

docker network create "$net" >/dev/null

# postgres
log "starting postgres"
docker run -d --name "$pg" --network "$net" --network-alias postgres \
	-e POSTGRES_USER=mas -e POSTGRES_PASSWORD=mas -e POSTGRES_DB=mas \
	"$PG_IMAGE" >/dev/null
for i in $(seq 1 30); do
	docker exec "$pg" pg_isready -U mas -d mas >/dev/null 2>&1 && break
	[ "$i" = 30 ] && fail "postgres not ready"
	sleep 1
done

# tuwunel (mas-testee)
log "starting tuwunel ($testee_image)"
docker run -d --name "$hs" --network "$net" --network-alias tuwunel \
	"$testee_image" >/dev/null
for i in $(seq 1 30); do
	docker run --rm --network "$net" "$CURL_IMAGE" \
		-sf http://tuwunel:8008/_matrix/client/versions >/dev/null 2>&1 && break
	[ "$i" = 30 ] && fail "tuwunel not ready"
	sleep 1
done

# MAS toolbox container, kept alive; subcommands are exec'd in.
log "starting MAS toolbox ($MAS_IMAGE)"
docker run -d --name "$mas" --network "$net" --network-alias mas \
	--entrypoint sh "$MAS_IMAGE" -c 'sleep 86400' >/dev/null

# Base config (random secrets) plus a static overlay carrying the database and
# matrix sections. MAS merges env vars as the base then admerges YAML files on
# top, so the secret lives in the overlay YAML, not an env var. The MAS image
# runs as a non-root user, so configs live under /tmp (the fs root is read-only).
docker exec "$mas" mas-cli config generate -o /tmp/config.yaml
write_overlay() { # $1=secret  $2=dest
	docker exec -i "$mas" sh -c "cat > $2" <<-YAML
		database:
		  uri: postgres://mas:mas@postgres/mas
		matrix:
		  kind: synapse
		  homeserver: ${SERVER_NAME}
		  secret: "$1"
		  endpoint: http://tuwunel:8008/
	YAML
}
write_overlay "$MAS_SECRET"                  /tmp/overlay.yaml
write_overlay "wrong-secret-negative-probe"  /tmp/overlay-bad.yaml

cfg="-c /tmp/config.yaml -c /tmp/overlay.yaml"
masx() { docker exec "$mas" mas-cli "$@"; }

# Preflight + migrate (postgres only, no homeserver).
masx config check $cfg    || fail "config check failed"
masx database migrate $cfg || fail "database migrate failed"

# Handshake probe. doctor GETs is_localpart_available with the bearer and
# expects 400 (auth OK). It always exits 0, so gate on the failure marker.
log "doctor (handshake against tuwunel)"
doctor_out=$(masx doctor $cfg 2>&1 || true)
printf '%s\n' "$doctor_out"
printf '%s' "$doctor_out" | grep -q '❌' && fail "doctor reported a failure against tuwunel"

# Provision: register-user runs a synchronous is_localpart_available precheck
# (nonzero on auth/reachability failure) then enqueues a ProvisionUserJob. The
# actual provision_user POST runs in the worker, so the worker is mandatory.
log "register-user $ALICE"
masx manage register-user $cfg --yes --ignore-password-complexity -p "$ALICE_PW" "$ALICE" \
	|| fail "register-user failed (precheck/auth)"

log "starting worker"
docker exec -d "$mas" mas-cli worker $cfg

# PRIMARY assertion: tuwunel state. query_user returns 404 until the worker has
# provisioned, then 200 with the user_id. Asserts tuwunel's actual state.
log "asserting provisioning landed (query_user)"
ok=0
for i in $(seq 1 30); do
	body=$(docker run --rm --network "$net" "$CURL_IMAGE" \
		-s -H "Authorization: Bearer ${MAS_SECRET}" \
		"http://tuwunel:8008/_synapse/mas/query_user?localpart=${ALICE}" 2>/dev/null || true)
	case "$body" in
	*"@${ALICE}:${SERVER_NAME}"*) ok=1; break ;;
	esac
	sleep 1
done
[ "$ok" = 1 ] || fail "query_user did not return @${ALICE}:${SERVER_NAME}"
log "query_user OK: $body"

# SECONDARY assertion: device surface end to end. issue-compatibility-token
# calls upsert_device synchronously and exits nonzero on a homeserver error.
log "issue-compatibility-token (upsert_device)"
masx manage issue-compatibility-token $cfg "$ALICE" "$ALICE_DEVICE" \
	|| fail "issue-compatibility-token failed (upsert_device)"

# NEGATIVE: prove the handshake is enforced. doctor with the wrong secret must
# report 403; tuwunel must 403 a wrong bearer directly.
log "negative: wrong secret must be rejected"
neg_out=$(masx doctor -c /tmp/config.yaml -c /tmp/overlay-bad.yaml 2>&1 || true)
printf '%s\n' "$neg_out"
printf '%s' "$neg_out" | grep -qiE '403|forbidden|❌' \
	|| fail "negative test: doctor did not reject the wrong secret"

code=$(docker run --rm --network "$net" "$CURL_IMAGE" \
	-s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer wrong-secret-negative-probe" \
	"http://tuwunel:8008/_synapse/mas/query_user?localpart=${ALICE}" 2>/dev/null || true)
[ "$code" = 403 ] || fail "tuwunel did not 403 a wrong bearer (got '$code')"

printf '{"target":"A","outcome":"pass","user":"@%s:%s","device":"%s"}\n' \
	"$ALICE" "$SERVER_NAME" "$ALICE_DEVICE" >"$results_dir/results.jsonl"
log "MAS smoke test PASSED"
printf '\033[1;42;30mACCEPT\033[0m\n'
