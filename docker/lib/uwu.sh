#!/bin/bash
# Complement test runner: drives `go test` and normalizes the JSON event stream
# into results.jsonl + full_output.jsonl. Deliberately not under pipefail, so a
# failing suite still produces results for the runner to gate against baseline.
env
set -eux

jq_res='{Action: .Action, Test: .Test}'
jq_sel='select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null)'
jq_tab='[.Action, .Test] | @tsv'
jq_out='select(.Test != null) | {Test: .Test, Output: .Output}'

COMPLEMENT_BASE_IMAGE="${1:-$complement_base_image}"
go test \
    -json \
    "-shuffle=$complement_shuffle" \
    "-parallel=$complement_parallel" \
    "-timeout=$complement_timeout" \
    "-count=$complement_count" \
    "-tags=$complement_tags" \
    "-skip=$complement_skip" \
    "-run=$complement_run" \
    "$complement_tests" |
    jq --unbuffered -c "." |
    tee output.jsonl |
    jq --unbuffered -c "$jq_sel | $jq_res" |
    tee results.jsonl |
    jq --unbuffered -r "$jq_tab"

jq -s -c "sort_by(.Test)[]" < results.jsonl | uniq > new_results.jsonl
jq -s -c "sort_by(.Test, .Timestamp)[] | $jq_out" < output.jsonl > full_output.jsonl
