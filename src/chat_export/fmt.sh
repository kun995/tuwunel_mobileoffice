#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)

docker run --rm \
	-e RUSTUP_TOOLCHAIN=1.95.0 \
	-v "$repo_root:/workspace" \
	-w /workspace \
	rust:1.95.0-bookworm \
	sh -eu -c '
		/usr/local/cargo/bin/rustup component add rustfmt
	find src/chat_export -type f -name "*.rs" -print0 | xargs -0 /usr/local/cargo/bin/rustfmt --edition 2024
	'
