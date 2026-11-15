#!/usr/bin/env bash
# Writes openapi/mrl-api-v1.yaml from the registered server modules. Run by `just gen`.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
mkdir -p "$root/openapi"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
cargo run --quiet --locked --manifest-path "$root/Cargo.toml" -p mrl-server -- openapi > "$tmp"
mv "$tmp" "$root/openapi/mrl-api-v1.yaml"
echo "openapi: wrote openapi/mrl-api-v1.yaml"
