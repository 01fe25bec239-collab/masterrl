#!/usr/bin/env bash
# Regenerate runtimes/trainer.lock: every uv.lock pin, with CPU-only torch for Linux x86_64 and
# without bitsandbytes (CUDA only). CI and the CPU worker image install it with --require-hashes.
set -euo pipefail
cd "$(dirname "$0")/.."
pins=$(mktemp)
trap 'rm -f "$pins"' EXIT
uv export --frozen --no-dev --no-hashes --no-emit-project --no-emit-local --no-header -o "$pins" >/dev/null
uv pip compile pyproject.toml --quiet --constraint "$pins" --torch-backend cpu \
  --python-version 3.12 --python-platform x86_64-manylinux_2_28 --generate-hashes --no-annotate --emit-index-url \
  --no-emit-package mrl-runtime --no-emit-package mrl-rewards --no-emit-package bitsandbytes \
  --custom-compile-command runtimes/lock-cpu.sh -o runtimes/trainer.lock
