#!/usr/bin/env bash
# Regenerates the SDKs' generated modules from the OpenAPI document (after openapi.sh).
set -euo pipefail
if [ -f python/mrl-sdk/scripts/generate.py ]; then
  uv run python python/mrl-sdk/scripts/generate.py
else
  echo "skipped: no Python SDK yet"
fi
pnpm --silent --filter @mrl/api-client run generate
