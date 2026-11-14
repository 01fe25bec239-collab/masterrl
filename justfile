# MasterRL dev tasks. Run `just` to list them.
# Toolchain steps skip with a message until that toolchain's manifest exists.

set shell := ["bash", "-euo", "pipefail", "-c"]

compose := "docker compose -f deploy/local/compose.yaml"

# Dev-only database from deploy/local/compose.yaml; override by exporting DATABASE_URL.
export DATABASE_URL := env("DATABASE_URL", "postgres://mrl:mrl-dev-only@127.0.0.1:55432/masterrl")

[private]
default:
    @just --list

# Format all code
fmt: (_cargo "fmt" "--all") (_pnpm "run" "--if-present" "format") (_uv "run" "ruff" "format" ".")

# Lint all code
lint: (_cargo "clippy" "--workspace" "--all-targets" "--" "-D" "warnings") (_pnpm "run" "--if-present" "lint") (_uv "run" "ruff" "check" ".")

# Run unit tests
test: (_cargo "test" "--workspace") (_pnpm "run" "--if-present" "test") (_uv "run" "pytest")

# Run integration tests against DATABASE_URL (Rust: #[ignore]d tests, Python: -m integration)
test-integration: (_cargo "test" "--workspace" "--" "--ignored") (_pnpm "run" "--if-present" "test:integration")
    @if [ -f pyproject.toml ]; then uv run pytest -m integration || [ $? -eq 5 ]; else echo "skipped: uv not set up yet"; fi

# Regenerate code: the guidance catalog, then every executable in tools/gen/
gen:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f guidance/tools/build.py ]; then uv run --script guidance/tools/build.py; else echo "skipped: guidance not set up yet"; fi
    shopt -s nullglob
    ran=0
    for g in tools/gen/*; do
        if [ -f "$g" ] && [ -x "$g" ]; then echo "gen: $g"; "$g"; ran=1; fi
    done
    [ "$ran" = 1 ] || echo "skipped: no generators in tools/gen yet"

# Start Postgres, SeaweedFS (S3) and the OpenTelemetry collector; waits until healthy
dev:
    {{compose}} up -d --build --wait

# Stop the local stack (data volumes are kept)
dev-down:
    {{compose}} down

# Run end-to-end tests (starts the local stack first)
e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f package.json ]; then echo "skipped: pnpm not set up yet"; exit 0; fi
    just dev
    pnpm run --if-present e2e

# Fail on unfinished-work markers in tracked files
audit-placeholders:
    uv run --script tools/audit/placeholders.py

# Fail if this branch edits paths outside a part, e.g. `just audit-ownership PART=P04 BASE=wave-00-done`
audit-ownership *ARGS:
    uv run --script tools/audit/ownership.py {{ARGS}}

# Check the guidance catalog
guidance-check:
    @if [ -f guidance/tools/build.py ]; then uv run --script guidance/tools/build.py --check; else echo "skipped: guidance not set up yet"; fi

# Print the requirement -> tests report (needs the local registry)
traceability:
    uv run --script tools/traceability/report.py

[private]
_cargo +args:
    @if [ -f Cargo.toml ]; then cargo {{args}}; else echo "skipped: cargo not set up yet"; fi

[private]
_pnpm +args:
    @if [ -f package.json ]; then pnpm {{args}}; else echo "skipped: pnpm not set up yet"; fi

[private]
_uv +args:
    @if [ -f pyproject.toml ]; then uv {{args}}; else echo "skipped: uv not set up yet"; fi
