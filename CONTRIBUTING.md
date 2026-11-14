# Contributing to MasterRL

Thanks for helping. Keep changes small and plain.

## Build from source

You need: git, [just](https://just.systems) 1.59+, Docker, Rust via rustup,
Node 26 with pnpm 11, and [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:01fe25bec239-collab/masterrl.git
cd masterrl
just dev        # start Postgres, S3 (SeaweedFS) and the OpenTelemetry collector
just test       # unit tests for Rust, TypeScript and Python
just lint
just dev-down   # stop the local stack
```

Run `just` to see every task. Steps for a toolchain that is not set up yet
print `skipped: ...` and pass.

Integration tests run with `just test-integration` against `DATABASE_URL`
(defaults to the `just dev` database). Mark them `#[ignore]` in Rust,
`@pytest.mark.integration` in Python, or put them in the `test:integration`
pnpm script.

## Branches and commits

- Branch name: `wave-NN/pPP-short-name`, for example `wave-01/p04-dev-governance`.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org):
  `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`, `test: ...`.

## Pull requests

- One PR per part. Title says what changed, 70 characters at most.
- Body is 10 lines at most, using the template: What, Why, How to test, Risk.
- CI must be green. Before pushing, run `just audit-placeholders` and
  `just audit-ownership PART=PNN`.
- No stubs, placeholders or unfinished-work markers. Pin every new version.

## How the work is run

1. A1 (orchestrator) plans a wave and writes one prompt per part.
2. A2 (builder) builds one part on its own branch and runs the checks.
3. A3 (reviewer) reviews A2's work read-only; A2 fixes every finding.
4. The loop repeats until A3 approves with no findings; then A2 opens the PR.
5. A1 merges the PRs, runs the wave exit checks and tags `wave-NN-done`.

## Security and conduct

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).
Everyone follows the [Code of Conduct](CODE_OF_CONDUCT.md).
