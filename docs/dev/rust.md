# Rust

Toolchain: Rust 1.100.0, edition 2021, pinned in `rust-toolchain.toml` (rustup installs it on first use). Tools: `cargo install --locked cargo-nextest@0.9.152 cargo-deny@0.20.2`.

## Commands

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo deny check
cargo build --workspace --release
cargo run -p mrl-server -- --version
```

## Crates

- `mrl-domain` — pure domain types: IDs, enums, state machines, validation. No IO.
- `mrl-proto` — RPC types generated from `proto/`.
- `mrl-server` — the core binary: API, scheduler, registries, events.
- `mrl-agent` — the machine agent binary that runs training jobs.
- `mrl-cli` — the `mrl` command-line client.
- `mrl-testkit` — shared test helpers, used only from tests.

## Dependency rules (checked in CI, `ci-rust.yml`)

- `mrl-domain` has no IO dependencies: no tokio, sqlx, tonic, hyper, axum, mio or reqwest.
- `mrl-agent` never depends on `mrl-server` (only on `mrl-domain` and `mrl-proto`).
- `unsafe_code` is forbidden; clippy warnings fail the build.

## Pinned versions (`[workspace.dependencies]`)
- async/web: tokio 1.54.1, axum 0.8.9, tower 0.5.3, tower-http 0.7.1, hyper 1.12.0
- RPC: tonic 0.14.9, prost 0.14.6
- database: sqlx 0.9.0 (postgres, runtime-tokio, tls-rustls, uuid, time, json, migrate, macros)
- serialization/errors: serde 1.0.229, serde_json 1.0.154, thiserror 2.0.21, anyhow 1.0.104
- observability: tracing 0.1.44, tracing-subscriber 0.3.23, opentelemetry 0.33.1, opentelemetry-otlp 0.33.1
- IDs/time: uuid 1.29.2 (v7), time 0.3.55
- API/storage: utoipa 6.0.0, object_store 0.14.2
- crypto/TLS: sha2 0.11.1, blake3 1.8.7, rustls 0.23.46, rcgen 0.14.10, chacha20poly1305 0.11.0, argon2 0.6.0
- SSH/keychain: russh 0.65.1, keyring 4.2.0
- system info: sysinfo 0.40.3, nvml-wrapper 0.13.0
- testing: proptest 1.12.0, insta 1.49.0
- build info: git-version 0.3.9

`Cargo.lock` holds the exact resolved versions. Use a pin with `name = { workspace = true }`.

## Targets and CI

- Targets: macOS (`aarch64-apple-darwin`, `x86_64-apple-darwin`), Windows (`x86_64-pc-windows-msvc`), Linux (`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`; the agent's x86_64 build runs inside WSL2).
- CI: Linux on every PR; macOS and Windows on pushes to `main` and manual runs.
