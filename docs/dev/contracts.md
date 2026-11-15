# Contracts: protos and JSON Schemas

Every wire format and document format MasterRL uses lives in two folders:

| Folder | What | Generated from it |
|---|---|---|
| `proto/mrl/<area>/v1/` | Internal RPC: `common`, `agent` (core ↔ agent), `worker` (harness ↔ agent), `reward`, `env` | Rust: `crates/mrl-proto` (at build time). Python: `mrl_runtime._generated`, `mrl_rewards._generated` (committed) |
| `schemas/` | JSON Schemas (draft 2020-12) for datasets, manifests and config documents | nothing; read at runtime |

Later parts implement these contracts. They do not redefine them.

## Commands

```bash
just gen                                          # regenerate Python code and goldens
uv run --script tests/contracts/check_schemas.py  # schemas + examples
uv run pytest tests/contracts                     # Python golden round-trip
cargo nextest run -p mrl-proto                    # Rust golden round-trip
buf lint proto                                    # proto style (buf 1.75.0)
```

CI (`ci-contracts`, check name `contracts`) runs all of these, `buf breaking` against the
base commit, and fails if `just gen` changes any file.

## Using the generated code

- Rust: `mrl_proto::worker::v1::WorkerMessage`, `mrl_proto::agent::v1::agent_gateway_service_server::AgentGatewayService`, and so on. One module per proto package.
- Python: `from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2, worker_control_pb2_grpc`.
  `mrl_runtime` holds common, worker, reward and env; `mrl_rewards` holds common, reward and env.
  The agent gateway is Rust only.
- `Failure.class` is a Python keyword: read it with `getattr(failure, "class")`, build with
  `Failure(**{"class": ...})`. The `.pyi` stub leaves that one field out.

## Rules for changing a proto

`v1` is additive only. `buf breaking` (category `FILE`) enforces it.

- Add new fields with new numbers. Never renumber, retype or rename a field, enum value or message.
- To retire a field, stop using it and add `reserved <number>; reserved "<name>";`.
- Every enum starts with `<ENUM>_UNSPECIFIED = 0`; values carry the enum name as prefix.
  `FailureClass` numbers are fixed forever (`mrl-domain` mirrors them).
- A new oneof arm is fine; receivers must treat an unknown arm as "ignore and log".
- Anything that cannot be additive goes in a new package `v2`.
- Lint is buf `STANDARD`. The only exceptions (in `proto/buf.yaml`) let the two session
  streams use named envelopes (`WorkerMessage`/`AgentMessage`, `AgentMessage`/`CoreMessage`).

Times are `google.protobuf.Timestamp`. IDs are prefixed strings (`job_…`, `node_…`).
Free-form config travels as JSON text in `*_json` fields.

## Rules for changing a schema

- `$id` is `https://schemas.masterrl.dev/<path under schemas/>`.
- Objects are closed (`additionalProperties: false`). An object that must stay open says why
  in `"x-mrl-open": "<reason>"`; the checker rejects any other open object.
- Record rules JSON Schema cannot express are listed in `"x-mrl-rules"` (`role_order`,
  `chosen_equals_rejected`, `length_mismatch`, `duplicate_path`). Validators must implement
  each listed rule; `tests/contracts/check_schemas.py` holds the reference version.
- A `v1` schema only gains optional fields or relaxes a rule. Anything else is `v2`.
- Every schema needs examples. Datasets: `tests/fixtures/datasets/<id>.jsonl` and
  `<id>-invalid.jsonl` with `DEFECTS.json` (or the same under `tests/contracts/examples/datasets/`).
  Others: `tests/contracts/examples/<path>/valid-*.json`, plus `invalid-*.json` files that patch
  one valid document and name where it must fail (`fails_at`).

## Goldens

`tools/gen/proto.py` writes one binary golden per message (one per arm of its oneof) with every
field set, plus `descriptors.binpb`. Rust and Python both decode each golden and must encode
the same bytes again. Maps hold one entry, because map order on the wire is not specified
and upb and prost order entries differently.
