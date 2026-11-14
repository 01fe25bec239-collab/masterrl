# Python

Python 3.12, managed with uv 0.11.28.

## Layout
- `python/mrl-runtime`, `python/mrl-rewards`, `python/mrl-sdk`: the uv workspace (one `uv.lock` at the root).
- `python/adapters/mrl-adapter-{trl,lmeval,mlx}`: separate uv projects, each with its own lock.
  They may depend on `mrl-runtime` only and never import each other.
- `tools/fixtures/`: fixture generator, test-model fetcher, and the TRL spike (own uv project).
- `tests/fixtures/`: dataset fixtures (committed), `GOLDEN.json`, `test-models.yaml`.

## Checks (same as CI)
```bash
uv sync && uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run lint-imports && uv run python tools/fixtures/make_fixtures.py --check && uv run pytest
```
Adapters: `cd python/adapters/<name> && uv sync --locked && uv run lint-imports && uv run pytest`.

## Rules
- Pyright is strict for `mrl_runtime` and `mrl_rewards`, standard elsewhere.
- `mrl_runtime` must not import torch, transformers, trl or vllm (import-linter checks every import, even inside functions).
- Pin exact versions. No ML tests in CI; GPU tests run on the owner's machines.

## Fixtures
- `uv run python tools/fixtures/make_fixtures.py` builds `tests/fixtures/models/.cache/tiny-llama`
  (1M-param random Llama, safetensors) and `tiny-llama-pickle` (same weights as a torch.save-format
  `pytorch_model.bin`, for refusal tests), and rewrites the dataset files and `GOLDEN.json`. Never commit `.cache/`.
- `--check` rebuilds in a temp dir and compares with `GOLDEN.json` and the committed datasets.
  Weight hashes are compared only on a platform listed in `GOLDEN.json`.
- Each `<schema>-invalid.jsonl` line has one defect; `DEFECTS.json` lists them.
  Size cap in tests is 4096 bytes (production default is 16 MiB).
- `uv run python tools/fixtures/fetch_test_models.py [--level L1]` downloads the real L1/L2 models
  into the shared Hugging Face cache and checks every file's sha256.

## TRL spike
`uv run --project tools/fixtures/spike python tools/fixtures/spike/spike_trl_sft.py OUT_DIR` re-runs the
CPU SFT spike (full + LoRA, resume, callbacks, masking, offline). Run it before any TRL re-pin.
