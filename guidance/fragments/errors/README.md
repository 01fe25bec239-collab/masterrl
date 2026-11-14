# Error fragments

Add your part's error codes in `pNN.yaml` (your part number, like `p07.yaml`).
Never edit `guidance/data/errors.yaml`.

```yaml
errors:
  - code: MRL-RUN-001
    title: The run could not start.
    why: Another run is using this GPU.
    what_to_do:
      steps:
        - Wait for the other run to finish, then start again.
      linux:
        - Run `nvidia-smi` to see which programs use the GPU.
```

Optional: per-OS steps (`windows`, `macos`, `linux`) and `docs_link` (an `https://` link).
Rules: codes are unique, text follows `guidance/copy-style.md`, and every acronym is in the glossary.
Rebuild with `uv run --script guidance/tools/build.py`.
