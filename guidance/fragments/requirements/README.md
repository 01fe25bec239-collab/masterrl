# Requirement fragments

Add your part's tasks in `pNN.yaml` (your part number, like `p07.yaml`).
Never edit `guidance/data/requirements.yaml`. Fragments can add `components` and `requirements`, not `platforms`.

```yaml
requirements:
  - { task: eval, platform: linux-nvidia, supported: true, components: [nvidia-driver, cuda, python-env, pytorch] }
  - { task: eval, platform: cpu, supported: false, note: "Evaluation needs a GPU." }
```

Rules: one row per task and platform, and each component needs a size for that platform.
Rebuild with `uv run --script guidance/tools/build.py`.
