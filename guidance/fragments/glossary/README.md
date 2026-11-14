# Glossary fragments

Add your part's terms in `pNN.yaml` (your part number, like `p07.yaml`).
Never edit `guidance/data/glossary.yaml`.

```yaml
terms:
  - term: KV cache
    plain: Memory a model uses to remember the text it already read.
    aliases: [key-value cache]
```

Rules: `plain` is exactly one sentence, and terms and aliases are unique (any case).
Rebuild with `uv run --script guidance/tools/build.py`.
