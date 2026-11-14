# Guidance catalog

`guidance/` holds the plain-language data the app shows users: goals, recipes, hardware profiles,
glossary, error explanations and task requirements. Writing rules: `guidance/copy-style.md`.

## Layout

- `data/*.yaml`: shared base data. Only P05 edits it.
- `fragments/{errors,glossary,requirements}/pNN.yaml`: what each part adds.
- `schemas/*.schema.json`: one schema per data file. Fragments use the same schema.
- `build/`: generated catalog and loaders. Never edit by hand.

## Add your part's entries

1. Create `guidance/fragments/<kind>/pNN.yaml` with your part number. The folder README shows the format.
2. Add any new acronym to `guidance/fragments/glossary/pNN.yaml`.
3. Rebuild and commit `guidance/build/` with your change:

```bash
uv run --script guidance/tools/build.py
```

The build fails on schema errors, duplicate codes or terms, unknown references,
and writing rule breaks (sentences over 20 words, a bare "invalid", unknown acronyms).

## Use the catalog

- TypeScript/JS: import `explainError` from `guidance/build/index.js` by relative path (types in `index.d.ts`).
- Rust: `#[path = ".../guidance/build/catalog.rs"] mod catalog;` then `catalog::explain_error(code)`.
- Python: put `guidance/build` on the path, then `catalog.explain_error(code)`.
- `explainError` returns the generic fallback for an unknown code. Show the code in Details.

## Test

```bash
uv run --script guidance/tools/build.py --check
node --test guidance/tests
python3 -m unittest discover guidance/tests
rustc --edition 2021 --test guidance/tests/catalog_test.rs -o /tmp/guidance_rs_test && /tmp/guidance_rs_test
```
