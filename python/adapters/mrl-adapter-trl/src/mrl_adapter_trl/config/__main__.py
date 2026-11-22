"""Write the committed config schemas: `uv run python -m mrl_adapter_trl.config`."""

from mrl_adapter_trl.config import ENGINE_MODELS, schema_path, schema_text

for name in ENGINE_MODELS:
    schema_path(name).write_text(schema_text(name), encoding="utf-8")
    print(f"wrote {schema_path(name).name}")
