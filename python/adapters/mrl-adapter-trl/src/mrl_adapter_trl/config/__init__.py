"""Config models for the TRL SFT family: the source of truth for config/*.schema.json.

A job's config has two blocks (Doc 05 §12.2). `common` is platform-owned; `CommonBlock` lists
only the keys this engine reads and ignores the rest. `engine` is ours and closed: an unknown
key is an error. Fields an algorithm pins (`switch` in the manifest) are rendered read-only
(`x-mrl-ui.pinned`) and may only repeat the pinned value.

Regenerate the committed schemas with `uv run python -m mrl_adapter_trl.config`.
"""

# pyright: reportMissingImports=false
# (the root type check runs without this adapter's environment)

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, ValidationError

from mrl_runtime.adapter import Issue
from mrl_runtime.errors import FIELD_INVALID

SCHEMA_DIR = Path(__file__).parent
Tier = Literal["beginner", "advanced", "expert"]
Strategy = Literal["full", "lora", "qlora"]
Precision = Literal["auto", "bf16", "fp16", "fp32"]
ModuleName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.]{1,128}$")]
SWITCH_PINNED = "MRL-CAP-012"


def ui(tier: Tier, group: str, order: int, help_text: str, **extra: Any) -> dict[str, Any]:
    """The `x-mrl-ui` annotation (validators ignore it; the form reads it)."""
    return {"x-mrl-ui": {"tier": tier, "group": group, "order": order, "help": help_text, **extra}}


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class LoraSettings(_Closed):
    """LoRA adapter shape. Used by the `lora` and `qlora` strategies."""

    r: int = Field(16, ge=1, le=1024, json_schema_extra=ui("advanced", "lora", 1, "Adapter rank."))
    alpha: float = Field(
        32.0, gt=0, le=4096, json_schema_extra=ui("advanced", "lora", 2, "Adapter scale.")
    )
    dropout: float = Field(
        0.05, ge=0, lt=1, json_schema_extra=ui("advanced", "lora", 3, "Adapter dropout.")
    )
    target_modules: Literal["all-linear"] | list[ModuleName] = Field(
        "all-linear",
        min_length=1,
        json_schema_extra=ui("expert", "lora", 4, "Layers that get an adapter."),
    )


class UpdateSettings(_Closed):
    strategy: Strategy = Field(
        "lora",
        json_schema_extra=ui(
            "beginner", "update", 1, "LoRA trains a small adapter; full updates every weight."
        ),
    )
    lora: LoraSettings = LoraSettings()


class CommonBlock(BaseModel):
    """The keys of the platform's common block (mrl.dev/config/common/v1) this engine reads."""

    model_config = ConfigDict(extra="ignore", frozen=True, strict=True)

    seed: int = Field(0, ge=0, le=2**32 - 1, json_schema_extra=ui("advanced", "run", 1, "Seed."))
    precision: Precision = Field(
        "auto",
        json_schema_extra=ui(
            "advanced", "run", 2, "auto: bf16 on GPUs that support it, else fp16 or fp32."
        ),
    )
    update: UpdateSettings = UpdateSettings()


class TrainSettings(_Closed):
    """Engine keys shared by sft and cpt. Names match TRL's SFTConfig, except warmup_ratio
    (passed as a fraction in warmup_steps) and samples_every (ours)."""

    learning_rate: float = Field(
        1e-4, gt=0, le=1, json_schema_extra=ui("advanced", "optimizer", 1, "Step size.")
    )
    num_train_epochs: float = Field(
        1.0, gt=0, le=1000, json_schema_extra=ui("beginner", "budget", 1, "Passes over the data.")
    )
    max_steps: int | None = Field(
        None,
        ge=1,
        le=10_000_000,
        json_schema_extra=ui(
            "advanced", "budget", 2, "Stop after this many steps (overrides epochs)."
        ),
    )
    per_device_train_batch_size: int = Field(
        4, ge=1, le=4096, json_schema_extra=ui("advanced", "batch", 1, "Examples per step per GPU.")
    )
    gradient_accumulation_steps: int = Field(
        1,
        ge=1,
        le=4096,
        json_schema_extra=ui("advanced", "batch", 2, "Steps summed before an update."),
    )
    max_length: int = Field(
        1024,
        ge=16,
        le=1_048_576,
        json_schema_extra=ui(
            "advanced", "data", 1, "Longest example in tokens; longer ones are cut.", unit="tokens"
        ),
    )
    packing: bool = Field(
        False, json_schema_extra=ui("advanced", "data", 2, "Pack short examples together.")
    )
    lr_scheduler_type: Literal["linear", "cosine", "constant", "constant_with_warmup"] = Field(
        "cosine", json_schema_extra=ui("advanced", "optimizer", 2, "How the step size changes.")
    )
    warmup_ratio: float = Field(
        0.03,
        ge=0,
        le=0.5,
        json_schema_extra=ui("advanced", "optimizer", 3, "Share of steps to warm up."),
    )
    weight_decay: float = Field(
        0.0, ge=0, le=1, json_schema_extra=ui("advanced", "optimizer", 4, "Weight decay.")
    )
    max_grad_norm: float = Field(
        1.0, gt=0, le=1e6, json_schema_extra=ui("expert", "optimizer", 5, "Gradient clipping.")
    )
    optim: Literal["adamw_torch", "adamw_8bit", "paged_adamw_8bit", "sgd", "adafactor"] = Field(
        "adamw_torch",
        json_schema_extra=ui("expert", "optimizer", 6, "8-bit optimizers need an NVIDIA GPU."),
    )
    gradient_checkpointing: bool = Field(
        False, json_schema_extra=ui("advanced", "memory", 1, "Less memory, about 30% slower.")
    )
    save_steps: int = Field(
        500,
        ge=1,
        le=10_000_000,
        json_schema_extra=ui("advanced", "checkpoints", 1, "Steps between checkpoints."),
    )
    logging_steps: int = Field(
        10,
        ge=1,
        le=100_000,
        json_schema_extra=ui("expert", "checkpoints", 2, "Steps between metric reports."),
    )
    samples_every: int = Field(
        0,
        ge=0,
        le=10_000_000,
        json_schema_extra=ui(
            "advanced", "checkpoints", 3, "Steps between sample answers (0: off)."
        ),
    )
    neftune_noise_alpha: float | None = Field(
        None, gt=0, le=100, json_schema_extra=ui("expert", "data", 3, "NEFTune embedding noise.")
    )
    loss_type: Literal["nll"] = Field(
        "nll", json_schema_extra=ui("expert", "loss", 1, "Loss.", pinned=True)
    )


class SftEngine(TrainSettings):
    """`sft`: conversations (sft.messages.v1) or prompt/completion (sft.prompt_completion.v1)."""

    assistant_only_loss: bool = Field(
        True,
        json_schema_extra=ui(
            "advanced", "loss", 2, "Conversations: learn only the assistant's turns."
        ),
    )
    completion_only_loss: bool = Field(
        True,
        json_schema_extra=ui(
            "advanced", "loss", 3, "Prompt/completion: learn only the completion."
        ),
    )


class CptEngine(TrainSettings):
    """`cpt`: continued pre-training on raw text (pretrain.text.v1); loss on every token."""

    assistant_only_loss: Literal[False] = Field(
        False, json_schema_extra=ui("expert", "loss", 2, "Learn every token.", pinned=True)
    )
    completion_only_loss: Literal[False] = Field(
        False, json_schema_extra=ui("expert", "loss", 3, "Learn every token.", pinned=True)
    )


ENGINE_MODELS: dict[str, type[TrainSettings]] = {"sft": SftEngine, "cpt": CptEngine}


class _JobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    common: CommonBlock = CommonBlock()


class SftConfig(_JobConfig):
    engine: SftEngine = SftEngine()


class CptConfig(_JobConfig):
    engine: CptEngine = CptEngine()


JOB_MODELS: dict[str, type[_JobConfig]] = {"sft": SftConfig, "cpt": CptConfig}


def json_schema(algorithm: str) -> dict[str, Any]:
    """The committed config schema for one algorithm: {common, engine}."""
    model = JOB_MODELS[algorithm]
    schema = model.model_json_schema()
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = f"https://schemas.masterrl.dev/engines/trl/{algorithm}.schema.json"
    schema["title"] = f"TRL {algorithm} config"
    return schema


def schema_path(algorithm: str) -> Path:
    return SCHEMA_DIR / f"{algorithm}.schema.json"


def schema_text(algorithm: str) -> str:
    return json.dumps(json_schema(algorithm), indent=2, sort_keys=True) + "\n"


@dataclass(frozen=True)
class Settings:
    """A validated job config: the common keys we read and the engine block, switch applied."""

    algorithm: str
    common: CommonBlock
    engine: TrainSettings

    @property
    def strategy(self) -> Strategy:
        return self.common.update.strategy

    def effective(self) -> dict[str, Any]:
        return {"common": self.common.model_dump(), "engine": self.engine.model_dump()}


def _pointer(block: str, loc: tuple[int | str, ...]) -> str:
    return "/" + "/".join([block, *(str(p) for p in loc)])


def _issues(block: str, err: ValidationError) -> list[Issue]:
    out: list[Issue] = []
    for e in err.errors(include_url=False, include_input=False):
        path = _pointer(block, tuple(e["loc"]))
        if e["type"] == "extra_forbidden":
            message = f"The TRL engine has no setting {path.rsplit('/', 1)[-1]!r}."
            hint = "Remove it, or check its spelling."
        else:
            message = f"{path.removeprefix('/')}: {e['msg']}."
            hint = ""
        out.append(Issue(FIELD_INVALID, message, path=path, hint=hint))
    return out


def parse(
    algorithm: str,
    common: Mapping[str, Any],
    engine: Mapping[str, Any],
    switch: Mapping[str, Any],
    pinned: Mapping[str, Any],
) -> tuple[Settings | None, list[Issue]]:
    """Validate a job's blocks. `pinned` is the manifest's switch; `switch` the job's copy.

    A user value or a job switch that differs from a pinned value is refused (MRL-CAP-012);
    the pinned values are then applied, so the engine always runs what the algorithm id means.
    """
    issues: list[Issue] = []
    for source, block in (("engine", engine), ("switch", switch)):
        for key, value in block.items():
            if key in pinned and value != pinned[key]:
                issues.append(
                    Issue(
                        SWITCH_PINNED,
                        f"{key} is fixed to {pinned[key]!r} for {algorithm}.",
                        path=f"/{source}/{key}",
                        hint=f"Remove {key}, or pick a different method.",
                    )
                )
    for key in switch:
        if key not in pinned:
            issues.append(
                Issue(FIELD_INVALID, f"{algorithm} pins no setting {key!r}.", path=f"/switch/{key}")
            )
    try:
        common_block = CommonBlock.model_validate(dict(common))
    except ValidationError as err:
        issues.extend(_issues("common", err))
        common_block = None
    try:
        engine_block = ENGINE_MODELS[algorithm].model_validate({**engine, **pinned})
    except ValidationError as err:
        issues.extend(_issues("engine", err))
        engine_block = None
    if issues or common_block is None or engine_block is None:
        return None, issues
    return Settings(algorithm, common_block, engine_block), []
