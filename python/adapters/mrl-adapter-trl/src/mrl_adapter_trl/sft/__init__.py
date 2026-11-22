"""The `trl` engine, SFT family: `sft` and `cpt` on trl.SFTTrainer (full, LoRA, QLoRA).

Staged inputs (JobContext.staged_paths): `model` (HF folder, safetensors), `train` and
optionally `eval` (any format the P15 readers read), `resume_from` (a committed checkpoint),
`checkpoint` (export jobs). Checkpoints are HF Trainer folders (`hf-trainer-v1`): kind
`full_resume` for full updates, `adapter_only` (adapter + optimizer state) for LoRA/QLoRA.
"""

# pyright: reportMissingImports=false
# (the root type check runs without this adapter's environment)

import importlib
import json
import shutil
from collections.abc import Iterable, Iterator, Mapping
from functools import cache
from itertools import chain, islice
from pathlib import Path
from typing import Any, Literal

from mrl_adapter_trl import common as trl_common
from mrl_adapter_trl import manifest
from mrl_adapter_trl.common import (
    CHECKPOINT_MANIFEST,
    Device,
    Flags,
    detect_device,
    drain,
    make_bridge,
    missing_resume_files,
    update_steps,
)
from mrl_adapter_trl.config import ENGINE_MODELS, Settings, parse
from mrl_adapter_trl.failures import ConfigError, DatasetError, classify
from mrl_runtime.adapter import (
    BaseEngine,
    EngineEvent,
    EnvInfo,
    ExportContext,
    ExportResult,
    FailureInfo,
    HardwareProfile,
    Issue,
    JobContext,
    PreparedJob,
    ProbeReport,
    ResolvedConfig,
    ResourceEstimate,
    RolloutSample,
    Samples,
    ValidationReport,
    ValidationRequest,
)
from mrl_runtime.data.readers import Limits, read_records
from mrl_runtime.data.validate import CheckOptions, RecordValidator, validate_dataset
from mrl_runtime.errors import FIELD_INVALID, FIELD_MISSING, METHOD_UNSUPPORTED
from mrl_runtime.estimate import DeviceSpec, Headroom, Topology, TrainConfig
from mrl_runtime.estimate import estimate as estimate_memory
from mrl_runtime.estimate.memory import DEFAULT_TARGETS, Optimizer
from mrl_runtime.model import ModelInspection, inspect_model
from mrl_runtime.model import MrlError as ModelError

NVIDIA_ONLY = "MRL-CAP-250"
CANNOT_RESUME = "MRL-CAP-251"
OTHER_FORMAT = "MRL-CAP-020"
NO_CHAT_TEMPLATE = "MRL-CAP-016"
DATASET_INVALID = "MRL-DATA-001"
ENV_BROKEN = "MRL-ENV-202"
CHECKPOINT_FORMAT = "hf-trainer-v1"
UPSTREAM = ("trl", "transformers", "peft", "accelerate", "datasets", "torch")
EXPORT_JOBS = ("EXPORT", "CONVERT")
L1_SAMPLE_RECORDS = 1000  # validate() checks this many records; prepare() checks every one
DRY_RUN_RECORDS = 64  # L2 dry run tokenizes this many
SAMPLE_PROMPTS = 2
SAMPLE_NEW_TOKENS = 48
COLUMNS = {
    "sft.messages.v1": ("messages",),
    "sft.prompt_completion.v1": ("prompt", "completion"),
    "pretrain.text.v1": ("text",),
}
UNSUPPORTED_FIELDS = ("tools", "images")
TOKENIZER_FILES = (
    "tokenizer.json",
    "tokenizer_config.json",
    "special_tokens_map.json",
    "chat_template.jinja",
    "tokenizer.model",
    "vocab.json",
    "merges.txt",
)
CONFIG_FILES = ("config.json", "generation_config.json")
OPTIMIZERS: dict[str, Optimizer] = {
    "adamw_torch": "adamw",
    "adamw_8bit": "adamw_8bit",
    "paged_adamw_8bit": "adamw_8bit",
    "sgd": "sgd",
    "adafactor": "adafactor",
}


@cache
def _manifest_switch(algorithm: str) -> Mapping[str, Any]:
    for algo in manifest().raw["spec"]["algorithms"]:
        if algo["id"] == algorithm:
            return dict(algo.get("switch", {}))
    return {}


@cache
def _metric_sources(algorithm: str) -> Mapping[str, str]:
    """Engine metric name -> split, for every metric the manifest declares for `algorithm`."""
    return {
        m.source: "eval" if m.source.startswith("eval_") else "train"
        for m in manifest().algorithms.get(algorithm, ())
        if m.source
    }


def dataset_schema(algorithm: str, path: Path) -> str | None:
    """The canonical schema of a dataset file: cpt reads text; sft tells its two apart."""
    if algorithm == "cpt":
        return "pretrain.text.v1"
    for record in read_records(path, limits=Limits(max_records=1)):
        value = record.value
        if isinstance(value, dict):
            if "messages" in value:
                return "sft.messages.v1"
            if "prompt" in value or "completion" in value:
                return "sft.prompt_completion.v1"
    return None


def _rows(path: Path, schema: str, limit: int | None) -> Iterator[dict[str, Any]]:
    """Every record as a training row; any record that fails its schema stops the run (DATA)."""
    validator = RecordValidator(schema)
    for record in read_records(path, limits=Limits(max_records=limit)):
        where = f"Record {record.number} of {path.name}"
        if record.defect is not None:
            raise DatasetError(DATASET_INVALID, f"{where} can't be read ({record.defect}).")
        defects = validator.defects(record.value)
        if defects:
            d = defects[0]
            raise DatasetError(
                DATASET_INVALID, f"{where}: {d.message} ({d.code} at {d.path or '/'})."
            )
        value: dict[str, Any] = record.value
        extra = [k for k in UNSUPPORTED_FIELDS if k in value]
        if extra:
            raise DatasetError(
                DATASET_INVALID,
                f"{where} has {extra[0]}, which this engine version doesn't train on.",
                f"Remove the {extra[0]} field from the dataset.",
            )
        row = {k: value[k] for k in COLUMNS[schema]}
        if schema == "sft.prompt_completion.v1" and _conversational(row):
            for key, role in (("prompt", "user"), ("completion", "assistant")):
                if isinstance(row[key], str):
                    row[key] = [{"role": role, "content": row[key]}]
        yield row


def _conversational(row: Mapping[str, Any]) -> bool:
    return any(isinstance(row.get(key), list) for key in ("messages", "prompt", "completion"))


def _load_dataset(path: Path, schema: str, limit: int | None) -> tuple[Any, ...]:
    import datasets  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

    # Arrow and TRL require a uniform form within each dataset.
    groups: dict[bool, list[dict[str, Any]]] = {}
    for row in _rows(path, schema, limit):
        groups.setdefault(_conversational(row), []).append(row)
    if not groups:
        raise DatasetError(DATASET_INVALID, f"{path.name} has no records.")
    return tuple(datasets.Dataset.from_list(rows) for rows in groups.values())


def _sample_prompts(rows: Iterable[Mapping[str, Any]], schema: str, tokenizer: Any) -> list[str]:
    prompts: list[str] = []
    for row in islice(rows, SAMPLE_PROMPTS):
        if schema == "sft.messages.v1":
            turns = row["messages"]
            last = max((i for i, m in enumerate(turns) if m["role"] == "assistant"), default=0)
            text = tokenizer.apply_chat_template(
                turns[:last], tokenize=False, add_generation_prompt=True
            )
        elif schema == "sft.prompt_completion.v1":
            prompt = row["prompt"]
            text = (
                tokenizer.apply_chat_template(prompt, tokenize=False, add_generation_prompt=True)
                if isinstance(prompt, list)
                else prompt
            )
        else:
            text = row["text"][:200]
        prompts.append(str(text))
    return prompts


def _make_sampler(prompts: list[str]) -> Any:
    def sample(model: Any, tokenizer: Any, step: int) -> Samples | None:
        import torch  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

        if model is None or tokenizer is None or not prompts:
            return None
        items: list[RolloutSample] = []
        was_training = model.training
        model.eval()
        try:
            for i, prompt in enumerate(prompts):
                inputs = tokenizer(prompt, return_tensors="pt", add_special_tokens=False)
                inputs = {k: v.to(model.device) for k, v in inputs.items()}
                with torch.no_grad():
                    out = model.generate(
                        **inputs,
                        max_new_tokens=SAMPLE_NEW_TOKENS,
                        do_sample=False,
                        pad_token_id=tokenizer.pad_token_id or tokenizer.eos_token_id,
                    )
                new = out[0][inputs["input_ids"].shape[1] :]
                text = tokenizer.decode(new, skip_special_tokens=True)
                items.append(
                    RolloutSample(f"step{step}-{i}", prompt, text, completion_tokens=len(new))
                )
        finally:
            model.train(was_training)
        return Samples(step, tuple(items))

    return sample


def _precision(settings: Settings, device: Device) -> Literal["bf16", "fp16", "fp32"]:
    chosen = settings.common.precision
    if chosen != "auto":
        return chosen
    if device.kind == "cuda":
        return "bf16" if device.bf16 else "fp16"
    return "fp32"


def _weight_dtype(precision: str, strategy: str) -> str:
    if precision == "bf16":
        return "bfloat16"
    if precision == "fp16" and strategy != "full":  # fp16 AMP can't step fp16 master weights
        return "float16"
    return "float32"


def _hardware_issues(settings: Settings, device: Device) -> list[Issue]:
    issues: list[Issue] = []
    if device.kind == "cuda":
        if settings.common.precision == "bf16" and not device.bf16:
            issues.append(
                Issue(
                    FIELD_INVALID,
                    f"This GPU ({device.name}) can't train in bf16.",
                    path="/common/precision",
                    hint="Set precision to auto or fp16.",
                )
            )
        return issues
    needs = {
        "QLoRA": settings.strategy == "qlora",
        "An 8-bit optimizer": settings.engine.optim in ("adamw_8bit", "paged_adamw_8bit"),
        "fp16 training": settings.common.precision == "fp16",
    }
    paths = {"QLoRA": "/common/update/strategy", "An 8-bit optimizer": "/engine/optim"}
    for what, needed in needs.items():
        if needed:
            issues.append(
                Issue(
                    NVIDIA_ONLY,
                    f"{what} needs an NVIDIA GPU; this machine trains on {device.name}.",
                    path=paths.get(what, "/common/precision"),
                    hint="Pick LoRA, adamw_torch and precision auto, or run on an NVIDIA GPU.",
                )
            )
    return issues


def _model_issues(inspection: ModelInspection, algorithm: str) -> list[Issue]:
    path = "/inputs/model"
    if inspection.refusal:
        return [Issue(inspection.refusal, "The app did not load this model's files.", path=path)]
    if inspection.requires_remote_code:
        return [
            Issue(
                inspection.remote_code_code or "MRL-MODEL-002",
                "This model needs custom code; this engine never runs model code.",
                path=path,
            )
        ]
    if inspection.inference_only_code:
        return [
            Issue(
                inspection.inference_only_code,
                "This model can answer prompts, but it can't be trained.",
                path=path,
            )
        ]
    if inspection.kind != "full_weights":
        return [
            Issue(
                FIELD_INVALID,
                "Training needs a full base model; this folder holds only an adapter.",
                path=path,
                hint="Merge the adapter into its base model first, or pick the base model.",
            )
        ]
    if inspection.vision is not None:
        return [
            Issue(
                METHOD_UNSUPPORTED,
                f"This engine version trains text models; {algorithm} can't train a vision model.",
                path=path,
            )
        ]
    return []


def _resume_step(path: Path) -> int:
    try:
        state = json.loads((path / "trainer_state.json").read_text(encoding="utf-8"))
        step = state.get("global_step") if isinstance(state, dict) else None
    except (OSError, ValueError):
        step = None
    if type(step) is not int or step < 0:
        raise ConfigError(CANNOT_RESUME, "The checkpoint has no valid training step.")
    return step


def _resume_issues(path: Path, adapter: bool) -> list[Issue]:
    where = "/inputs/resume_from"
    manifest_path = path / CHECKPOINT_MANIFEST
    if manifest_path.is_file():
        try:
            doc: object = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, ValueError):
            doc = None
        if not isinstance(doc, dict):
            return [Issue(CANNOT_RESUME, "The checkpoint's manifest can't be read.", path=where)]
        engine = doc.get("engine")
        name = engine.get("name") if isinstance(engine, dict) else None
        fmt = doc.get("format")
        if fmt != CHECKPOINT_FORMAT or name != "trl":
            return [
                Issue(
                    OTHER_FORMAT,
                    f"This checkpoint came from {name!r} ({fmt!r}), not the TRL engine.",
                    path=where,
                )
            ]
    missing = missing_resume_files(path, adapter)
    if missing:
        return [
            Issue(
                CANNOT_RESUME,
                f"This checkpoint can't resume training: it has no {', '.join(missing)}.",
                path=where,
                hint="Resume from an earlier checkpoint, or start a new run.",
            )
        ]
    try:
        _resume_step(path)
    except ConfigError as exc:
        return [Issue(exc.code, exc.message, path=where)]
    return []


def _inspect(model_dir: Path | None, algorithm: str) -> tuple[ModelInspection | None, list[Issue]]:
    if model_dir is None:
        return None, []
    try:
        inspection = inspect_model(model_dir)
    except ModelError as exc:
        return None, [Issue(exc.code, exc.message, path="/inputs/model", hint=exc.detail)]
    return inspection, _model_issues(inspection, algorithm)


def _settings_issues(
    settings: Settings, staged: Mapping[str, Path], inspection: ModelInspection | None
) -> list[Issue]:
    issues = _hardware_issues(settings, detect_device())
    if "resume_from" in staged:
        issues += _resume_issues(staged["resume_from"], settings.strategy != "full")
    context = inspection.context_length if inspection is not None else None
    if context and settings.engine.max_length > context:
        issues.append(
            Issue(
                FIELD_INVALID,
                f"max_length {settings.engine.max_length} is longer than this model's "
                f"context ({context} tokens).",
                severity="warning",
                path="/engine/max_length",
                hint=f"Set max_length to {context} or less.",
            )
        )
    return issues


def _dataset_issues(
    role: str, path: Path, algorithm: str, inspection: ModelInspection | None
) -> list[Issue]:
    schema, issues = _dataset_report(role, path, algorithm)
    if issues or inspection is None or inspection.chat_template_sha256 is not None:
        return issues
    needs_template = schema == "sft.messages.v1" or (
        schema == "sft.prompt_completion.v1"
        and any(
            isinstance(record.value, dict) and _conversational(record.value)
            for record in read_records(path, limits=Limits(max_records=L1_SAMPLE_RECORDS))
        )
    )
    if needs_template:
        issues.append(
            Issue(
                NO_CHAT_TEMPLATE,
                "This model has no chat template, so it can't read conversations.",
                path=f"/inputs/{role}",
                hint="Pick a chat model, or use prompt/completion text.",
            )
        )
    return issues


def _dataset_report(role: str, path: Path, algorithm: str) -> tuple[str | None, list[Issue]]:
    where = f"/inputs/{role}"
    try:
        schema = dataset_schema(algorithm, path)
    except ModelError as exc:
        return None, [Issue(exc.code, exc.message, path=where)]
    if schema is None:
        return None, [
            Issue(
                DATASET_INVALID,
                f"The {role} dataset holds neither conversations nor prompt/completion pairs.",
                path=where,
            )
        ]
    try:
        report = validate_dataset(
            path, schema, CheckOptions(limits=Limits(max_records=L1_SAMPLE_RECORDS))
        )
    except ModelError as exc:
        return schema, [Issue(exc.code, exc.message, path=where, hint=exc.detail)]
    if not report.passed:
        first = report.examples[0] if report.examples else None
        detail = f" First problem: record {first.record}, {first.message}" if first else ""
        return schema, [
            Issue(
                report.error_code or DATASET_INVALID,
                f"The {role} dataset did not pass its checks ({report.invalid} of "
                f"{report.records} records).{detail}",
                path=where,
            )
        ]
    return schema, []


class TrlSftEngine(BaseEngine):
    """TrainingEngine for `sft` and `cpt` (see the module docstring for inputs and outputs)."""

    def __init__(self) -> None:
        super().__init__()
        self._trainer: Any = None
        self._bridge: Any = None
        self._inspection: ModelInspection | None = None
        self._total_steps = 0
        self._start_step = 0
        self._export_model: Path | None = None
        self._resume: Path | None = None

    # ---- side-effect free ----

    def probe(self, env: EnvInfo) -> ProbeReport:
        versions: dict[str, str] = {"mrl-runtime": env.mrl_runtime_version}
        for name in UPSTREAM:
            try:
                versions[name] = importlib.import_module(name).__version__
            except ImportError as exc:
                message = f"The TRL environment is missing {name} ({exc})."
                return ProbeReport(False, versions, issues=(Issue(ENV_BROKEN, message),))
        device = detect_device()
        try:
            importlib.import_module("bitsandbytes")
            bnb = device.kind == "cuda"
        except ImportError:
            bnb = False
        features = {
            "cuda": device.kind == "cuda",
            "mps": device.kind == "mps",
            "cpu": device.kind == "cpu",
            "bf16": device.bf16,
            "qlora": bnb,
        }
        return ProbeReport(True, versions, features)

    def validate(self, req: ValidationRequest) -> ValidationReport:
        cfg = req.config
        if cfg.algorithm not in ENGINE_MODELS:
            return ValidationReport(
                (
                    Issue(
                        METHOD_UNSUPPORTED,
                        f"The TRL SFT engine has no {cfg.algorithm!r}.",
                        path="/algorithm",
                    ),
                )
            )
        if str(cfg.raw.get("job_kind", "TRAIN")).upper() in EXPORT_JOBS:
            return ValidationReport(tuple(self._export_issues(cfg, req.staged_paths)))
        settings, issues = self._settings(cfg)
        staged = req.staged_paths
        for role in ("model", "train"):
            if role not in staged:
                issues.append(
                    Issue(FIELD_MISSING, f"The job staged no {role}.", path=f"/inputs/{role}")
                )
        inspection, found = _inspect(staged.get("model"), cfg.algorithm)
        issues += found
        for role in ("train", "eval"):
            if role in staged:
                issues += _dataset_issues(role, staged[role], cfg.algorithm, inspection)
        if settings is not None:
            issues += _settings_issues(settings, staged, inspection)
        return ValidationReport(tuple(issues))

    def estimate(self, cfg: ResolvedConfig, hw: HardwareProfile) -> ResourceEstimate:
        settings, _ = self._settings(cfg)
        if settings is None or self._inspection is None:
            return ResourceEstimate(
                total_steps=self._total_steps, assumptions=("model not inspected",)
            )
        device = detect_device()
        precision = _precision(settings, device)
        e = settings.engine
        lora = settings.common.update.lora
        targets = lora.target_modules
        train = TrainConfig(
            method=settings.strategy,
            seq_len=e.max_length,
            micro_batch=e.per_device_train_batch_size,
            weight_dtype=_weight_dtype(precision, settings.strategy),
            optimizer=OPTIMIZERS[e.optim],
            grad_checkpointing=e.gradient_checkpointing,
            activation_dtype="float32" if precision == "fp32" else "bfloat16",
            lora_rank=lora.r,
            lora_targets=DEFAULT_TARGETS if isinstance(targets, str) else tuple(targets),
        )
        kind = {"cuda": "gpu", "mps": "unified", "cpu": "cpu"}[device.kind]
        spec = DeviceSpec(kind, device.memory_gib, device.kind, Headroom())  # type: ignore[arg-type]
        est = estimate_memory(self._inspection, train, Topology(spec))
        peak = est.devices[0].peak if est.devices else None
        mem = (peak.low, peak.high) if peak else (0.0, 0.0)
        notes = (f"verdict {est.verdict}, confidence {est.confidence}", *est.assumptions)
        return ResourceEstimate(mem, self._total_steps, assumptions=notes)

    # ---- in the job sandbox ----

    def prepare(self, ctx: JobContext) -> PreparedJob:
        cfg = ctx.config
        if str(cfg.raw.get("job_kind", "TRAIN")).upper() in EXPORT_JOBS:
            self._export_model = ctx.staged_paths.get("model")
            return PreparedJob(effective_config={"export": dict(cfg.raw.get("export", {}))})
        settings, issues = self._settings(cfg)
        if settings is None:
            raise ConfigError(issues[0].code, issues[0].message, issues[0].hint)
        model_dir = ctx.staged_paths["model"]
        self._inspection = inspect_model(model_dir)
        self._resume = ctx.resume_from
        self._start_step = _resume_step(self._resume) if self._resume is not None else 0
        limit = DRY_RUN_RECORDS if ctx.dry_run else None
        schema = dataset_schema(cfg.algorithm, ctx.staged_paths["train"])
        if schema is None:
            raise DatasetError(DATASET_INVALID, "The train dataset has no usable records.")
        train = _load_dataset(ctx.staged_paths["train"], schema, limit)
        evaluation = None
        if "eval" in ctx.staged_paths:
            eval_schema = dataset_schema(cfg.algorithm, ctx.staged_paths["eval"]) or schema
            evaluation = _load_dataset(ctx.staged_paths["eval"], eval_schema, limit)
        self._trainer, self._bridge = self._build(ctx, settings, schema, train, evaluation)
        e = settings.engine
        batches = len(self._trainer.get_train_dataloader())
        self._total_steps = update_steps(
            batches, e.gradient_accumulation_steps, e.num_train_epochs, e.max_steps or -1
        )
        effective = {**settings.effective(), "schema": schema, "trl": self._trainer.args.to_dict()}
        return PreparedJob(effective_config=effective, total_steps=self._total_steps)

    def run(self, ctx: JobContext) -> Iterator[EngineEvent]:
        trainer = self._trainer
        if trainer is None:
            raise ConfigError(FIELD_INVALID, "run() was called before prepare().")
        if self._resume is not None and self._start_step >= self._total_steps:
            return
        resume = str(self._resume) if self._resume is not None else None
        yield from drain(self._bridge, lambda: trainer.train(resume_from_checkpoint=resume))

    def export(self, ctx: ExportContext) -> ExportResult:
        source = ctx.checkpoint_path
        adapter = (source / "adapter_config.json").is_file()
        out = ctx.output_dir
        if ctx.format == "hf-peft-adapter":
            if not adapter:
                raise ConfigError(
                    FIELD_INVALID,
                    "This checkpoint is a full model, so it has no adapter to export.",
                    "Export it as hf-safetensors instead.",
                )
            files = self._copy(source, out, (*trl_common.ADAPTER_FILES, *TOKENIZER_FILES))
            return ExportResult(ctx.format, files)
        if not adapter:
            names = [p.name for p in source.glob("model*.safetensors*")]
            return ExportResult(
                ctx.format, self._copy(source, out, (*names, *CONFIG_FILES, *TOKENIZER_FILES))
            )
        if self._export_model is None:
            raise ConfigError(
                FIELD_MISSING,
                "Merging an adapter needs its base model, and the job staged none.",
                "Start the export from the app, so it stages the base model too.",
            )
        return ExportResult(ctx.format, self._merge(source, self._export_model, out))

    def cleanup(self, ctx: JobContext) -> None:
        self._trainer = None
        self._bridge = None

    def classify_failure(
        self, exc: BaseException | None, exit_code: int | None, log_tail: str
    ) -> FailureInfo | None:
        return classify(exc, exit_code, log_tail)

    # ---- helpers ----

    def _settings(self, cfg: ResolvedConfig) -> tuple[Settings | None, list[Issue]]:
        return parse(
            cfg.algorithm, cfg.common, cfg.engine, cfg.switch, _manifest_switch(cfg.algorithm)
        )

    def _export_issues(self, cfg: ResolvedConfig, staged: Mapping[str, Path]) -> list[Issue]:
        checkpoint = staged.get("checkpoint")
        if checkpoint is None:
            return [
                Issue(
                    FIELD_MISSING, "The export job staged no checkpoint.", path="/inputs/checkpoint"
                )
            ]
        _, issues = _inspect(staged.get("model"), cfg.algorithm)
        adapter = (checkpoint / "adapter_config.json").is_file()
        if adapter and not (checkpoint / "adapter_model.safetensors").is_file():
            issues.append(
                Issue(
                    "MRL-MODEL-001",
                    "The checkpoint's adapter isn't a safetensors file, so the app won't load it.",
                    path="/inputs/checkpoint",
                )
            )
        fmt = str(dict(cfg.raw.get("export") or {}).get("format", ""))
        if adapter and fmt != "hf-peft-adapter" and "model" not in staged:
            issues.append(
                Issue(
                    FIELD_MISSING,
                    "Merging an adapter needs its base model, and the job staged none.",
                    path="/inputs/model",
                    hint="Start the export from the app, so it stages the base model too.",
                )
            )
        return issues

    def _build(
        self,
        ctx: JobContext,
        settings: Settings,
        schema: str,
        train: tuple[Any, ...],
        evaluation: tuple[Any, ...] | None,
    ) -> tuple[Any, Any]:
        from peft import LoraConfig  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)
        from transformers import AutoTokenizer, BitsAndBytesConfig, PrinterCallback  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)
        from trl import SFTConfig  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

        from mrl_adapter_trl.sft.trainer import SftTrainer  # noqa: PLC0415 (loads TRL)

        device = detect_device()
        precision = _precision(settings, device)
        e = settings.engine
        strategy = settings.strategy
        model_dir = ctx.staged_paths["model"]
        tokenizer = AutoTokenizer.from_pretrained(model_dir, trust_remote_code=False)
        model_kwargs: dict[str, Any] = {"dtype": _weight_dtype(precision, strategy)}
        model_kwargs["device_map"] = {"": 0} if device.kind == "cuda" else None
        args = SFTConfig(
            output_dir=str(ctx.work_dir / "trainer"),
            max_steps=e.max_steps or -1,
            num_train_epochs=e.num_train_epochs,
            per_device_train_batch_size=e.per_device_train_batch_size,
            per_device_eval_batch_size=e.per_device_train_batch_size,
            gradient_accumulation_steps=e.gradient_accumulation_steps,
            learning_rate=e.learning_rate,
            lr_scheduler_type=e.lr_scheduler_type,
            warmup_steps=e.warmup_ratio,  # below 1: a share of all steps
            weight_decay=e.weight_decay,
            max_grad_norm=e.max_grad_norm,
            optim=e.optim,
            gradient_checkpointing=e.gradient_checkpointing,
            gradient_checkpointing_kwargs={"use_reentrant": False},
            bf16=precision == "bf16",
            fp16=precision == "fp16",
            use_cpu=device.kind == "cpu",
            seed=settings.common.seed,
            data_seed=settings.common.seed,
            logging_steps=e.logging_steps,
            save_strategy="steps",
            save_steps=e.save_steps,
            save_total_limit=None,
            eval_strategy="steps" if evaluation is not None else "no",
            eval_steps=e.save_steps,
            report_to=[],
            logging_nan_inf_filter=False,  # the harness must see a NaN loss to stop the run
            push_to_hub=False,
            disable_tqdm=True,
            dataloader_num_workers=0,
            dataloader_pin_memory=device.kind == "cuda",
            max_length=e.max_length,
            packing=e.packing,
            loss_type=e.loss_type,
            assistant_only_loss=e.assistant_only_loss and schema == "sft.messages.v1",
            completion_only_loss=e.completion_only_loss,
            neftune_noise_alpha=e.neftune_noise_alpha,
            dataset_text_field="text",
            model_init_kwargs=model_kwargs,
            trust_remote_code=False,
        )
        lora = settings.common.update.lora
        peft_config = None
        if strategy in ("lora", "qlora"):
            peft_config = LoraConfig(
                r=lora.r,
                lora_alpha=lora.alpha,
                lora_dropout=lora.dropout,
                target_modules=lora.target_modules
                if isinstance(lora.target_modules, str)
                else list(lora.target_modules),
                task_type="CAUSAL_LM",
            )
        quantization = None
        if strategy == "qlora":
            compute = "bfloat16" if precision == "bf16" else "float16"
            quantization = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_use_double_quant=True,
                bnb_4bit_compute_dtype=compute,
            )
        prompts = (
            _sample_prompts(chain.from_iterable(train), schema, tokenizer)
            if e.samples_every
            else []
        )
        flags = Flags(self.pause_requested, self.cancel_requested, self.checkpoint_requested)
        bridge = make_bridge(
            flags,
            sources=_metric_sources(settings.algorithm),
            kind="full_resume" if strategy == "full" else "adapter_only",
            method=strategy,
            sampler=_make_sampler(prompts) if prompts else None,
            samples_every=e.samples_every,
        )
        trainer = SftTrainer(
            model=str(model_dir),
            args=args,
            train_dataset=train[0],
            eval_dataset=evaluation[0] if evaluation else None,
            datasets={"train": train, "eval": evaluation},
            mask_assistant=e.assistant_only_loss,
            processing_class=tokenizer,
            peft_config=peft_config,
            quantization_config=quantization,
            callbacks=[bridge],
        )
        trainer.remove_callback(PrinterCallback)  # metrics reach the app through the bridge
        return trainer, bridge

    @staticmethod
    def _copy(source: Path, out: Path, names: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
        files: list[tuple[str, str]] = []
        for name in dict.fromkeys(names):
            if (source / name).is_file():
                shutil.copyfile(source / name, out / name)
                files.append((name, _role(name)))
        if not any(role in ("weights", "adapter") for _, role in files):
            raise ConfigError(FIELD_INVALID, "The checkpoint has no weights to export.")
        return tuple(files)

    @staticmethod
    def _merge(checkpoint: Path, base: Path, out: Path) -> tuple[tuple[str, str], ...]:
        from peft import PeftModel  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)
        from transformers import AutoModelForCausalLM, AutoTokenizer  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

        model = AutoModelForCausalLM.from_pretrained(base, dtype="auto", trust_remote_code=False)
        merged = PeftModel.from_pretrained(model, checkpoint).merge_and_unload()
        merged.save_pretrained(out, safe_serialization=True)
        AutoTokenizer.from_pretrained(checkpoint, trust_remote_code=False).save_pretrained(out)
        return tuple(
            (p.relative_to(out).as_posix(), _role(p.name))
            for p in sorted(out.rglob("*"))
            if p.is_file() and p.name != "artifact-manifest.json"
        )


def _role(name: str) -> str:
    if name.startswith("adapter_"):
        return "adapter"
    if name.endswith((".safetensors", ".safetensors.index.json")):
        return "weights"
    if name in CONFIG_FILES:
        return "config"
    return "tokenizer"
