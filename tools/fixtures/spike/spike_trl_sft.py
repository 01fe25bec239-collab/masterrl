"""DG-03 spike: TRL SFTTrainer on CPU with the tiny-llama fixture (full and LoRA).

    uv run python tools/fixtures/make_fixtures.py
    uv run --project tools/fixtures/spike python tools/fixtures/spike/spike_trl_sft.py OUT_DIR

Writes OUT_DIR/results.json: checkpoint contents, resume behaviour, which checkpoint files
resume needs, callback hooks, assistant-only loss masking and the offline check.
"""

# ruff: noqa: E402  (HF env vars and the network block must be set before HF imports)
import os

# Hub off before any HF import.
os.environ |= {
    "HF_HUB_OFFLINE": "1",
    "HF_DATASETS_OFFLINE": "1",
    "TRANSFORMERS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
    "TOKENIZERS_PARALLELISM": "false",
}

import json
import platform
import shutil
import socket
import sys
import time
from pathlib import Path
from typing import Any

attempted_connections: list[str] = []


def _blocked_connect(self: socket.socket, address: Any) -> None:
    attempted_connections.append(repr(address))
    raise OSError(f"spike blocks network: {address!r}")


socket.socket.connect = _blocked_connect  # type: ignore[method-assign]

import accelerate
import datasets
import peft
import torch
import transformers
import trl
from peft import LoraConfig
from transformers import AutoModelForCausalLM, AutoTokenizer, TrainerCallback, set_seed
from trl import SFTConfig, SFTTrainer

REPO = Path(__file__).resolve().parents[3]
MODEL = REPO / "tests/fixtures/models/.cache/tiny-llama"
DATA = REPO / "tests/fixtures/datasets/sft.messages.v1.jsonl"
MAX_STEPS, SAVE_STEPS, CUSTOM_SAVE_AT, STOP_AT = 40, 20, 7, 25


class Probe(TrainerCallback):
    """Records hook calls; optionally forces a save or a stop at a step."""

    def __init__(self, save_at: int | None = None, stop_at: int | None = None) -> None:
        self.save_at, self.stop_at = save_at, stop_at
        self.loss: dict[int, float] = {}
        self.lr: dict[int, float] = {}
        self.log_keys: set[str] = set()
        self.saves: list[int] = []
        self.step_ends: list[int] = []

    def on_step_end(self, args, state, control, **kwargs):  # type: ignore[no-untyped-def]
        self.step_ends.append(state.global_step)
        self.lr[state.global_step] = kwargs["lr_scheduler"].get_last_lr()[0]
        if state.global_step in (self.save_at, self.stop_at):
            control.should_save = True
        if state.global_step == self.stop_at:
            control.should_training_stop = True

    def on_log(self, args, state, control, logs=None, **kwargs):  # type: ignore[no-untyped-def]
        logs = logs or {}
        self.log_keys |= set(logs)
        if "loss" in logs:
            self.loss[state.global_step] = logs["loss"]

    def on_save(self, args, state, control, **kwargs):  # type: ignore[no-untyped-def]
        self.saves.append(state.global_step)


def make_trainer(out: Path, lora: bool, probe: Probe) -> SFTTrainer:
    set_seed(0)
    config = SFTConfig(
        output_dir=str(out),
        max_steps=MAX_STEPS,
        per_device_train_batch_size=8,
        learning_rate=1e-2 if lora else 3e-3,
        lr_scheduler_type="cosine",
        warmup_steps=4,
        logging_steps=1,
        save_strategy="steps",
        save_steps=SAVE_STEPS,
        report_to=[],
        use_cpu=True,
        seed=0,
        data_seed=0,
        assistant_only_loss=True,
        max_length=128,
        disable_tqdm=True,
    )
    return SFTTrainer(
        model=AutoModelForCausalLM.from_pretrained(MODEL, dtype=torch.float32),
        args=config,
        train_dataset=datasets.load_dataset(
            "json", data_files=str(DATA), split="train", cache_dir=str(out / "hf-datasets")
        ),
        processing_class=AutoTokenizer.from_pretrained(MODEL),
        peft_config=LoraConfig(r=8, lora_alpha=16, target_modules="all-linear") if lora else None,
        callbacks=[probe],
    )


def listing(folder: Path) -> dict[str, int]:
    return {p.name: p.stat().st_size for p in sorted(folder.iterdir())}


def diff(a: dict[int, float], b: dict[int, float], steps: range) -> float:
    return max(abs(a[s] - b[s]) for s in steps)


def masking(trainer: SFTTrainer) -> dict[str, Any]:
    """TRL folds the template's assistant mask into a `labels` column (-100 = no loss)."""
    tok = trainer.processing_class
    row = trainer.train_dataset[0]
    batch = next(iter(trainer.get_train_dataloader()))
    return {
        "dataset_columns": trainer.train_dataset.column_names,
        "trl_swapped_chat_template": trainer.chat_template is not None,
        "first_record_full_text": tok.decode(row["input_ids"]),
        "first_record_loss_text": tok.decode([t for t in row["labels"] if t != -100]),
        "first_batch_row_loss_text": tok.decode(
            [t for t in batch["labels"][0].tolist() if t != -100]
        ),
    }


def resume_from(ckpt: Path, out: Path, lora: bool) -> tuple[SFTTrainer, Probe]:
    probe = Probe()
    trainer = make_trainer(out, lora, probe)
    trainer.train(resume_from_checkpoint=str(ckpt))
    return trainer, probe


def resume_check(work: Path, lora: bool) -> dict[str, Any]:
    ref = Probe(save_at=CUSTOM_SAVE_AT)
    ref_trainer = make_trainer(work / "ref", lora, ref)
    started = time.perf_counter()
    ref_trainer.train()
    ref_seconds = time.perf_counter() - started

    stopped = Probe(stop_at=STOP_AT)
    stop_trainer = make_trainer(work / "stopped", lora, stopped)
    stop_trainer.train()
    ckpt = work / "stopped" / f"checkpoint-{STOP_AT}"

    resumed_trainer, resumed = resume_from(ckpt, work / "stopped", lora)
    after = range(STOP_AT + 1, MAX_STEPS + 1)
    return {
        "train_seconds_40_steps": round(ref_seconds, 1),
        "loss_first_last": [ref.loss[1], ref.loss[MAX_STEPS]],
        "ref_saves": ref.saves,
        "ref_checkpoints": sorted(p.name for p in (work / "ref").glob("checkpoint-*")),
        "stopped_at_step": stop_trainer.state.global_step,
        "stopped_saves": stopped.saves,
        "checkpoint_files": listing(ckpt),
        "checkpoint_trainer_state_keys": sorted(
            json.loads((ckpt / "trainer_state.json").read_text())
        ),
        "resumed_first_step": resumed.step_ends[0],
        "resumed_final_step": resumed_trainer.state.global_step,
        "resumed_loss_max_abs_diff_vs_ref": diff(resumed.loss, ref.loss, after),
        "resumed_lr_max_abs_diff_vs_ref": diff(resumed.lr, ref.lr, after),
        "loss_at_stop_ref": ref.loss[STOP_AT],
        "loss_after_resume": {s: resumed.loss[s] for s in (STOP_AT + 1, MAX_STEPS)},
        "loss_ref": {s: ref.loss[s] for s in (STOP_AT + 1, MAX_STEPS)},
        "on_log_keys": sorted(ref.log_keys),
        "on_step_end_calls": len(ref.step_ends),
        "callbacks": [type(c).__name__ for c in ref_trainer.callback_handler.callbacks],
        "masking": masking(ref_trainer),
        "_ref_probe": ref,
    }


def ablation(work: Path, ckpt: Path, ref: Probe) -> dict[str, str]:
    """Delete one checkpoint file at a time and resume: what breaks or drifts?"""
    out: dict[str, str] = {}
    after = range(STOP_AT + 1, MAX_STEPS + 1)
    for name in sorted(listing(ckpt)):
        trial = work / f"ablate-{name}"
        shutil.copytree(ckpt, trial / ckpt.name)
        (trial / ckpt.name / name).unlink()
        try:
            _, probe = resume_from(trial / ckpt.name, trial, lora=False)
        except Exception as err:  # recording the failure is the point
            out[name] = f"resume fails: {type(err).__name__}: {str(err)[:160]}"
            continue
        loss, lr = diff(probe.loss, ref.loss, after), diff(probe.lr, ref.lr, after)
        same = loss < 1e-6 and lr < 1e-12
        out[name] = (
            "resumes identically"
            if same
            else f"resumes; first step {probe.step_ends[0]}, max loss diff {loss:.4g}, "
            f"max lr diff {lr:.3g}"
        )
    return out


def main() -> None:
    work = Path(sys.argv[1]).resolve()
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    results: dict[str, Any] = {
        "versions": {
            "python": platform.python_version(),
            "platform": f"{sys.platform}-{platform.machine()}",
            **{m.__name__: m.__version__ for m in (trl, transformers, peft, accelerate, datasets)},
            "torch": torch.__version__,
        },
        "torch_threads": torch.get_num_threads(),
    }
    full = resume_check(work / "full", lora=False)
    lora = resume_check(work / "lora", lora=True)
    results["ablation_full"] = ablation(
        work / "ablation", work / "full/stopped" / f"checkpoint-{STOP_AT}", full.pop("_ref_probe")
    )
    lora.pop("_ref_probe")
    results |= {"full": full, "lora": lora, "attempted_network_connections": attempted_connections}
    (work / "results.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
    print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
