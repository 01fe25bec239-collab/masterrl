"""Pieces every TRL trainer family shares: offline mode, device facts, the callback bridge that
turns HF Trainer callbacks into engine events, and resume-checkpoint checks.

The bridge runs `trainer.train()` on its own thread. Callbacks put events on a queue; `run()`
yields them to the harness. A checkpoint event blocks training until the harness has staged
the folder (it moves it), so the trainer never writes into a folder that is being moved.
"""

# pyright: reportMissingImports=false
# (the root type check runs without this adapter's environment)

import json
import math
import os
import queue
import threading
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from mrl_runtime.adapter import (
    AdapterInfo,
    CheckpointKind,
    CheckpointReady,
    EngineEvent,
    Metrics,
    Progress,
    Samples,
)

# Rule 3 (Doc 05 §11.6): no network. Set before transformers/datasets are imported.
OFFLINE_ENV = {
    "HF_HUB_OFFLINE": "1",
    "HF_DATASETS_OFFLINE": "1",
    "TRANSFORMERS_OFFLINE": "1",
    "HF_HUB_DISABLE_TELEMETRY": "1",
    "TOKENIZERS_PARALLELISM": "false",
    "HF_DATASETS_DISABLE_PROGRESS_BARS": "1",
    "HF_HUB_DISABLE_PROGRESS_BARS": "1",
}
os.environ.update(OFFLINE_ENV)


def _thread_lock_for_progress_bars() -> None:
    """tqdm's default lock is a multiprocessing lock, which starts multiprocessing's resource
    tracker: a child process that outlives the worker and holds its captured stdout/stderr
    open. Training runs in one process, so a thread lock is enough."""
    from tqdm import tqdm  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

    tqdm.set_lock(threading.RLock())


_thread_lock_for_progress_bars()

# What a resumable HF Trainer checkpoint must hold (DG-03 spike): TRL silently restarts the
# schedule when the optimizer or scheduler file is missing, so a missing one is an error here.
STATE_FILES = ("trainer_state.json", "optimizer.pt", "scheduler.pt", "rng_state.pth")
ADAPTER_FILES = ("adapter_model.safetensors", "adapter_config.json")
CHECKPOINT_MANIFEST = "checkpoint-manifest.json"
ACK_POLL_S = 0.2

DeviceKind = Literal["cuda", "mps", "cpu"]


@dataclass(frozen=True)
class Device:
    """The accelerator this process trains on."""

    kind: DeviceKind
    name: str
    bf16: bool
    memory_gib: float | None = None


def detect_device() -> Device:
    import torch  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

    if torch.cuda.is_available():
        props = torch.cuda.get_device_properties(0)
        return Device(
            "cuda", props.name, torch.cuda.is_bf16_supported(), props.total_memory / 2**30
        )
    if torch.backends.mps.is_available():
        return Device("mps", "Apple GPU", bf16=False)
    return Device("cpu", "CPU", bf16=False)


def missing_resume_files(path: Path, adapter: bool) -> list[str]:
    """Files a resume needs that `path` lacks (weights or adapter, plus the training state)."""
    names = {p.name for p in path.iterdir()} if path.is_dir() else set()
    weights = ADAPTER_FILES if adapter else ()
    missing = [n for n in (*weights, *STATE_FILES) if n not in names]
    has_full = "model.safetensors" in names or "model.safetensors.index.json" in names
    if not adapter and not has_full:
        missing.insert(0, "model.safetensors")
    return missing


def adapter_info(checkpoint: Path, method: Literal["lora", "qlora"]) -> AdapterInfo:
    """LoRA facts from the adapter_config.json PEFT wrote (target modules as resolved)."""
    cfg = json.loads((checkpoint / "adapter_config.json").read_text(encoding="utf-8"))
    targets = cfg.get("target_modules") or []
    modules = (targets,) if isinstance(targets, str) else tuple(sorted(targets))
    return AdapterInfo(method, int(cfg["r"]), float(cfg["lora_alpha"]), modules)


class _Done:
    pass


@dataclass(frozen=True)
class _Failed:
    exc: BaseException


_Item = tuple[EngineEvent, threading.Event] | _Done | _Failed
Sampler = Callable[[Any, Any, int], Samples | None]


@dataclass
class Flags:
    """The engine's control flags (BaseEngine sets them from the harness thread)."""

    pause: threading.Event
    cancel: threading.Event
    checkpoint: threading.Event


def _trainer_callback_base() -> type:
    from transformers import TrainerCallback  # noqa: PLC0415 (lazy: probe() reports a broken ML environment)

    return TrainerCallback


def make_bridge(  # noqa: PLR0913 (keyword-only settings)
    flags: Flags,
    *,
    sources: Mapping[str, str],
    kind: CheckpointKind,
    method: Literal["full", "lora", "qlora"],
    sampler: Sampler | None,
    samples_every: int,
) -> Any:
    """A TrainerCallback that reports progress, declared metrics, checkpoints and samples.

    `sources` maps engine metric names (as Trainer logs them) to their split; only those are
    forwarded, so every emitted metric is declared in the manifest.
    """
    base = _trainer_callback_base()

    class Bridge(base):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            self.events: queue.Queue[_Item] = queue.Queue()

        def emit(self, event: EngineEvent, wait: bool = False) -> None:
            ack = threading.Event()
            self.events.put((event, ack))
            while wait and not ack.wait(ACK_POLL_S):
                if flags.cancel.is_set():
                    return

        def on_step_end(self, args: Any, state: Any, control: Any, **kw: Any) -> None:
            step = int(state.global_step)
            self.emit(Progress(step, int(state.max_steps), epoch=float(state.epoch or 0.0)))
            if flags.cancel.is_set():
                control.should_save = False
                control.should_training_stop = True
                return
            if step >= state.max_steps or flags.pause.is_set():
                control.should_log = True
            if step >= state.max_steps or flags.checkpoint.is_set():
                control.should_save = True
            if flags.pause.is_set():
                control.should_save = True
                control.should_training_stop = True
            if sampler is not None and samples_every and step % samples_every == 0:
                batch = sampler(kw.get("model"), kw.get("processing_class"), step)
                if batch is not None:
                    self.emit(batch)

        def on_log(self, args: Any, state: Any, control: Any, logs: Any = None, **kw: Any) -> None:
            values = {
                k: float(v)
                for k, v in (logs or {}).items()
                if k in sources and isinstance(v, int | float) and not isinstance(v, bool)
            }
            for split in sorted({sources[k] for k in values}):
                part = {k: v for k, v in values.items() if sources[k] == split}
                self.emit(Metrics(int(state.global_step), part, split=split))

        def on_save(self, args: Any, state: Any, control: Any, **kw: Any) -> None:
            flags.checkpoint.clear()
            step = int(state.global_step)
            path = Path(args.output_dir) / f"checkpoint-{step}"
            info = adapter_info(path, method) if method in ("lora", "qlora") else None
            final = step >= state.max_steps
            self.emit(CheckpointReady(step, path, kind, is_final=final, adapter=info), wait=True)

    return Bridge()


def drain(bridge: Any, train: Callable[[], object]) -> Iterator[EngineEvent]:
    """Run `train` on its own thread and yield the bridge's events until it returns.

    An exception in `train` is raised here, so the harness classifies it. Closing the
    generator early releases a trainer waiting on a checkpoint.
    """

    def target() -> None:
        try:
            train()
        except BaseException as exc:  # handed to run(), which re-raises it to the harness
            bridge.events.put(_Failed(exc))
        else:
            bridge.events.put(_Done())

    thread = threading.Thread(target=target, name="mrl-trl-train", daemon=True)
    thread.start()
    while True:
        item = bridge.events.get()
        if isinstance(item, _Done):
            thread.join()
            return
        if isinstance(item, _Failed):
            thread.join()
            raise item.exc
        event, ack = item
        try:
            yield event
        finally:
            ack.set()


def update_steps(batches_per_epoch: int, accumulation: int, epochs: float, max_steps: int) -> int:
    """Optimizer steps the Trainer will run (transformers' own formula)."""
    if max_steps > 0:
        return max_steps
    per_epoch = max(
        batches_per_epoch // accumulation + int(batches_per_epoch % accumulation > 0), 1
    )
    return math.ceil(epochs * per_epoch)
