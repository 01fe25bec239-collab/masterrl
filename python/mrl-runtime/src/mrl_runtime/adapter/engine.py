"""TrainingEngine: the one interface every adapter implements (Doc 05 §11.1, API 1.0).

probe/validate/estimate are side-effect free and need no GPU. prepare, run, cleanup run inside
the job sandbox in that order. request_pause/request_cancel/checkpoint are called from the
harness thread while run() iterates in its own thread: they only set flags the engine checks at
its next safe point (a step boundary). checkpoint() asks for a checkpoint at the next safe point;
the engine answers by yielding CheckpointReady and keeps training.
"""

import threading
from collections.abc import Iterator
from typing import ClassVar, Protocol, runtime_checkable

from mrl_runtime.adapter.events import EngineEvent
from mrl_runtime.adapter.types import (
    EnvInfo,
    ExportContext,
    ExportResult,
    FailureInfo,
    HardwareProfile,
    JobContext,
    PreparedJob,
    ProbeReport,
    ResolvedConfig,
    ResourceEstimate,
    ValidationReport,
    ValidationRequest,
)

API_VERSION = "1.0"


@runtime_checkable
class TrainingEngine(Protocol):
    api_version: ClassVar[str]

    def probe(self, env: EnvInfo) -> ProbeReport: ...
    def validate(self, req: ValidationRequest) -> ValidationReport: ...
    def estimate(self, cfg: ResolvedConfig, hw: HardwareProfile) -> ResourceEstimate: ...
    def prepare(self, ctx: JobContext) -> PreparedJob: ...
    def run(self, ctx: JobContext) -> Iterator[EngineEvent]: ...
    def checkpoint(self, ctx: JobContext) -> None: ...
    def request_pause(self) -> None: ...
    def request_cancel(self) -> None: ...
    def export(self, ctx: ExportContext) -> ExportResult: ...
    def cleanup(self, ctx: JobContext) -> None: ...
    def classify_failure(
        self, exc: BaseException | None, exit_code: int | None, log_tail: str
    ) -> FailureInfo | None: ...


class BaseEngine:
    """Flag handling and the optional hooks. Subclasses implement probe..run, cleanup, export."""

    api_version: ClassVar[str] = API_VERSION

    def __init__(self) -> None:
        self.pause_requested = threading.Event()
        self.cancel_requested = threading.Event()
        self.checkpoint_requested = threading.Event()

    def request_pause(self) -> None:
        self.pause_requested.set()

    def request_cancel(self) -> None:
        self.cancel_requested.set()

    def checkpoint(self, ctx: JobContext) -> None:
        self.checkpoint_requested.set()

    def classify_failure(
        self, exc: BaseException | None, exit_code: int | None, log_tail: str
    ) -> FailureInfo | None:
        return None
