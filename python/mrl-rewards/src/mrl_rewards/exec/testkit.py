"""Run mrl-reward-exec inside this process, for tests and local development.

This is the only path that loads a bundle's user code (code/), and it does so with no
sandbox: never point it at code you don't trust. Running user code in the reward sandbox is
P29's job (the sandbox launcher); until then `mrl-reward-exec` itself refuses user code.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

import grpc

from mrl_rewards.exec.bundle import LoadedBundle, load_bundle
from mrl_rewards.exec.server import Audit, ExecLimits, RewardExecService, serve
from mrl_rewards.policy.model import load_policy
from mrl_rewards.policy.runtime import NormalizerState, PolicyRuntime, SampleScore
from mrl_rewards.registry import builtin_registry
from mrl_rewards.types import JSONValue, RewardBatch, RewardSample


@dataclass
class LocalService:
    service: RewardExecService
    server: grpc.Server
    target: str  # gRPC target for a client channel
    nonce: str
    reward_version_id: str

    @property
    def bundle(self) -> LoadedBundle:
        return self.service.bundle

    def close(self) -> None:
        self.server.stop(grace=None).wait()
        self.service.close()

    def __enter__(self) -> LocalService:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def start_local(  # noqa: PLR0913 (keyword-only settings)
    bundle_dir: Path,
    *,
    address: str = "127.0.0.1:0",
    nonce: str = "testkit-nonce",
    reward_version_id: str = "rwdv_testkit",
    limits: ExecLimits | None = None,
    audit: Audit | None = None,
    allow_user_code: bool = True,
) -> LocalService:
    """Load the bundle (user code allowed by default) and serve it on `address`."""
    bundle = load_bundle(bundle_dir, allow_user_code=allow_user_code)
    runtime = PolicyRuntime.build(bundle.policy, bundle.registry, bundle_dir=bundle.root)
    service = RewardExecService(
        bundle,
        runtime,
        nonce=nonce,
        reward_version_id=reward_version_id,
        limits=limits,
        audit=audit,
        on_stuck=lambda: audit("stuck_ignored_in_testkit") if audit else None,
    )
    server, port = serve(service, address)
    target = address if address.startswith("unix:") else f"{address.rpartition(':')[0]}:{port}"
    return LocalService(service, server, target, nonce, reward_version_id)


def score_policy(
    policy: JSONValue,
    samples: Sequence[RewardSample],
    *,
    trainer_step: int = 0,
    state: NormalizerState | None = None,
) -> list[SampleScore]:
    """Score samples with a built-in-only policy document, no server (local `test` runs)."""
    runtime = PolicyRuntime.build(load_policy(policy), builtin_registry())
    try:
        return runtime.score(RewardBatch(samples, trainer_step), state=state)
    finally:
        runtime.teardown()


__all__ = ["LocalService", "score_policy", "start_local"]
