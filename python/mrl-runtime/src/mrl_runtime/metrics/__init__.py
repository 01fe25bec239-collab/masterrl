"""MetricsBatch and SampleBatch emitters.

Engine-side metric names map to canonical names through, in order: a declared metric's
`source`, a declared canonical name, an exact `spec.metrics.namespaces` key, the longest
matching namespace prefix. Declared metrics carry their freshness (live, sampled, delayed);
anything else is forwarded as undeclared (Doc 05 §11.6 rule 6).
"""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.adapter.events import Metrics, Samples
from mrl_runtime.adapter.manifest import DeclaredMetric
from mrl_runtime.redact import Redactor

FRESHNESS: dict[str, pb.MetricKind] = {
    "live": pb.METRIC_KIND_LIVE,
    "sampled": pb.METRIC_KIND_SAMPLED,
    "delayed": pb.METRIC_KIND_DELAYED,
}


class MetricsMapper:
    def __init__(self, declared: Sequence[DeclaredMetric], namespaces: Mapping[str, str]) -> None:
        self._by_name = {m.name: m for m in declared}
        self._by_source = {m.source: m for m in declared if m.source}
        self._namespaces = sorted(namespaces.items(), key=lambda kv: len(kv[0]), reverse=True)
        self.seen: set[str] = set()
        self.undeclared: set[str] = set()

    def canonical(self, engine_name: str) -> tuple[str, DeclaredMetric | None]:
        if engine_name in self._by_source:
            metric = self._by_source[engine_name]
            return metric.name, metric
        name = engine_name
        if name not in self._by_name:
            for key, value in self._namespaces:
                if name == key or name.startswith(key):
                    name = value + name[len(key) :]
                    break
        return name, self._by_name.get(name)

    def batch(self, event: Metrics) -> pb.MetricsBatch:
        out = pb.MetricsBatch()
        for engine_name, value in event.values.items():
            name, declared = self.canonical(engine_name)
            point = out.points.add(
                name=name,
                step=event.step,
                value=float(value),
                split=event.split,
                kind=FRESHNESS[declared.kind] if declared else pb.METRIC_KIND_LIVE,
                undeclared=declared is None,
            )
            point.time.GetCurrentTime()
            self.seen.add(name)
            if declared is None:
                self.undeclared.add(name)
        return out

    def missing_required(self) -> list[str]:
        return sorted(
            m.name for m in self._by_name.values() if m.required and m.name not in self.seen
        )


class SampleEmitter:
    """Redacts samples; a batch above the message cap goes to a staged JSONL file instead."""

    def __init__(self, staging_dir: Path, redactor: Redactor, max_message_bytes: int) -> None:
        self._staging = staging_dir
        self._dir = staging_dir / "samples"
        self._redactor = redactor
        self._max = max_message_bytes
        self._files = 0

    def batch(self, event: Samples) -> pb.SampleBatch:
        out = pb.SampleBatch(step=event.step)
        for s in event.items:
            out.samples.add(
                sample_id=s.sample_id,
                group_id=s.group_id,
                prompt=self._redactor.text(s.prompt),
                completion=self._redactor.text(s.completion),
                reward_total=s.reward_total,
                reward_components=dict(s.reward_components),
                truncated=s.truncated,
                completion_tokens=s.completion_tokens,
                policy_version=s.policy_version,
            )
        if out.ByteSize() <= self._max * 9 // 10:  # leave room for the envelope
            return out
        self._dir.mkdir(parents=True, exist_ok=True)
        self._files += 1
        path = self._dir / f"step-{event.step:09d}-{self._files:04d}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for s in out.samples:
                row = {
                    "sample_id": s.sample_id,
                    "group_id": s.group_id,
                    "prompt": s.prompt,
                    "completion": s.completion,
                    "reward_total": s.reward_total,
                    "reward_components": dict(s.reward_components),
                    "truncated": s.truncated,
                    "completion_tokens": s.completion_tokens,
                    "policy_version": s.policy_version,
                }
                f.write(json.dumps(row) + "\n")
        # The agent uploads staged files and rewrites this local ref to an object-store ref.
        return pb.SampleBatch(
            step=event.step, overflow_ref=path.relative_to(self._staging).as_posix()
        )
