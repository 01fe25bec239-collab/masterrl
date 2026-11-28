"""The `reward_preview` task (REWARD_PREVIEW jobs): runs a reward version in its sandbox tier.

Modes (`inputs.mode`):
- `test`: score the version's test cases twice (deterministic components must agree) and
  record each case's outcome; the server decides pass/fail from it (the publish gate).
- `preview`: score N seeded samples of a dataset, with its stored completions or with
  `num_generations` fresh ones per prompt from a model (Hugging Face `generate` on CPU).
- `regression`: score the previous version's test cases with both versions.
- `compare`: score one sample set with two versions.

Inputs (written by the server's rewards module):

    {"mode": …, "reward": {"version_id", "sandbox_tier", "policy", …},
     "other": {… the second version (regression baseline, compare)},
     "cases": [{"position", "name", "sample", "expect"}], "baseline_cases": [...],
     "preview": {"n", "seed", "completions": "stored"|"generate", "num_generations",
                 "max_new_tokens", "dataset": <input name>, "model": <input name>,
                 "prompt_field", "completion_field", "reference_field"}}

Staged files: `reward.code/<file>` (and `reward_b.code/<file>` for the second version), the
dataset and the model inputs the preview names.

The reward service runs as `mrl-reward-sandboxed --jail` (see `mrl_rewards.server_bundle`):
its own bubblewrap jail inside the job's `code_exec` sandbox, so custom reward code never
shares a process, folder or namespace with this task, which writes the report. Outputs:
`report.json` (format `mrl-reward-report/1`) and, except for tests, `rows.jsonl` (one scored
sample per line; the server computes the statistics). This task imports no reward code.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import os
import random
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.rewards_client import (
    SANDBOX_VIOLATION,
    RewardClient,
    RewardInfraError,
    RewardOutcome,
    Sample,
)

REPORT = "report.json"
ROWS = "rows.jsonl"
REPORT_FORMAT = "mrl-reward-report/1"
MODES = ("test", "preview", "regression", "compare")
NO_GENERATION = "MRL-RWD-296"
SERVICE_FAILED = "MRL-RWD-293"
MAX_SAMPLES = 5000
MAX_GOLDEN_SAMPLES = 16
MAX_GOLDEN_BYTES = 128 << 10
MAX_GENERATIONS = 16
MAX_NEW_TOKENS = 1024
START_TIMEOUT_S = 60.0
LOG_EXCERPT = 4096


def _bad(message: str, path: str, hint: str = "") -> Issue:
    return Issue(TASK_INPUT_INVALID, message, path=path, hint=hint)


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:  # noqa: PLR0912
    mode = inputs.get("mode")
    if mode not in MODES:
        return [_bad(f"mode must be one of {', '.join(MODES)}.", "/inputs/mode")]
    issues: list[Issue] = []
    for key in ("reward", *(("other",) if mode in ("regression", "compare") else ())):
        r = inputs.get(key)
        if not isinstance(r, dict) or not isinstance(cast(dict[str, Any], r).get("policy"), dict):
            issues.append(_bad(f"{key} needs a version_id and a policy object.", f"/inputs/{key}"))
        elif not str(cast(dict[str, Any], r).get("version_id", "")).startswith("rwdv_"):
            issues.append(_bad(f"{key}.version_id must be a reward version id.", f"/inputs/{key}"))
    if mode == "test" and not inputs.get("cases"):
        issues.append(_bad("A test run needs test cases.", "/inputs/cases"))
    if mode == "regression" and not inputs.get("baseline_cases"):
        issues.append(
            _bad("A regression run needs the baseline's test cases.", "/inputs/baseline_cases")
        )
    if mode in ("preview", "compare"):
        p = cast(dict[str, Any], inputs.get("preview") or {})
        n = p.get("n", 5)
        g = p.get("num_generations", 1)
        if not isinstance(n, int) or not 1 <= n <= MAX_SAMPLES:
            issues.append(_bad(f"preview.n must be 1 to {MAX_SAMPLES}.", "/inputs/preview/n"))
        if not isinstance(g, int) or not 1 <= g <= MAX_GENERATIONS:
            issues.append(
                _bad(
                    f"preview.num_generations must be 1 to {MAX_GENERATIONS}.",
                    "/inputs/preview/num_generations",
                )
            )
        ds = p.get("dataset", "dataset")
        if ds not in staged:
            issues.append(_bad(f"No staged input is named {ds!r}.", "/inputs/preview/dataset"))
        if p.get("completions", "stored") == "generate":
            if p.get("model", "model") not in staged:
                issues.append(
                    _bad(
                        "Writing answers needs the model as a staged input.",
                        "/inputs/preview/model",
                    )
                )
            if not all(importlib.util.find_spec(m) for m in ("torch", "transformers")):
                issues.append(
                    Issue(
                        NO_GENERATION,
                        "This environment has no PyTorch and Transformers to write answers with.",
                        path="/inputs/preview/completions",
                        hint="Use completions: stored, or an environment with both installed.",
                    )
                )
    return issues


# ---- the reward service ----


class Service:
    """One sandboxed reward service for one version; stopped by `close()`."""

    def __init__(
        self, workdir: Path, staged: Mapping[str, Path], reward: Mapping[str, Any], prefix: str
    ) -> None:
        self.version = str(reward["version_id"])
        self.dir = Path(tempfile.mkdtemp(prefix="rs", dir=workdir))
        bundle = self.dir / "bundle"
        bundle.mkdir()
        (bundle / "policy.json").write_text(
            json.dumps(reward["policy"], sort_keys=True), encoding="utf-8"
        )
        code = [p for name, p in staged.items() if name.startswith(f"{prefix}.code/")]
        if code:
            (bundle / "code").mkdir()
            for p in code:
                (bundle / "code" / p.name).write_bytes(p.read_bytes())
        self.user_code = bool(code)
        socket_dir = self.dir / "s"
        socket_dir.mkdir()
        self.socket = socket_dir / "s.sock"
        self.nonce = secrets.token_hex(24)
        read, write = os.pipe()
        os.write(write, self.nonce.encode())
        os.close(write)
        tier = "2" if reward.get("sandbox_tier") == "code_exec" else "1"
        cmd = [
            sys.executable,
            "-I",
            "-m",
            "mrl_rewards.server_bundle.serve",
            "--jail",
            "--tier",
            tier,
            "--bundle",
            str(bundle),
            "--listen",
            f"unix:{self.socket}",
            "--reward-version-id",
            self.version,
            "--nonce-fd",
            str(read),
        ]
        cmd.extend(("--cache-entries", "0"))
        if self.user_code:
            cmd.append("--user-code")
        elif os.environ.get("MRL_USER_CODE_ALLOWED") != "1":
            # Not in an isolating sandbox (macOS, Windows, a non-isolating backend): the
            # shipped components are trusted, so they may run on a machine with a network.
            cmd.append("--allow-network")
        env = {k: v for k, v in os.environ.items() if k in _PASS_ENV}
        self.log = (self.dir / "service.log").open("w+b")
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=subprocess.STDOUT,
            env=env,
            pass_fds=(read,),
            start_new_session=True,
        )
        os.close(read)
        self.client = RewardClient(
            str(self.socket),
            nonce=self.nonce,
            reward_version_id=self.version,
            attempt_id=f"preview:{self.version}",
            expected_sha256=cast(str | None, reward.get("exec_sha256")),
            timeout_s=60.0,
        )

    def start(self) -> str:
        """Waits until it answers Describe; returns the content hash it reports."""
        deadline = time.monotonic() + START_TIMEOUT_S
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                code, message = self.start_failure()
                raise MrlError(code, message, detail=self.excerpt())
            if self.socket.exists():
                try:
                    return self.client.describe().content_sha256
                except RewardInfraError:
                    pass
            time.sleep(0.2)
        raise MrlError(
            SERVICE_FAILED, "The reward service didn't start in time.", detail=self.excerpt()
        )

    def start_failure(self) -> tuple[str, str]:
        """The code and message the service gave for not starting (its `start_failed` line)."""
        if self.violated_sandbox():
            return SANDBOX_VIOLATION, "The sandbox stopped the reward service."
        code, message = SERVICE_FAILED, "The reward service stopped while starting."
        for line in self.excerpt().splitlines():
            try:
                doc = json.loads(line)
            except ValueError:
                continue
            if (
                isinstance(doc, dict)
                and cast(dict[str, Any], doc).get("event") == "reward_exec.start_failed"
            ):
                d = cast(dict[str, Any], doc)
                if str(d.get("code", "")).startswith("MRL-"):
                    code, message = str(d["code"]), str(d.get("message", message))
        return code, message

    def violated_sandbox(self) -> bool:
        """Whether the service was stopped by the sandbox's system-call filter."""
        sigsys = getattr(signal, "SIGSYS", None)
        return sigsys is not None and self.proc.poll() in (-sigsys, 128 + sigsys)

    def score(self, samples: Sequence[Sample]) -> list[RewardOutcome]:
        """Scores a batch and reports sandbox termination distinctly from service downtime."""
        try:
            outcomes = self.client.score(samples)
        except RewardInfraError as e:
            if self.violated_sandbox():
                raise MrlError(SANDBOX_VIOLATION, "The sandbox stopped the reward service.") from e
            raise
        if any("sandbox_violation" in outcome.flags for outcome in outcomes):
            raise MrlError(SANDBOX_VIOLATION, "The sandbox stopped the reward code.")
        return outcomes

    def excerpt(self) -> str:
        """The end of the service's log."""
        self.log.flush()
        self.log.seek(0, os.SEEK_END)
        size = self.log.tell()
        self.log.seek(max(0, size - LOG_EXCERPT))
        return self.log.read().decode("utf-8", "replace")

    def close(self) -> None:
        self.client.close()
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.log.close()


_PASS_ENV = (
    "PATH",
    "LANG",
    "LC_ALL",
    "TZ",
    "HOME",
    "TMPDIR",
    "MRL_SANDBOX_PROFILE",
    "MRL_USER_CODE_ALLOWED",
    "MRL_JAIL_SECCOMP",
    "MRL_SUPERVISOR_SECCOMP",
)


# ---- samples ----


def _case_sample(case: Mapping[str, Any]) -> Sample:
    s = cast(Mapping[str, Any], case.get("sample") or {})
    return Sample(
        prompt=str(s.get("prompt", "")),
        completion=str(s.get("completion", "")),
        group_id=str(s.get("group_id") or f"case:{case.get('position', 0)}"),
        reference=s.get("reference"),
        completion_token_count=int(s.get("completion_token_count", 0) or 0),
        finish_reason=str(s.get("finish_reason", "stop")),
    )


def _read_rows(path: Path) -> Iterator[dict[str, Any]]:
    files = sorted(p for p in path.rglob("*") if p.is_file()) if path.is_dir() else [path]
    for file in files:
        if file.suffix == ".parquet":
            pq: Any = importlib.import_module("pyarrow.parquet")
            for batch in pq.ParquetFile(file).iter_batches(batch_size=1024):
                yield from cast(list[dict[str, Any]], batch.to_pylist())
        elif file.suffix in (".jsonl", ".json"):
            with file.open(encoding="utf-8") as stream:
                for line in stream:
                    if line.strip():
                        yield cast(dict[str, Any], json.loads(line))


def _prompt_text(value: object) -> str:
    if isinstance(value, list):
        return "\n".join(
            str(cast(dict[str, Any], m).get("content", ""))
            for m in cast(list[object], value)
            if isinstance(m, dict)
        )
    return str(value or "")


class _Generator:
    """Fresh completions with Hugging Face `generate` on CPU, loaded only when asked for."""

    def __init__(self, model_dir: Path, seed: int) -> None:
        self.torch: Any = importlib.import_module("torch")
        transformers: Any = importlib.import_module("transformers")
        self.torch.manual_seed(seed)
        self.tok: Any = transformers.AutoTokenizer.from_pretrained(
            str(model_dir), local_files_only=True, trust_remote_code=False
        )
        self.model: Any = transformers.AutoModelForCausalLM.from_pretrained(
            str(model_dir), local_files_only=True, trust_remote_code=False, use_safetensors=True
        )
        self.model.eval()

    def __call__(self, prompt: str, n: int, max_new_tokens: int) -> list[tuple[str, int, str]]:
        enc = self.tok(prompt, return_tensors="pt")
        with self.torch.no_grad():
            out = self.model.generate(
                **enc,
                do_sample=n > 1,
                num_return_sequences=n,
                max_new_tokens=max_new_tokens,
                pad_token_id=self.tok.eos_token_id,
            )
        start = int(enc["input_ids"].shape[1])
        res: list[tuple[str, int, str]] = []
        for seq in out:
            new = seq[start:]
            text = self.tok.decode(new, skip_special_tokens=True)
            finish = "length" if len(new) >= max_new_tokens else "stop"
            res.append((text, len(new), finish))
        return res


def _preview_samples(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Sample]:
    p = cast(Mapping[str, Any], inputs.get("preview") or {})
    rows = _read_rows(staged[str(p.get("dataset", "dataset"))])
    rng = random.Random(int(p.get("seed", 0)))
    n = int(p.get("n", 5))
    picked: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if index < n:
            picked.append(row)
        elif (slot := rng.randrange(index + 1)) < n:
            picked[slot] = row
    rng.shuffle(picked)
    pf, cf, rf = (
        str(p.get(k, d))
        for k, d in (
            ("prompt_field", "prompt"),
            ("completion_field", "completion"),
            ("reference_field", "reference"),
        )
    )
    gens = int(p.get("num_generations", 1))
    generate = p.get("completions", "stored") == "generate"
    gen = (
        _Generator(staged[str(p.get("model", "model"))], int(p.get("seed", 0)))
        if generate
        else None
    )
    out: list[Sample] = []
    for i, row in enumerate(picked):
        prompt = _prompt_text(row.get(pf))
        ref = row.get(rf)
        group = hashlib.sha256(prompt.encode()).hexdigest()[:16] + f":{i}"
        if gen is not None:
            for text, tokens, finish in gen(prompt, gens, int(p.get("max_new_tokens", 64))):
                out.append(Sample(prompt, text, group, ref, tokens, finish))
        else:
            completion = str(row.get(cf, ""))
            out.append(Sample(prompt, completion, group, ref, len(completion.split()), "stop"))
    return out


# ---- results ----


def _case_result(
    position: int, o: RewardOutcome, again: RewardOutcome | None, deterministic: set[str]
) -> dict[str, Any]:
    flags = list(o.flags)
    for cid, st in o.statuses.items():
        if st != "OK":
            flags.append(f"{st.lower()}:{cid}")
    repeatable = again is None or all(
        o.components.get(c) == again.components.get(c)
        and o.statuses.get(c) == again.statuses.get(c)
        and c in o.components
        and c in again.components
        for c in deterministic
    )
    return {
        "position": position,
        "total": o.value,
        "resolution": o.resolution,
        "components": dict(o.components),
        "statuses": dict(o.statuses),
        "flags": flags,
        "diagnostics": {k: dict(v) for k, v in o.diagnostics.items()},
        "repeatable": repeatable,
    }


def _row(i: int, s: Sample, o: RewardOutcome, version: str) -> dict[str, Any]:
    return {
        "i": i,
        "group_id": s.group_id,
        "version": version,
        "total": o.value,
        "resolution": o.resolution,
        "status": o.worst_status,
        "components": dict(o.components),
        "flags": list(o.flags),
        "completion_tokens": s.completion_token_count,
        "chunk_index": o.chunk_index,
        "latency_ms": o.latency_ms,
        "prompt_sha256": hashlib.sha256(s.prompt.encode()).hexdigest(),
        "completion": s.completion[:2000],
    }


def _golden_samples(samples: Sequence[Sample]) -> list[dict[str, Any]]:
    frozen: list[dict[str, Any]] = []
    remaining = MAX_GOLDEN_BYTES
    for sample in samples:
        entry = asdict(sample)
        n = len(json.dumps(entry, ensure_ascii=False).encode())
        if n <= remaining:
            frozen.append(entry)
            remaining -= n
            if len(frozen) == MAX_GOLDEN_SAMPLES:
                break
    return frozen


def _score_cases(
    svc: Service, cases: Sequence[Mapping[str, Any]], twice: bool
) -> list[dict[str, Any]]:
    samples = [_case_sample(c) for c in cases]
    det = {
        c.component_id
        for c in (svc.client.description.components if svc.client.description else ())
        if c.deterministic
    }
    first = svc.score(samples)
    second = svc.score(samples) if twice else [None] * len(samples)
    return [
        _case_result(int(c.get("position", i)), a, b, det)
        for i, (c, a, b) in enumerate(zip(cases, first, second, strict=True))
    ]


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    mode = str(ctx.inputs["mode"])
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="mrl-rp-"))
    services: list[Service] = []
    violation = False
    try:
        a = Service(work, ctx.staged_paths, cast(Mapping[str, Any], ctx.inputs["reward"]), "reward")
        services.append(a)
        report: dict[str, Any] = {
            "format": REPORT_FORMAT,
            "mode": mode,
            "reward_version_id": a.version,
            "content_sha256": a.start(),
            "cases": [],
        }
        yield Progress(step=1, total_steps=3, phase="reward_preview")
        b: Service | None = None
        if mode in ("regression", "compare"):
            b = Service(
                work, ctx.staged_paths, cast(Mapping[str, Any], ctx.inputs["other"]), "reward_b"
            )
            services.append(b)
            report["other_content_sha256"] = b.start()
        outputs = [TaskOutput(REPORT, role="report", format="json")]
        if mode == "test":
            report["cases"] = _score_cases(
                a, cast(list[Mapping[str, Any]], ctx.inputs["cases"]), twice=True
            )
        elif mode == "regression" and b is not None:
            base = cast(list[Mapping[str, Any]], ctx.inputs["baseline_cases"])
            report["cases"] = _score_cases(a, base, twice=False)
            report["baseline_cases"] = _score_cases(b, base, twice=False)
        else:
            samples = _preview_samples(ctx.inputs, ctx.staged_paths)
            if mode == "preview":
                report["golden_samples"] = _golden_samples(samples)
            yield Progress(step=2, total_steps=3, phase="reward_preview")
            lines: list[str] = []
            for tag, svc in (("a", a), *((("b", b),) if b is not None else ())):
                got = svc.score(samples)
                lines.extend(
                    json.dumps(_row(i, s, o, tag))
                    for i, (s, o) in enumerate(zip(samples, got, strict=True))
                )
                if ctx.cancelled():
                    return
            (ctx.output_dir / ROWS).write_text("\n".join(lines) + "\n", encoding="utf-8")
            outputs.append(TaskOutput(ROWS, role="data", format="jsonl"))
        report["sandbox"] = {
            "profile": os.environ.get("MRL_SANDBOX_PROFILE", ""),
            "user_code": a.user_code or (b.user_code if b else False),
            "log_excerpt": a.excerpt()[-LOG_EXCERPT:],
        }
        (ctx.output_dir / REPORT).write_text(json.dumps(report, sort_keys=True), encoding="utf-8")
        yield Progress(step=3, total_steps=3, phase="reward_preview")
        yield from outputs
    except MrlError as exc:
        violation = (
            exc.code == SANDBOX_VIOLATION
            and os.environ.get("MRL_SANDBOX_PROFILE") == "code_exec"
            and os.environ.get("MRL_USER_CODE_ALLOWED") == "1"
            and sys.platform.startswith("linux")
        )
        if not violation:
            raise
    finally:
        for service in services:
            service.close()
        shutil.rmtree(work)
    if violation:
        # The agent receives the same exit status as a bubblewrap wrapper that caught SIGSYS.
        os._exit(128 + signal.SIGSYS)


REWARD_PREVIEW = BuiltinTask(
    name="reward_preview",
    job_kind="REWARD_PREVIEW",
    artifact_kind="eval",
    validate=_validate,
    run=_run,
)

__all__ = ["REPORT", "REPORT_FORMAT", "REWARD_PREVIEW", "ROWS", "Service"]
