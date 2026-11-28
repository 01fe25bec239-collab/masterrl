"""The `reward_preview` task end to end: it starts the real `mrl-reward-sandboxed` service as a
subprocess (no jail here unless the machine has one) and writes the report the server reads."""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from mrl_runtime.builtin import reward_preview
from mrl_runtime.builtin.engine import discover_tasks
from mrl_runtime.builtin.reward_preview import REPORT, REPORT_FORMAT, REWARD_PREVIEW, ROWS
from mrl_runtime.builtin.tasks import TaskContext, TaskOutput
from mrl_runtime.errors import MrlError
from mrl_runtime.rewards_client import SANDBOX_VIOLATION

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets")


def policy(name: str = "math") -> dict[str, Any]:
    return {
        "apiVersion": "mrl.dev/reward/v1",
        "kind": "RewardPolicy",
        "metadata": {"name": name, "signal": "scalar.sequence"},
        "spec": {
            "components": [{"id": "correct", "use": "builtin/math.verify@1", "weight": 1.0}],
            "aggregation": {"method": "weighted_sum", "where": "platform"},
            "failure": {
                "on_timeout": {"action": "mask"},
                "on_error": {"action": "mask"},
                "max_failure_rate": 1.0,
            },
            "tests": "tests.yaml",
        },
    }


def reward(version: str = "rwdv_a", **kw: Any) -> dict[str, Any]:
    return {"version_id": version, "sandbox_tier": "standard", "policy": policy(), **kw}


CASES = [
    {
        "position": 0,
        "name": "right",
        "sample": {"prompt": "1+1?", "completion": "<answer>2</answer>", "reference": "2"},
        "expect": {},
    },
    {
        "position": 1,
        "name": "wrong",
        "sample": {"prompt": "1+1?", "completion": "<answer>3</answer>", "reference": "2"},
        "expect": {},
    },
]


@pytest.fixture
def tmp() -> Iterator[Path]:
    # Short path: the service's Unix socket lives under it.
    with tempfile.TemporaryDirectory(prefix="rp", dir="/tmp") as d:
        yield Path(d)


def run(
    tmp: Path, inputs: dict[str, Any], staged: dict[str, Path] | None = None
) -> tuple[dict[str, Any], list[TaskOutput], Path]:
    staged = staged or {}
    assert REWARD_PREVIEW.validate(inputs, staged) == []
    out = tmp / "out"
    events = list(REWARD_PREVIEW.run(TaskContext(inputs, staged, out, lambda: False)))
    outputs = [e for e in events if isinstance(e, TaskOutput)]
    return json.loads((out / REPORT).read_text(encoding="utf-8")), outputs, out


def test_the_task_is_registered_for_reward_preview_jobs() -> None:
    tasks, problems = discover_tasks()
    assert problems == []
    assert tasks["reward_preview"].job_kind == "REWARD_PREVIEW"


def test_test_mode_scores_every_case_twice_and_reports_it(tmp: Path) -> None:
    report, outputs, _ = run(tmp, {"mode": "test", "reward": reward(), "cases": CASES})
    assert report["format"] == REPORT_FORMAT
    assert report["mode"] == "test"
    assert report["reward_version_id"] == "rwdv_a"
    assert len(report["content_sha256"]) == 64
    got = {c["position"]: c for c in report["cases"]}
    assert got[0]["total"] == 1.0
    assert got[1]["total"] == 0.0
    assert all(c["repeatable"] and c["resolution"] == "SCORED" for c in got.values())
    assert got[0]["components"] == {"correct": 1.0}
    assert [o.path for o in outputs] == [REPORT]


def test_preview_scores_seeded_samples_of_a_dataset(tmp: Path) -> None:
    data = tmp / "data.jsonl"
    data.write_text(
        "\n".join(
            json.dumps(
                {"prompt": f"{i}+0?", "completion": f"<answer>{i}</answer>", "reference": str(i)}
            )
            for i in range(20)
        ),
        encoding="utf-8",
    )
    inputs = {"mode": "preview", "reward": reward(), "preview": {"n": 6, "seed": 7}}
    report, outputs, out = run(tmp, inputs, {"dataset": data})
    rows = [json.loads(line) for line in (out / ROWS).read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 6
    assert {r["version"] for r in rows} == {"a"}
    assert all(r["total"] == 1.0 for r in rows)
    assert [o.path for o in outputs] == [REPORT, ROWS]
    (tmp / "again").mkdir()
    _, _, out2 = run(tmp / "again", inputs, {"dataset": data})
    again = [
        json.loads(line)["prompt_sha256"]
        for line in (out2 / ROWS).read_text(encoding="utf-8").splitlines()
    ]
    assert again == [r["prompt_sha256"] for r in rows], "the same seed picks the same samples"
    assert report["sandbox"]["user_code"] is False


def test_compare_and_regression_score_with_both_versions(tmp: Path) -> None:
    data = tmp / "data.jsonl"
    data.write_text(
        json.dumps({"prompt": "1+1?", "completion": "<answer>2</answer>", "reference": "2"}) + "\n",
        encoding="utf-8",
    )
    report, _, out = run(
        tmp,
        {"mode": "compare", "reward": reward(), "other": reward("rwdv_b"), "preview": {"n": 1}},
        {"dataset": data},
    )
    rows = [json.loads(line) for line in (out / ROWS).read_text(encoding="utf-8").splitlines()]
    assert sorted(r["version"] for r in rows) == ["a", "b"]
    assert report["other_content_sha256"] == report["content_sha256"], (
        "same policy, same bundle hash"
    )
    report, _, _ = run(
        tmp,
        {
            "mode": "regression",
            "reward": reward(),
            "other": reward("rwdv_b"),
            "baseline_cases": CASES,
        },
    )
    assert len(report["cases"]) == 2
    assert len(report["baseline_cases"]) == 2


def test_inputs_are_checked_before_anything_runs(tmp: Path) -> None:
    v = REWARD_PREVIEW.validate
    assert v({"mode": "train"}, {})[0].path == "/inputs/mode"
    assert (
        v({"mode": "test", "reward": {"version_id": "x", "policy": {}}, "cases": CASES}, {})[0].path
        == "/inputs/reward"
    )
    assert v({"mode": "test", "reward": reward()}, {})[0].path == "/inputs/cases"
    assert (
        v({"mode": "regression", "reward": reward(), "baseline_cases": CASES}, {})[0].path
        == "/inputs/other"
    )
    bad = v({"mode": "preview", "reward": reward(), "preview": {"n": 0, "num_generations": 99}}, {})
    assert {i.path for i in bad} == {
        "/inputs/preview/n",
        "/inputs/preview/num_generations",
        "/inputs/preview/dataset",
    }


def test_writing_answers_needs_torch_and_the_model(tmp: Path) -> None:
    if importlib.util.find_spec("torch") is not None:
        pytest.skip("PyTorch is installed here")
    issues = REWARD_PREVIEW.validate(
        {"mode": "preview", "reward": reward(), "preview": {"completions": "generate"}},
        {"dataset": tmp},
    )
    assert {i.code for i in issues} >= {"MRL-RWD-296"}
    assert "/inputs/preview/model" in {i.path for i in issues}


def test_custom_code_without_a_jail_is_refused_with_its_own_code(tmp: Path) -> None:
    if sys.platform.startswith("linux"):
        pytest.skip("Linux machines may have a jail; the sandbox suite covers them")
    code = tmp / "staged"
    code.mkdir()
    (code / "components.yaml").write_text(
        "components: [{module: mine, class: Mine}]\n", encoding="utf-8"
    )
    (code / "mine.py").write_text("raise SystemExit('must never run')\n", encoding="utf-8")
    staged = {
        "reward.code/components.yaml": code / "components.yaml",
        "reward.code/mine.py": code / "mine.py",
    }
    with pytest.raises(MrlError) as e:
        list(
            REWARD_PREVIEW.run(
                TaskContext(
                    {"mode": "test", "reward": reward(), "cases": CASES},
                    staged,
                    tmp / "out",
                    lambda: False,
                )
            )
        )
    assert e.value.code == "MRL-RWD-295", e.value
    assert not (tmp / "out" / REPORT).exists()


@pytest.mark.skipif(
    not (sys.platform.startswith("linux") and os.environ.get("MRL_JAIL_SECCOMP")),
    reason="needs Linux, bubblewrap and an agent filter in MRL_JAIL_SECCOMP (ci-security-rewards)",
)
def test_with_a_jail_the_reward_service_runs_inside_it(tmp: Path) -> None:
    report, _, _ = run(tmp, {"mode": "test", "reward": reward(), "cases": CASES})
    log = report["sandbox"]["log_excerpt"]
    assert "reward_exec.ready" in log, log
    assert "jail_skipped" not in log, log
    assert [c["total"] for c in sorted(report["cases"], key=lambda c: c["position"])] == [1.0, 0.0]


@pytest.mark.parametrize("mode", ["preview", "compare"])
def test_cpu_model_generates_and_scores_local_completions(tmp: Path, mode: str) -> None:
    torch = pytest.importorskip("torch")
    hf = pytest.importorskip("transformers")
    tk = pytest.importorskip("tokenizers")
    assert torch.version.cuda is None
    model = tmp / "model"
    tokenizer = tk.Tokenizer(
        tk.models.WordLevel({"[unk]": 0, "hello": 1, "[end]": 2}, unk_token="[unk]")
    )
    tokenizer.pre_tokenizer = tk.pre_tokenizers.Whitespace()
    hf.PreTrainedTokenizerFast(
        tokenizer_object=tokenizer,
        unk_token="[unk]",
        eos_token="[end]",
    ).save_pretrained(model)
    hf.GPT2LMHeadModel(
        hf.GPT2Config(
            vocab_size=3,
            n_layer=1,
            n_head=1,
            n_embd=8,
            n_positions=16,
            bos_token_id=2,
            eos_token_id=2,
            pad_token_id=2,
            tie_word_embeddings=False,
        )
    ).save_pretrained(model)
    data = tmp / "rows.jsonl"
    data.write_text('{"prompt":"hello","reference":"42"}\n', encoding="utf-8")
    inputs = {
        "mode": mode,
        "reward": reward(),
        "other": reward("rwdv_b"),
        "preview": {"n": 1, "completions": "generate", "num_generations": 2, "max_new_tokens": 2},
    }
    _, _, out = run(tmp, inputs, {"model": model, "dataset": data})
    rows = [json.loads(line) for line in (out / ROWS).read_text(encoding="utf-8").splitlines()]
    assert sorted(row["version"] for row in rows) == (
        ["a", "a", "b", "b"] if mode == "compare" else ["a", "a"]
    )
    assert all(row["completion_tokens"] > 0 and row["total"] is not None for row in rows)


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="bubblewrap exit status")
def test_preview_propagates_a_nested_sandbox_failure_to_agent(
    tmp: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StoppedService:
        version = "rwdv_a"

        def __init__(self, *_args: object) -> None:
            pass

        def start(self) -> str:
            raise MrlError(SANDBOX_VIOLATION, "The sandbox stopped the service.")

        def close(self) -> None:
            pass

    def exit_with_status(status: int) -> None:
        raise SystemExit(status)

    monkeypatch.setenv("MRL_SANDBOX_PROFILE", "code_exec")
    monkeypatch.setenv("MRL_USER_CODE_ALLOWED", "1")
    monkeypatch.setattr(reward_preview, "Service", StoppedService)
    monkeypatch.setattr(os, "_exit", exit_with_status)
    with pytest.raises(SystemExit) as reported:
        run(tmp, {"mode": "test", "reward": reward(), "cases": CASES})
    assert reported.value.code == 128 + signal.SIGSYS
    assert not (tmp / "out" / REPORT).exists()
