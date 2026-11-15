"""The Doc 06 example math policy: its test cases, and the fixture rl.prompt set."""

import json
from pathlib import Path
from typing import Any

import pytest

from mrl_rewards.exec.testkit import score_policy
from mrl_rewards.policy import BatchFailedError
from mrl_rewards.types import Resolution, RewardSample, Status

ROOT = Path(__file__).parents[3]
POLICY = json.loads(
    (ROOT / "tests/contracts/examples/reward-policy/v1/valid-math.json").read_text()
)
STEP = 200  # past the format-warmup schedule: format weight 0.1, as in Doc 06 §1.4.2


def one(completion: str, reference: Any, tokens: int = 20) -> RewardSample:
    return RewardSample("s", "g", "What is 12*12?", completion, tokens, "stop", reference)


def test_doc06_cases() -> None:
    ok, untagged, spam = score_policy(
        POLICY,
        [
            one("<think>12*12=144</think><answer>144</answer>", {"answer": "144"}),
            one("144", {"answer": "144"}),
            one("<answer>144</answer><answer>143</answer><answer>145</answer>", {"answer": "144"}),
        ],
        trainer_step=STEP,
    )
    assert ok.total == pytest.approx(1.1, abs=1e-6)
    by_id = {c.component_id: c.value for c in ok.components}
    assert (by_id["correct"], by_id["format"]) == (1.0, 1.0)
    assert untagged.total == -0.5
    assert "gated" in untagged.flags
    assert {c.component_id: c.value for c in spam.components}["correct"] == 0.0


def test_scores_the_fixture_rl_prompt_set() -> None:
    """One batch per prompt group, as the harness sends them."""
    rows = [
        json.loads(line)
        for line in (ROOT / "tests/fixtures/datasets/rl.prompt.v1.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(rows) >= 2
    for i, row in enumerate(rows):
        prompt = row["prompt"] if isinstance(row["prompt"], str) else row["prompt"][-1]["content"]
        answer = prompt.removeprefix("Repeat: ")
        completions = [
            f"<think>copy it</think><answer>{answer}</answer>",
            answer,
            "<think>x</think>",
        ]
        group = [
            RewardSample(f"r{i}-{j}", f"r{i}", prompt, c, 12, "stop", row.get("reference"))
            for j, c in enumerate(completions)
        ]
        if "reference" not in row:
            # math.verify needs a reference: every sample fails, above max_failure_rate (5%).
            with pytest.raises(BatchFailedError) as err:
                score_policy(POLICY, group, trainer_step=STEP)
            assert err.value.failures_by_status == {"INVALID_INPUT": 3}
            continue
        tagged, untagged, no_answer = score_policy(POLICY, group, trainer_step=STEP)
        for s in (tagged, untagged, no_answer):
            assert s.resolution == Resolution.SCORED
            assert s.total is not None
            assert -1.0 <= s.total <= 1.2
            lang = {c.component_id: c for c in s.components}["lang"]
            assert lang.status == Status.NOT_APPLICABLE  # the reference has no lang
        assert tagged.total == pytest.approx(1.1)
        assert untagged.total == -0.5
        assert "gated" in untagged.flags
        assert no_answer.total == -0.5  # think only, no answer tag: gated too
