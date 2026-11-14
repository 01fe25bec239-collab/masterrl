"""Dataset fixtures: valid files pass, every invalid line has exactly its listed defect.

`defects()` is the reference reading of the canonical schemas. Later validators
(Rust or Python) must reject each `-invalid` line with the code in DEFECTS.json.
"""

import itertools
import json
import math
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

DATASETS = Path(__file__).parent / "datasets"
MANIFEST = json.loads((DATASETS / "DEFECTS.json").read_text())
MAX_RECORD_BYTES: int = MANIFEST["max_record_bytes"]
CHAT_ROLES = {"system", "user", "assistant"}
AGENT_ROLES = CHAT_ROLES | {"tool"}


def _text(rec: dict[str, Any], key: str, out: list[str]) -> None:
    if key not in rec:
        out.append("missing_field")
    elif not isinstance(rec[key], str):
        out.append("wrong_type")
    elif not rec[key]:
        out.append("empty_field")


def _role_order(roles: list[str], out: list[str]) -> None:
    """Optional leading system, then user/assistant alternating, ending with assistant."""
    body = roles[1:] if roles[:1] == ["system"] else roles
    expected = ["user", "assistant"] * (len(body) // 2 + 1)
    if not body or body != expected[: len(body)] or body[-1] != "assistant":
        out.append("role_order")


def _messages(value: Any, out: list[str], parts: bool = False, order: bool = True) -> None:
    if not isinstance(value, list):
        out.append("wrong_type")
        return
    if not value:
        out.append("empty_field")
        return
    roles: list[str] = []
    for msg in value:
        if not isinstance(msg, dict) or "role" not in msg or "content" not in msg:
            out.append("missing_field")
            return
        if msg["role"] not in CHAT_ROLES:
            out.append("unknown_role")
            return
        roles.append(msg["role"])
        if parts:
            _parts(msg["content"], out)
        else:
            _text(msg, "content", out)
    if order:
        _role_order(roles, out)


def _parts(content: Any, out: list[str]) -> None:
    if not isinstance(content, list) or not content:
        out.append("empty_field" if content == [] else "wrong_type")
        return
    for part in content:
        kind = part.get("type") if isinstance(part, dict) else None
        if kind == "text":
            _text(part, "text", out)
        elif kind == "image_ref":
            _text(part, "image_ref", out)
        else:
            out.append("unknown_part_type")


def _text_or_messages(rec: dict[str, Any], key: str, out: list[str]) -> None:
    if isinstance(rec.get(key), list):
        _messages(rec[key], out, order=False)
    else:
        _text(rec, key, out)


def _optional(rec: dict[str, Any], key: str, kind: type | tuple[type, ...], out: list[str]) -> None:
    if key in rec and (not isinstance(rec[key], kind) or isinstance(rec[key], bool)):
        out.append("wrong_type")


def _number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _pairwise(rec: dict[str, Any], out: list[str]) -> None:
    for key in ("prompt", "chosen", "rejected"):
        _text_or_messages(rec, key, out)
    if "chosen" in rec and rec.get("chosen") == rec.get("rejected"):
        out.append("chosen_equals_rejected")
    if "margin" in rec and not _number(rec["margin"]):
        out.append("wrong_type")
    _optional(rec, "annotator_ids", list, out)


def _sft_messages(rec: dict[str, Any], out: list[str]) -> None:
    if "messages" not in rec:
        out.append("missing_field")
    else:
        _messages(rec["messages"], out)
    _optional(rec, "tools", list, out)
    _optional(rec, "images", list, out)


def _prompt_completion(rec: dict[str, Any], out: list[str]) -> None:
    _text_or_messages(rec, "prompt", out)
    _text_or_messages(rec, "completion", out)


def _unpaired(rec: dict[str, Any], out: list[str]) -> None:
    _prompt_completion(rec, out)
    if "label" not in rec:
        out.append("missing_field")
    elif not isinstance(rec["label"], bool):
        out.append("label_not_bool")


def _pointwise(rec: dict[str, Any], out: list[str]) -> None:
    _prompt_completion(rec, out)
    if "score" not in rec:
        out.append("missing_field")
    elif not _number(rec["score"]):
        out.append("wrong_type")


def _process(rec: dict[str, Any], out: list[str]) -> None:
    _text(rec, "prompt", out)
    steps, labels = rec.get("completions"), rec.get("labels")
    if not isinstance(steps, list) or not isinstance(labels, list):
        out.append("missing_field")
    elif not steps or not all(isinstance(s, str) and s for s in steps):
        out.append("empty_field")
    elif len(steps) != len(labels):
        out.append("length_mismatch")
    elif not all(isinstance(label, bool) for label in labels):
        out.append("wrong_type")


def _rl_prompt(rec: dict[str, Any], out: list[str]) -> None:
    _text_or_messages(rec, "prompt", out)
    _optional(rec, "reference", str, out)
    _optional(rec, "verifier", (str, dict), out)
    _optional(rec, "meta", dict, out)


def _agent(rec: dict[str, Any], out: list[str]) -> None:
    _text(rec, "episode_id", out)
    _text(rec, "env_spec_ref", out)
    turns = rec.get("turns")
    if not isinstance(turns, list):
        out.append("missing_field")
        return
    if not turns:
        out.append("empty_field")
    for turn in turns:
        if turn.get("role") not in AGENT_ROLES:
            out.append("unknown_role")
        elif not isinstance(turn.get("content"), str):
            out.append("wrong_type")
        elif not (turn["content"] or turn.get("tool_calls") or turn.get("tool_results")):
            out.append("empty_field")


def _distill(rec: dict[str, Any], out: list[str]) -> None:
    _text_or_messages(rec, "prompt", out)
    teachers = [k for k in ("teacher_completion", "teacher_logits_ref") if k in rec]
    if not teachers:
        out.append("missing_teacher")
    for key in teachers:
        _text(rec, key, out)


def _eval(rec: dict[str, Any], out: list[str]) -> None:
    _text_or_messages(rec, "input", out)
    _optional(rec, "reference", str, out)
    _optional(rec, "meta", dict, out)


def _mm_messages(rec: dict[str, Any], out: list[str]) -> None:
    if "messages" not in rec:
        out.append("missing_field")
    else:
        _messages(rec["messages"], out, parts=True)


RULES: dict[str, Callable[[dict[str, Any], list[str]], None]] = {
    "pretrain.text.v1": lambda rec, out: _text(rec, "text", out),
    "sft.messages.v1": _sft_messages,
    "sft.prompt_completion.v1": _prompt_completion,
    "pref.pairwise.v1": _pairwise,
    "pref.unpaired.v1": _unpaired,
    "rm.pairwise.v1": _pairwise,
    "rm.pointwise.v1": _pointwise,
    "rm.process.v1": _process,
    "rl.prompt.v1": _rl_prompt,
    "agent.multiturn.v1": _agent,
    "distill.v1": _distill,
    "eval.v1": _eval,
    "mm.messages.v1": _mm_messages,
}


def defects(schema: str, line: bytes) -> list[str]:
    """Defect codes for one JSONL line (empty list = valid)."""
    if len(line) > MAX_RECORD_BYTES:
        return ["oversized_record"]
    try:
        rec = json.loads(line.decode("utf-8"))
    except UnicodeDecodeError:
        return ["bad_utf8"]
    except json.JSONDecodeError:
        return ["invalid_json"]
    if not isinstance(rec, dict):
        return ["not_object"]
    out: list[str] = []
    RULES[schema](rec, out)
    return out


def _lines(name: str) -> list[bytes]:
    return (DATASETS / name).read_bytes().splitlines()


VALID_FILES = sorted(p.name for p in DATASETS.glob("*.jsonl") if "-invalid" not in p.name)


def test_every_schema_has_valid_and_invalid_file() -> None:
    assert sorted(f"{s}.jsonl" for s in RULES) == VALID_FILES
    assert sorted(MANIFEST["files"]) == sorted(f"{s}-invalid.jsonl" for s in RULES)


@pytest.mark.parametrize("name", VALID_FILES)
def test_valid_file_has_no_defects(name: str) -> None:
    schema = name.removesuffix(".jsonl")
    lines = _lines(name)
    assert lines
    assert [
        (n, defects(schema, line)) for n, line in enumerate(lines, 1) if defects(schema, line)
    ] == []


@pytest.mark.parametrize("name", sorted(MANIFEST["files"]))
def test_invalid_line_has_exactly_its_listed_defect(name: str) -> None:
    schema = name.removesuffix("-invalid.jsonl")
    listed = [(d["line"], [d["code"]]) for d in MANIFEST["files"][name]]
    assert [(n, defects(schema, line)) for n, line in enumerate(_lines(name), 1)] == listed


def test_invalid_twins_cover_required_defects() -> None:
    codes = {(name, d["code"]) for name, ds in MANIFEST["files"].items() for d in ds}
    for required in [
        ("sft.messages.v1-invalid.jsonl", "empty_field"),
        ("sft.messages.v1-invalid.jsonl", "role_order"),
        ("pref.pairwise.v1-invalid.jsonl", "chosen_equals_rejected"),
        ("pref.unpaired.v1-invalid.jsonl", "label_not_bool"),
        ("rl.prompt.v1-invalid.jsonl", "missing_field"),
        ("pretrain.text.v1-invalid.jsonl", "bad_utf8"),
        ("pretrain.text.v1-invalid.jsonl", "oversized_record"),
    ]:
        assert required in codes


def test_sft_pattern_and_held_out_eval_are_disjoint() -> None:
    train = [json.loads(line) for line in _lines("sft.messages.v1.jsonl")]
    held_out = [json.loads(line) for line in _lines("eval.v1.jsonl")]
    assert len(train) == 200
    assert len(held_out) == 50
    seen = {m["content"] for r in train for m in r["messages"] if m["role"] == "user"}
    for rec in held_out:
        assert rec["input"] == f"Repeat: {rec['reference']}"
        assert rec["input"] not in seen
    for rec in train:
        msgs = rec["messages"]
        for prev, msg in itertools.pairwise(msgs):
            if msg["role"] == "assistant":
                assert prev["content"] == f"Repeat: {msg['content']}"
