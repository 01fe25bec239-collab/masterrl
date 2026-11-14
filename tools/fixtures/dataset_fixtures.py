"""Dataset fixture records for every canonical schema.

Each schema gets a small valid file and an `-invalid` twin. Every invalid line carries
exactly one defect, listed in DEFECTS.json. Lines are raw bytes so the twins can hold
bad UTF-8 and records over the size cap used in tests.
"""

import json
import random
from typing import Any

# Test cap for the "oversized record" defect. Production readers default to 16 MiB;
# the cap is configurable, so tests use a small one to keep fixtures small.
MAX_RECORD_BYTES = 4096

SEED = 20261114
WORDS = [
    "apple",
    "river",
    "stone",
    "cloud",
    "green",
    "paper",
    "lamp",
    "seven",
    "orange",
    "window",
    "tiger",
    "bread",
    "music",
    "silver",
    "forest",
    "candle",
    "rocket",
    "yellow",
    "garden",
    "mirror",
    "planet",
    "violin",
    "winter",
    "basket",
    "coffee",
    "dragon",
    "island",
    "jacket",
    "kettle",
    "lemon",
    "marble",
    "needle",
    "ocean",
    "pencil",
    "quartz",
    "rabbit",
    "saddle",
    "tunnel",
    "velvet",
    "walnut",
]
SYSTEM_PROMPT = "You repeat the user's words exactly."
TRAIN_RECORDS = 200
EVAL_RECORDS = 50

type Line = bytes | dict[str, Any]
type Defect = tuple[Line, str, str]  # (line, defect code, note)


def encode(record: dict[str, Any]) -> bytes:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode()


def _triples() -> list[tuple[str, str, str]]:
    rng = random.Random(SEED)
    picked: set[tuple[str, str, str]] = set()
    out: list[tuple[str, str, str]] = []
    while len(out) < TRAIN_RECORDS + EVAL_RECORDS:
        a, b, c = rng.sample(WORDS, 3)
        if (a, b, c) not in picked:
            picked.add((a, b, c))
            out.append((a, b, c))
    return out


def _turn(words: tuple[str, str, str]) -> list[dict[str, str]]:
    text = " ".join(words)
    return [
        {"role": "user", "content": f"Repeat: {text}"},
        {"role": "assistant", "content": text},
    ]


def sft_train() -> list[dict[str, Any]]:
    """200 'repeat my words' chats. Some have a system prompt, some two turns."""
    triples = _triples()[:TRAIN_RECORDS]
    records: list[dict[str, Any]] = []
    for i, words in enumerate(triples):
        messages: list[dict[str, str]] = []
        if i % 4 == 0:
            messages.append({"role": "system", "content": SYSTEM_PROMPT})
        messages += _turn(words)
        if i % 5 == 0:
            messages += _turn(triples[(i + 1) % TRAIN_RECORDS])
        records.append({"messages": messages})
    return records


def eval_held_out() -> list[dict[str, Any]]:
    """50 held-out prompts with the same pattern; no triple appears in training."""
    return [
        {
            "input": f"Repeat: {' '.join(words)}",
            "reference": " ".join(words),
            "meta": {"id": f"repeat-{i:03d}", "task": "repeat"},
        }
        for i, words in enumerate(_triples()[TRAIN_RECORDS:])
    ]


def _msgs(*pairs: tuple[str, str]) -> list[dict[str, str]]:
    return [{"role": r, "content": c} for r, c in pairs]


U, A = "user", "assistant"
HASH = "sha256:" + "ab" * 32


def _part_msg(role: str, *parts: dict[str, str]) -> dict[str, Any]:
    return {"role": role, "content": list(parts)}


def _ok() -> dict[str, Any]:
    return _part_msg(A, {"type": "text", "text": "Ok."})


VALID: dict[str, list[dict[str, Any]]] = {
    "pretrain.text.v1": [
        {"text": "Rivers carry stone and sand to the ocean."},
        {"text": "A green lamp stood by the window."},
        {"text": "Snow fell on the silver forest all winter. ❄"},
    ],
    "sft.prompt_completion.v1": [
        {"prompt": "Repeat: apple river stone", "completion": "apple river stone"},
        {
            "prompt": _msgs((U, "Repeat: lamp seven tiger")),
            "completion": _msgs((A, "lamp seven tiger")),
        },
    ],
    "pref.pairwise.v1": [
        {"prompt": "Repeat: cloud paper lamp", "chosen": "cloud paper lamp", "rejected": "cloud"},
        {
            "prompt": "Repeat: tiger bread music",
            "chosen": "tiger bread music",
            "rejected": "music bread tiger",
            "margin": 1.5,
            "annotator_ids": ["ann-1", "ann-2"],
        },
    ],
    "pref.unpaired.v1": [
        {"prompt": "Repeat: rocket garden", "completion": "rocket garden", "label": True},
        {"prompt": "Repeat: rocket garden", "completion": "garden", "label": False},
    ],
    "rm.pairwise.v1": [
        {"prompt": "Repeat: ocean pencil", "chosen": "ocean pencil", "rejected": "pencil ocean"},
    ],
    "rm.pointwise.v1": [
        {"prompt": "Repeat: velvet walnut", "completion": "velvet walnut", "score": 1.0},
        {"prompt": "Repeat: velvet walnut", "completion": "walnut", "score": -0.5},
        {"prompt": "Repeat: velvet walnut", "completion": "velvet", "score": 0},
    ],
    "rm.process.v1": [
        {
            "prompt": "What is 2 + 3 * 4?",
            "completions": ["3 * 4 = 12", "2 + 12 = 14", "The answer is 14."],
            "labels": [True, True, True],
        },
        {
            "prompt": "What is 2 + 3 * 4?",
            "completions": ["2 + 3 = 5", "5 * 4 = 20"],
            "labels": [False, False],
        },
    ],
    "rl.prompt.v1": [
        {"prompt": "Repeat: marble needle"},
        {
            "prompt": _msgs((U, "Repeat: quartz rabbit")),
            "reference": "quartz rabbit",
            "verifier": "exact_match",
            "meta": {"split": "train"},
        },
    ],
    "agent.multiturn.v1": [
        {
            "episode_id": "ep-0001",
            "env_spec_ref": "env/calculator@1",
            "turns": [
                {"role": "user", "content": "What is 6 times 7?"},
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{"name": "multiply", "arguments": {"a": 6, "b": 7}}],
                },
                {
                    "role": "tool",
                    "content": "",
                    "tool_results": [{"name": "multiply", "output": 42}],
                },
                {"role": "assistant", "content": "6 times 7 is 42."},
            ],
        },
    ],
    "distill.v1": [
        {"prompt": "Repeat: saddle tunnel", "teacher_completion": "saddle tunnel"},
        {"prompt": "Repeat: kettle lemon", "teacher_logits_ref": HASH},
        {
            "prompt": "Repeat: jacket island",
            "teacher_completion": "jacket island",
            "teacher_logits_ref": HASH,
        },
    ],
    "mm.messages.v1": [
        {
            "messages": [
                {
                    "role": U,
                    "content": [
                        {"type": "image_ref", "image_ref": HASH},
                        {"type": "text", "text": "What color is this?"},
                    ],
                },
                {"role": A, "content": [{"type": "text", "text": "It is yellow."}]},
            ]
        },
    ],
}

INVALID: dict[str, list[Defect]] = {
    "pretrain.text.v1": [
        ({"text": ""}, "empty_field", "text is empty"),
        ({"body": "no text field"}, "missing_field", "text is missing"),
        ({"text": 42}, "wrong_type", "text is not a string"),
        (b'{"text":"caf\xe9"}', "bad_utf8", "Latin-1 byte 0xE9, not UTF-8"),
        (
            encode({"text": "stone " * (MAX_RECORD_BYTES // 6 + 1)}),
            "oversized_record",
            f"record is larger than {MAX_RECORD_BYTES} bytes",
        ),
        (b'{"text": "unterminated', "invalid_json", "line is not valid JSON"),
        (b'["text"]', "not_object", "record is not a JSON object"),
    ],
    "sft.messages.v1": [
        ({"messages": _msgs((U, "Hi"), (U, "Hi again"), (A, "Hello"))}, "role_order", "two user"),
        ({"messages": _msgs((U, "Repeat: apple"))}, "role_order", "ends with user"),
        ({"messages": _msgs((A, "Hello"), (U, "Hi"), (A, "Hi"))}, "role_order", "starts assistant"),
        ({"messages": _msgs((U, "Repeat: lamp"), (A, ""))}, "empty_field", "empty content"),
        ({"messages": []}, "empty_field", "no messages"),
        ({"messages": _msgs((U, "Hi"), ("robot", "Hi"))}, "unknown_role", "role robot"),
        ({"text": "no messages"}, "missing_field", "messages is missing"),
        (b'{"messages":[{"role":"user","content":"\xff"}]}', "bad_utf8", "byte 0xFF"),
    ],
    "sft.prompt_completion.v1": [
        ({"prompt": "", "completion": "apple"}, "empty_field", "prompt is empty"),
        ({"prompt": "Repeat: apple"}, "missing_field", "completion is missing"),
        ({"prompt": "Repeat: apple", "completion": 7}, "wrong_type", "completion is a number"),
    ],
    "pref.pairwise.v1": [
        ({"prompt": "Repeat: a", "chosen": "a", "rejected": "a"}, "chosen_equals_rejected", ""),
        ({"prompt": "Repeat: a", "chosen": "a"}, "missing_field", "rejected is missing"),
        ({"prompt": "", "chosen": "a", "rejected": "b"}, "empty_field", "prompt is empty"),
        (
            {"prompt": "Repeat: a", "chosen": "a", "rejected": "b", "margin": "high"},
            "wrong_type",
            "margin is not a number",
        ),
    ],
    "pref.unpaired.v1": [
        ({"prompt": "Repeat: a", "completion": "a", "label": "true"}, "label_not_bool", "string"),
        ({"prompt": "Repeat: a", "completion": "a", "label": 1}, "label_not_bool", "integer 1"),
        ({"prompt": "Repeat: a", "completion": "a"}, "missing_field", "label is missing"),
    ],
    "rm.pairwise.v1": [
        ({"prompt": "Repeat: b", "chosen": "b", "rejected": "b"}, "chosen_equals_rejected", ""),
        ({"prompt": "Repeat: b", "rejected": "c"}, "missing_field", "chosen is missing"),
    ],
    "rm.pointwise.v1": [
        ({"prompt": "Repeat: a", "completion": "a", "score": "1.0"}, "wrong_type", "string score"),
        ({"prompt": "Repeat: a", "completion": "a", "score": True}, "wrong_type", "bool score"),
        ({"prompt": "Repeat: a", "completion": "a"}, "missing_field", "score is missing"),
    ],
    "rm.process.v1": [
        (
            {"prompt": "2 + 2?", "completions": ["2 + 2 = 4", "So 4."], "labels": [True]},
            "length_mismatch",
            "2 steps, 1 label",
        ),
        ({"prompt": "2 + 2?", "completions": [], "labels": []}, "empty_field", "no steps"),
        (
            {"prompt": "2 + 2?", "completions": ["2 + 2 = 4"], "labels": ["good"]},
            "wrong_type",
            "label is not a bool",
        ),
    ],
    "rl.prompt.v1": [
        ({"reference": "apple"}, "missing_field", "prompt is missing"),
        ({"prompt": ""}, "empty_field", "prompt is empty"),
        ({"prompt": "Repeat: a", "meta": "x"}, "wrong_type", "meta is not an object"),
    ],
    "agent.multiturn.v1": [
        (
            {"env_spec_ref": "env/calculator@1", "turns": _msgs((U, "Hi"))},
            "missing_field",
            "episode_id is missing",
        ),
        (
            {"episode_id": "ep-1", "env_spec_ref": "env/calculator@1", "turns": []},
            "empty_field",
            "no turns",
        ),
        ({"episode_id": "ep-1", "turns": _msgs((U, "Hi"))}, "missing_field", "no env_spec_ref"),
        (
            {"episode_id": "ep-1", "env_spec_ref": "env/c@1", "turns": _msgs(("robot", "Hi"))},
            "unknown_role",
            "role robot",
        ),
    ],
    "distill.v1": [
        ({"prompt": "Repeat: a"}, "missing_teacher", "no teacher_completion or teacher_logits_ref"),
        ({"teacher_completion": "a"}, "missing_field", "prompt is missing"),
        ({"prompt": "Repeat: a", "teacher_completion": ""}, "empty_field", "empty completion"),
    ],
    "eval.v1": [
        ({"reference": "apple"}, "missing_field", "input is missing"),
        ({"input": ""}, "empty_field", "input is empty"),
        ({"input": "Repeat: a", "reference": 3}, "wrong_type", "reference is not a string"),
    ],
    "mm.messages.v1": [
        (
            {"messages": [_part_msg(U, {"type": "audio", "audio_ref": HASH}), _ok()]},
            "unknown_part_type",
            "audio part",
        ),
        (
            {"messages": [_part_msg(U, {"type": "image_ref", "image_ref": ""}), _ok()]},
            "empty_field",
            "empty image_ref",
        ),
        (
            {"messages": [_ok(), _part_msg(U, {"type": "text", "text": "Hi"})]},
            "role_order",
            "starts with assistant, ends with user",
        ),
    ],
}


def all_files() -> dict[str, list[bytes]]:
    """File name -> JSONL lines (without newlines)."""
    files: dict[str, list[bytes]] = {
        "sft.messages.v1.jsonl": [encode(r) for r in sft_train()],
        "eval.v1.jsonl": [encode(r) for r in eval_held_out()],
    }
    for schema, records in VALID.items():
        files[f"{schema}.jsonl"] = [encode(r) for r in records]
    for schema, defects in INVALID.items():
        files[f"{schema}-invalid.jsonl"] = [_raw(line) for line, _, _ in defects]
    return files


def defects_manifest() -> dict[str, Any]:
    return {
        "max_record_bytes": MAX_RECORD_BYTES,
        "files": {
            f"{schema}-invalid.jsonl": [
                {"line": n, "code": code, "note": note}
                for n, (_, code, note) in enumerate(defects, start=1)
            ]
            for schema, defects in INVALID.items()
        },
    }


def _raw(line: Line) -> bytes:
    return line if isinstance(line, bytes) else encode(line)
