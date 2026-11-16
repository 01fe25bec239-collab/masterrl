"""Tokenization preview (Doc 08 §2.4 step 4): rendered chat template, token-length histogram,
share of records over the context length, and the assistant-only loss mask.

Chat templates come from the model folder and are untrusted: they render in Jinja's immutable
sandbox. `{% generation %}…{% endgeneration %}` blocks (the same tag transformers uses for
`return_assistant_tokens_mask`) mark the tokens that training learns from.
The `tokenizers` library is imported on first use.
"""

import json
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from jinja2 import TemplateError, nodes
from jinja2.ext import Extension, loopcontrols
from jinja2.parser import Parser
from jinja2.sandbox import ImmutableSandboxedEnvironment

from mrl_runtime.data.validate import LengthStats
from mrl_runtime.model._json import Json, arr, obj, text
from mrl_runtime.model.errors import NO_CHAT_TEMPLATE, TOKENIZER_UNUSABLE, MrlError
from mrl_runtime.model.inspection import (
    GENERATION_TAG,
    chat_templates,
    inspect_model,
    read_json,
)

if TYPE_CHECKING:
    from tokenizers import Tokenizer

# Private-use characters that bracket generation blocks while rendering.
START, END = "\ue000", "\ue001"
_MARKS = re.compile(f"([{START}{END}])")
type Spans = list[tuple[int, int]]


class _GenerationTags(Extension):
    """`{% generation %}…{% endgeneration %}`: wraps the block's output in START/END marks."""

    tags = {"generation"}  # noqa: RUF012 (jinja2's Extension API declares it this way)

    def parse(self, parser: Parser) -> nodes.Node:
        lineno = next(parser.stream).lineno
        body = parser.parse_statements(("name:endgeneration",), drop_needle=True)
        return nodes.CallBlock(self.call_method("_mark"), [], [], body).set_lineno(lineno)

    def _mark(self, caller: Callable[[], str]) -> str:
        return START + caller() + END


def _raise(message: str) -> None:
    raise TemplateError(message)


def _tojson(value: object, indent: int | None = None) -> str:
    return json.dumps(value, ensure_ascii=False, indent=indent)


def _strftime_now(fmt: str) -> str:
    return datetime.now().astimezone().strftime(fmt)


def _environment() -> ImmutableSandboxedEnvironment:
    env = ImmutableSandboxedEnvironment(
        trim_blocks=True, lstrip_blocks=True, extensions=[_GenerationTags, loopcontrols]
    )
    filters = cast(dict[str, Any], env.filters)  # pyright: ignore[reportUnknownMemberType]
    names = cast(dict[str, Any], env.globals)  # pyright: ignore[reportUnknownMemberType]
    filters["tojson"] = _tojson
    names["raise_exception"] = _raise
    names["strftime_now"] = _strftime_now
    return env


def _token_text(value: object) -> str | None:
    """A special token from tokenizer_config.json: a string or {"content": ...}."""
    return text(value) or text((obj(value) or {}).get("content"))


@dataclass(frozen=True)
class ChatTemplate:
    """A model's default chat template, compiled in the sandbox."""

    source: str
    special_tokens: dict[str, str]
    has_generation_tags: bool
    _render: Callable[..., str] = field(repr=False)

    @staticmethod
    def load(model_dir: Path) -> "ChatTemplate | None":
        config_path = model_dir / "tokenizer_config.json"
        config = read_json(config_path) if config_path.is_file() else {}
        source = chat_templates(model_dir, config).get("default")
        if source is None:
            return None
        try:
            template = _environment().from_string(source)
        except TemplateError as exc:
            raise MrlError(
                TOKENIZER_UNUSABLE,
                "The app could not use this model's tokenizer or chat template.",
                f"chat template: {exc}",
            ) from exc
        special = {
            k: t
            for k in ("bos_token", "eos_token", "pad_token", "unk_token")
            if (t := _token_text(config.get(k))) is not None
        }
        tags = GENERATION_TAG.search(source) is not None
        return ChatTemplate(source, special, tags, template.render)

    def render(
        self, messages: list[Any], *, add_generation_prompt: bool = False
    ) -> tuple[str, Spans | None]:
        """Rendered text and the character spans inside generation blocks (None: no tags)."""
        raw = json.dumps(messages, ensure_ascii=False)
        out = self._render(
            messages=messages, add_generation_prompt=add_generation_prompt, **self.special_tokens
        )
        if START in raw or END in raw:  # the data itself holds the marks: spans unknowable
            return out.replace(START, "").replace(END, ""), None
        plain: list[str] = []
        spans: Spans = []
        length, start = 0, None
        for part in _MARKS.split(out):
            if part == START:
                start = length
            elif part == END:
                if start is not None:
                    spans.append((start, length))
                start = None
            else:
                plain.append(part)
                length += len(part)
        return "".join(plain), spans if self.has_generation_tags else None


@dataclass(frozen=True)
class TokenSequence:
    """One token sequence a record turns into, with the characters training learns from."""

    text: str
    trained: Spans | None
    templated: bool


def _prompt_text(value: object, template: ChatTemplate | None) -> tuple[str, bool]:
    """(text, templated) for a prompt that is a string or a list of messages."""
    if isinstance(value, str):
        return value, False
    if template is None:
        raise MrlError(NO_CHAT_TEMPLATE, "This model has no chat template for conversation data.")
    rendered, _ = template.render(arr(value) or [], add_generation_prompt=True)
    return rendered, True


def _with_completion(
    prompt: object, completion: object, template: ChatTemplate | None
) -> TokenSequence:
    if isinstance(completion, str):
        head, templated = _prompt_text(prompt, template)
        return TokenSequence(
            head + completion, [(len(head), len(head) + len(completion))], templated
        )
    if template is None:
        raise MrlError(NO_CHAT_TEMPLATE, "This model has no chat template for conversation data.")
    messages = (arr(prompt) or [{"role": "user", "content": prompt}]) + (arr(completion) or [])
    full, tagged = template.render(messages)
    head, _ = _prompt_text(prompt, template)
    if full.startswith(head):
        return TokenSequence(full, [(len(head), len(full))], True)
    return TokenSequence(full, tagged, True)


def _conversation(messages: object, template: ChatTemplate | None) -> TokenSequence:
    if template is None:
        raise MrlError(NO_CHAT_TEMPLATE, "This model has no chat template for conversation data.")
    rendered, spans = template.render(arr(messages) or [])
    return TokenSequence(rendered, spans, True)


def _agent_messages(turns: object) -> list[Json]:
    out: list[Json] = []
    for t in arr(turns) or []:
        turn = obj(t) or {}
        msg: Json = {"role": turn.get("role"), "content": turn.get("content", "")}
        for key in ("tool_calls", "tool_call_id"):
            if key in turn:
                msg[key] = turn[key]
        out.append(msg)
    return out


def _pairwise(r: Json, t: ChatTemplate | None) -> list[TokenSequence]:
    return [_with_completion(r.get("prompt"), r.get(k), t) for k in ("chosen", "rejected")]


def _steps(r: Json, t: ChatTemplate | None) -> list[TokenSequence]:
    head, templated = _prompt_text(r.get("prompt"), t)
    steps = "\n".join(str(s) for s in arr(r.get("completions")) or [])
    return [TokenSequence(head + steps, None, templated)]


def _prompt_only(key: str) -> Callable[[Json, ChatTemplate | None], list[TokenSequence]]:
    def build(r: Json, t: ChatTemplate | None) -> list[TokenSequence]:
        body, templated = _prompt_text(r.get(key), t)
        return [TokenSequence(body, None, templated)]

    return build


def _pretrain(r: Json, _: ChatTemplate | None) -> list[TokenSequence]:
    body = str(r.get("text", ""))
    return [TokenSequence(body, [(0, len(body))], False)]


def _completion(key: str) -> Callable[[Json, ChatTemplate | None], list[TokenSequence]]:
    def build(r: Json, t: ChatTemplate | None) -> list[TokenSequence]:
        return [_with_completion(r.get("prompt"), r.get(key, ""), t)]

    return build


# How each schema turns into token sequences (what the trainer would tokenize).
BUILDERS: dict[str, Callable[[Json, ChatTemplate | None], list[TokenSequence]]] = {
    "pretrain.text.v1": _pretrain,
    "sft.messages.v1": lambda r, t: [_conversation(r.get("messages"), t)],
    "mm.messages.v1": lambda r, t: [_conversation(r.get("messages"), t)],
    "agent.multiturn.v1": lambda r, t: [_conversation(_agent_messages(r.get("turns")), t)],
    "sft.prompt_completion.v1": _completion("completion"),
    "pref.unpaired.v1": _completion("completion"),
    "rm.pointwise.v1": _completion("completion"),
    "rl.rollout.v1": _completion("completion"),
    "distill.v1": _completion("teacher_completion"),
    "pref.pairwise.v1": _pairwise,
    "rm.pairwise.v1": _pairwise,
    "rm.process.v1": _steps,
    "rl.prompt.v1": _prompt_only("prompt"),
    "eval.v1": _prompt_only("input"),
}


def sequences(schema_id: str, record: Json, template: ChatTemplate | None) -> list[TokenSequence]:
    """The token sequences training builds from one record of a canonical schema."""
    return BUILDERS[schema_id](record, template)


@dataclass(frozen=True)
class Segment:
    text: str
    trained: bool


@dataclass(frozen=True)
class PreviewSample:
    record: int
    text: str
    tokens: int
    trained_tokens: int | None
    over_context: bool
    segments: tuple[Segment, ...]


@dataclass(frozen=True)
class Bin:
    low: int
    high: int | None  # None: everything above `low` (over the context length)
    count: int


@dataclass(frozen=True)
class PreviewOptions:
    context_length: int | None = None  # default: the model's context length
    samples: int = 3
    bins: int = 10
    max_records: int | None = None


@dataclass(frozen=True)
class TokenPreview:
    """Token statistics for a dataset against one model's tokenizer and chat template."""

    schema: str
    records: int
    sequences: int
    failed: int
    failures: tuple[tuple[int, str], ...]
    context_length: int | None
    lengths: LengthStats | None
    histogram: tuple[Bin, ...]
    over_context: int
    over_context_pct: float
    mask_supported: bool
    trained_token_pct: float | None
    chat_template_sha256: str | None
    samples: tuple[PreviewSample, ...]


def load_tokenizer(model_dir: Path) -> "Tokenizer":
    path = model_dir / "tokenizer.json"
    if not path.is_file():
        raise MrlError(
            TOKENIZER_UNUSABLE,
            "The app could not use this model's tokenizer or chat template.",
            "tokenizer.json is missing",
        )
    from tokenizers import Tokenizer  # noqa: PLC0415 (lazy: native library)

    try:
        return Tokenizer.from_file(str(path))
    except Exception as exc:  # tokenizers raises a plain Exception for bad files
        raise MrlError(
            TOKENIZER_UNUSABLE,
            "The app could not use this model's tokenizer or chat template.",
            f"tokenizer.json: {exc}",
        ) from exc


def _mask(offsets: list[tuple[int, int]], spans: Spans) -> list[bool]:
    return [any(s <= start < e for s, e in spans) for start, _ in offsets]


def _segments(body: str, offsets: list[tuple[int, int]], mask: list[bool]) -> tuple[Segment, ...]:
    out: list[Segment] = []
    i = 0
    while i < len(offsets):
        j = i
        while j + 1 < len(offsets) and mask[j + 1] == mask[i]:
            j += 1
        end = offsets[j + 1][0] if j + 1 < len(offsets) else len(body)
        out.append(Segment(body[offsets[i][0] : end], mask[i]))
        i = j + 1
    return tuple(out)


def _histogram(lengths: list[int], context: int | None, bins: int) -> tuple[Bin, ...]:
    if not lengths:
        return ()
    top = context or max(lengths)
    width = max(1, -(-top // bins))
    counts = [0] * bins
    over = 0
    for n in lengths:
        if context is not None and n > context:
            over += 1
        else:
            counts[min(n // width, bins - 1)] += 1
    out = [Bin(i * width, min((i + 1) * width, top), c) for i, c in enumerate(counts)]
    if context is not None:
        out.append(Bin(context, None, over))
    return tuple(out)


def preview_tokens(
    model_dir: Path,
    schema_id: str,
    records: Iterable[object],
    options: PreviewOptions | None = None,
) -> TokenPreview:
    """Tokenize records (parsed, schema-valid) with the model's tokenizer and chat template."""
    opts = options or PreviewOptions()
    tokenizer = load_tokenizer(model_dir)
    template = ChatTemplate.load(model_dir)
    model = inspect_model(model_dir)
    context = opts.context_length or model.context_length
    lengths: list[int] = []
    failures: list[tuple[int, str]] = []
    samples: list[PreviewSample] = []
    trained_total = token_total = 0
    count = 0
    for number, value in enumerate(records, 1):
        if opts.max_records is not None and number > opts.max_records:
            break
        count = number
        try:
            seqs = sequences(schema_id, obj(value) or {}, template)
        except MrlError:
            raise
        except Exception as exc:  # untrusted template code can raise any Python error
            failures.append((number, f"The chat template failed on this record: {exc}"))
            continue
        for seq in seqs:
            enc = tokenizer.encode(seq.text, add_special_tokens=not seq.templated)
            offsets: list[tuple[int, int]] = list(enc.offsets)
            mask = _mask(offsets, seq.trained) if seq.trained is not None else None
            lengths.append(len(enc.ids))
            if mask is not None:
                trained_total += sum(mask)
                token_total += len(mask)
            if len(samples) < opts.samples:
                samples.append(
                    PreviewSample(
                        number,
                        seq.text,
                        len(enc.ids),
                        sum(mask) if mask is not None else None,
                        context is not None and len(enc.ids) > context,
                        _segments(seq.text, offsets, mask or [False] * len(offsets)),
                    )
                )
    over = sum(1 for n in lengths if context is not None and n > context)
    return TokenPreview(
        schema=schema_id,
        records=count,
        sequences=len(lengths),
        failed=len(failures),
        failures=tuple(failures[:20]),
        context_length=context,
        lengths=LengthStats.of(lengths),
        histogram=_histogram(lengths, context, opts.bins),
        over_context=over,
        over_context_pct=round(100 * over / len(lengths), 2) if lengths else 0.0,
        mask_supported=template is not None and template.has_generation_tags,
        trained_token_pct=round(100 * trained_total / token_total, 2) if token_total else None,
        chat_template_sha256=model.chat_template_sha256 if template else None,
        samples=tuple(samples),
    )
