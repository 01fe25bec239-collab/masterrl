"""Built-in reward components (namespace `builtin/`). All tier 1 and deterministic."""

from mrl_rewards.builtin._base import Component
from mrl_rewards.builtin.format import JsonSchema, RegexFull, ThinkAnswerTags
from mrl_rewards.builtin.lang import LangMatch
from mrl_rewards.builtin.length import PenaltyLinear, SoftOverlong, TargetBand
from mrl_rewards.builtin.math import MathVerify, NumericClose
from mrl_rewards.builtin.text import Contains, ExactMatch, Regex

BUILTINS: tuple[type[Component], ...] = (
    ExactMatch,
    Contains,
    Regex,
    ThinkAnswerTags,
    JsonSchema,
    RegexFull,
    MathVerify,
    NumericClose,
    PenaltyLinear,
    SoftOverlong,
    TargetBand,
    LangMatch,
)

__all__ = ["BUILTINS", "Component"]
