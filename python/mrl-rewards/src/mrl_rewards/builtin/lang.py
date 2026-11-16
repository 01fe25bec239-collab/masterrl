"""Language check by writing system (beta: no language-ID model yet)."""

from __future__ import annotations

import unicodedata
from collections import Counter
from typing import cast

from mrl_rewards.builtin._base import Component, capped, reference_field, schema
from mrl_rewards.types import ComponentSpec, JSONValue, RewardResult, RewardSample

# ISO 639-1 code -> scripts that count as that language (Unicode name prefixes).
_SCRIPTS: dict[str, frozenset[str]] = {
    **dict.fromkeys(
        [
            "af",
            "ca",
            "cs",
            "cy",
            "da",
            "de",
            "en",
            "es",
            "et",
            "eu",
            "fi",
            "fr",
            "ga",
            "gl",
            "hr",
            "hu",
            "id",
            "is",
            "it",
            "lt",
            "lv",
            "ms",
            "mt",
            "nl",
            "no",
            "nb",
            "nn",
            "pl",
            "pt",
            "ro",
            "sk",
            "sl",
            "sq",
            "sv",
            "sw",
            "tl",
            "tr",
            "uz",
            "vi",
            "yo",
            "zu",
        ],
        frozenset({"LATIN"}),
    ),
    **dict.fromkeys(
        ["be", "bg", "kk", "ky", "mk", "mn", "ru", "sr", "tg", "uk"], frozenset({"CYRILLIC"})
    ),
    **dict.fromkeys(["ar", "fa", "ps", "ur"], frozenset({"ARABIC"})),
    **dict.fromkeys(["hi", "mr", "ne", "sa"], frozenset({"DEVANAGARI"})),
    "bn": frozenset({"BENGALI"}),
    "el": frozenset({"GREEK"}),
    "he": frozenset({"HEBREW"}),
    "hy": frozenset({"ARMENIAN"}),
    "ka": frozenset({"GEORGIAN"}),
    "gu": frozenset({"GUJARATI"}),
    "pa": frozenset({"GURMUKHI"}),
    "ta": frozenset({"TAMIL"}),
    "te": frozenset({"TELUGU"}),
    "kn": frozenset({"KANNADA"}),
    "ml": frozenset({"MALAYALAM"}),
    "th": frozenset({"THAI"}),
    "ko": frozenset({"HANGUL"}),
    "zh": frozenset({"CJK"}),
    "ja": frozenset({"HIRAGANA", "KATAKANA", "CJK"}),
}
_KANA = frozenset({"HIRAGANA", "KATAKANA"})


def dominant_scripts(text: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for ch in text:
        if ch.isalpha():
            counts[unicodedata.name(ch, "UNKNOWN").split(" ", 1)[0]] += 1
    return counts


class LangMatch(Component):
    """1 when most letters of the completion are in the writing system of the expected
    language (`expected` param, else the reference's `lang`), else 0. Beta: it tells scripts
    apart (Latin, Cyrillic, Han, …), not languages that share one (English vs French)."""

    spec = ComponentSpec(
        name="lang.match",
        version="1.0.0",
        value_range=(0.0, 1.0),
        deterministic=True,
        needs=("reference",),
        beta=True,
        description=__doc__ or "",
        param_schema=schema(
            {
                "expected": {"enum": cast("list[JSONValue]", sorted(_SCRIPTS))},
                "min_share": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "maximum": 1,
                    "default": 0.5,
                },
            }
        ),
    )

    def configure(self) -> None:
        self.expected = self.params.get("expected")
        self.min_share = self.p_num("min_share", 0.5)

    def score_one(self, sample: RewardSample) -> RewardResult:
        lang = self.expected or reference_field(sample.reference, "lang")
        if not isinstance(lang, str) or lang.lower() not in _SCRIPTS:
            return RewardResult.invalid_input(f"unknown or missing language code {lang!r}")
        want = _SCRIPTS[lang.lower()]
        text, flags = capped(sample.completion)
        counts = dominant_scripts(text)
        total = sum(counts.values())
        if not total:
            return RewardResult.ok(0.0, {"letters": 0}, *flags, "no_letters")
        share = sum(n for s, n in counts.items() if s in want) / total
        ok = share >= self.min_share
        if lang.lower() == "ja":
            ok = ok and any(counts[k] for k in _KANA)
        top = counts.most_common(1)[0][0]
        return RewardResult.ok(float(ok), {"share": round(share, 6), "top_script": top}, *flags)


__all__ = ["LangMatch", "dominant_scripts"]
