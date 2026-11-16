"""Remove secrets from text before it leaves the worker: known values first, then patterns."""

import os
import re
from collections.abc import Iterable, Mapping

MASK = "[REDACTED]"
MIN_SECRET_LEN = 8
_SECRET_ENV = re.compile(
    r"TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_KEY|PRIVATE_KEY|CREDENTIAL|NONCE"
)

# Group 1 is kept (a label like "password="), the rest of the match is replaced.
PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        r"()-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)"
    ),
    re.compile(r"()\bAKIA[0-9A-Z]{16}\b"),  # AWS access key id
    re.compile(r"()\bgh[pousr]_[A-Za-z0-9]{36,}\b"),  # GitHub token
    re.compile(r"()\bgithub_pat_[A-Za-z0-9_]{40,}\b"),
    re.compile(r"()\bhf_[A-Za-z0-9]{30,}\b"),  # Hugging Face token
    re.compile(r"()\bsk-[A-Za-z0-9_-]{20,}"),  # OpenAI / Anthropic style API key
    re.compile(r"()\bxox[abprs]-[A-Za-z0-9-]{10,}"),  # Slack token
    re.compile(r"()\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),  # JWT
    re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/=-]{16,}"),
    re.compile(
        r"(?i)(\b(?:password|passwd|secret|token|api[_-]?key|access[_-]?key)[\"']?\s*[=:]\s*[\"']?)"
        r"[^\s\"',;]{6,}"
    ),
)


def secret_values_from_env(env: Mapping[str, str]) -> list[str]:
    return [v for k, v in env.items() if _SECRET_ENV.search(k.upper()) and len(v) >= MIN_SECRET_LEN]


Span = tuple[int, int, int]  # (start, mask_from, end): text[start:mask_from] is a kept label


class Redactor:
    def __init__(self, secrets: Iterable[str] = (), *, from_env: bool = True) -> None:
        values = list(secrets) + (secret_values_from_env(os.environ) if from_env else [])
        self._values = sorted(
            {v for v in values if len(v) >= MIN_SECRET_LEN}, key=len, reverse=True
        )

    @property
    def longest(self) -> int:
        """Length of the longest known secret value (0 if none)."""
        return len(self._values[0]) if self._values else 0

    def add(self, secret: str) -> None:
        if len(secret) >= MIN_SECRET_LEN and secret not in self._values:
            self._values = sorted([*self._values, secret], key=len, reverse=True)

    def spans(self, text: str) -> list[Span]:
        """Every secret in `text`, sorted and merged so no two spans overlap."""
        found: list[Span] = []
        for value in self._values:
            i = text.find(value)
            while i >= 0:
                found.append((i, i, i + len(value)))
                i = text.find(value, i + len(value))
        for pattern in PATTERNS:
            found.extend((m.start(), m.end(1), m.end()) for m in pattern.finditer(text))
        merged: list[Span] = []
        for start, mask_from, end in sorted(found):
            if merged and start < merged[-1][2]:
                s0, m0, e0 = merged[-1]
                merged[-1] = (s0, min(m0, mask_from), max(e0, end))
            else:
                merged.append((start, mask_from, end))
        return merged

    @staticmethod
    def mask(text: str, spans: Iterable[Span]) -> str:
        out: list[str] = []
        pos = 0
        for _, mask_from, end in spans:
            out.append(text[pos:mask_from])
            out.append(MASK)
            pos = end
        out.append(text[pos:])
        return "".join(out)

    def text(self, text: str) -> str:
        return self.mask(text, self.spans(text))

    def bytes(self, data: bytes) -> bytes:
        """Byte-preserving: undecodable bytes pass through unchanged."""
        return self.text(data.decode("utf-8", "surrogateescape")).encode("utf-8", "surrogateescape")
