"""The low/expected/high range the estimator reports, and its units."""

from collections.abc import Iterable
from dataclasses import dataclass

GIB = float(2**30)  # memory is reported in GiB (Doc 07 §6.3-6.6)
GB = 1e9  # disk and storage are reported in GB (Doc 07 §6.7, §6.9)


@dataclass(frozen=True)
class Range:
    """An amount (GiB for memory, GB for storage) as low / expected / high."""

    low: float
    expected: float
    high: float

    @staticmethod
    def exact(value: float) -> "Range":
        return Range(value, value, value)

    @staticmethod
    def of_bytes(low: float, expected: float, high: float, unit: float = GIB) -> "Range":
        return Range(low / unit, expected / unit, high / unit)

    @staticmethod
    def exact_bytes(n: float, unit: float = GIB) -> "Range":
        return Range.exact(n / unit)

    def __add__(self, other: "Range") -> "Range":
        return Range(self.low + other.low, self.expected + other.expected, self.high + other.high)

    def scale(self, factor: float) -> "Range":
        return Range(self.low * factor, self.expected * factor, self.high * factor)

    def rounded(self, digits: int = 2) -> "Range":
        return Range(
            round(self.low, digits), round(self.expected, digits), round(self.high, digits)
        )


def total(ranges: Iterable[Range]) -> Range:
    out = Range.exact(0.0)
    for r in ranges:
        out = out + r
    return out
