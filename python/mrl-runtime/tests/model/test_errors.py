"""Every code the P15 libraries raise is in the guidance catalog."""

import json
from pathlib import Path

from mrl_runtime.model import errors

CATALOG = Path(__file__).resolve().parents[4] / "guidance/build/catalog.json"


def test_codes_are_in_the_catalog() -> None:
    known = {e["code"] for e in json.loads(CATALOG.read_text())["errors"]}
    raised = {v for k, v in vars(errors).items() if k.isupper() and isinstance(v, str)}
    assert raised
    assert raised <= known, raised - known


def test_error_text() -> None:
    err = errors.MrlError(
        errors.MODEL_UNREADABLE, "The app could not read this model's files.", "x.json"
    )
    assert str(err) == "MRL-MODEL-150: The app could not read this model's files. (x.json)"
    assert (err.code, err.detail) == ("MRL-MODEL-150", "x.json")
