import tomllib
from pathlib import Path

import mrl_adapter_lmeval


def test_installed_version_matches_pyproject() -> None:
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    expected = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert mrl_adapter_lmeval.__version__ == expected
