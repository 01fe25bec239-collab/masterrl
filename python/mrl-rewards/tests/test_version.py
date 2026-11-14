import tomllib
from pathlib import Path

import mrl_rewards


def test_installed_version_matches_pyproject() -> None:
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    expected = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert mrl_rewards.__version__ == expected
