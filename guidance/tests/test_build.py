"""Tests for guidance/tools/build.py: planted bad fragments must fail the build.

Needs uv on PATH (the build runs with `uv run --script`).
"""

import json
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

GUIDANCE = Path(__file__).resolve().parents[1]
BUILD = GUIDANCE / "tools" / "build.py"


def entry(code: str, title: str, why: str = "This is a test entry.") -> str:
    return textwrap.dedent(f"""\
        errors:
          - code: {code}
            title: "{title}"
            why: "{why}"
            what_to_do:
              steps: [Try again.]
        """)


class BuildTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.root = self.tmp / "guidance"
        for part in ("schemas", "data", "fragments", "build"):
            shutil.copytree(GUIDANCE / part, self.root / part)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_build(self, *args: str) -> subprocess.CompletedProcess:
        uv = shutil.which("uv")
        self.assertIsNotNone(uv, "uv must be installed to run the build tests")
        cmd = [uv, "run", "--quiet", "--script", str(BUILD), "--root", str(self.root), *args]
        return subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    def plant(self, folder: str, text: str, name: str = "p99.yaml") -> None:
        (self.root / "fragments" / folder / name).write_text(text, encoding="utf-8")

    def assert_fails(self, message: str) -> None:
        result = self.run_build("--check")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(message, result.stderr)

    def test_committed_build_is_current(self):
        result = self.run_build("--check")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_long_sentence_fails(self):
        long = " ".join(["word"] * 30) + "."
        self.plant("errors", entry("MRL-TEST-001", "Something happened.", long))
        self.assert_fails("sentence has 30 words (max 20)")

    def test_duplicate_code_fails(self):
        self.plant("errors", entry("OOM", "Memory ran out."))
        self.assert_fails("duplicate error code 'OOM'")

    def test_bare_invalid_fails(self):
        self.plant("errors", entry("MRL-TEST-001", "Invalid value."))
        self.assert_fails('bare "invalid"')

    def test_blame_fails(self):
        self.plant("errors", entry("MRL-TEST-001", "You forgot the dataset."))
        self.assert_fails("don't blame the user")

    def test_unknown_acronym_fails(self):
        self.plant("errors", entry("MRL-TEST-001", "The XYZ service stopped."))
        self.assert_fails("acronym 'XYZ' is not in the glossary")

    def test_missing_why_fails(self):
        self.plant("errors", "errors:\n  - code: MRL-TEST-001\n    title: Stopped.\n    what_to_do: {steps: [Retry.]}\n")
        self.assert_fails("'why' is a required property")

    def test_fragment_name_and_keys_are_checked(self):
        self.plant("errors", entry("MRL-TEST-001", "Stopped."), name="extra.yaml")
        self.assert_fails("fragment files must be named pNN.yaml")
        (self.root / "fragments" / "errors" / "extra.yaml").unlink()
        self.plant("requirements", "requirements: []\nplatforms: []\n")
        self.assert_fails("fragments can't set 'platforms'")

    def test_good_fragments_merge(self):
        self.plant("errors", entry("MRL-TEST-001", "The test service stopped."))
        self.plant("glossary", "terms:\n  - term: XYZ\n    plain: A made-up service used in this test.\n")
        stale = self.run_build("--check")
        self.assertIn("out of date", stale.stderr)
        result = self.run_build()
        self.assertEqual(result.returncode, 0, result.stderr)
        built = json.loads((self.root / "build" / "catalog.json").read_text(encoding="utf-8"))
        self.assertIn("MRL-TEST-001", {e["code"] for e in built["errors"]})
        self.assertIn("XYZ", {t["term"] for t in built["glossary"]})


if __name__ == "__main__":
    unittest.main()
