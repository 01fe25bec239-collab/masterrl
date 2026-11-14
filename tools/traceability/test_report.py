import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

import report

REGISTRY = """
- {id: FR-RUN-1, text: "Start a run | fast", release: R1}
- {id: FR-RUN-2, text: Stop a run, release: R1}
- {id: NFR-1, text: Correctness, release: null}
"""
REQ = "req" + ":"  # split so this file does not tag itself
MARK = "@pytest.mark." + "req"


def temp_dir(case: unittest.TestCase) -> Path:
    tmp = tempfile.TemporaryDirectory()
    case.addCleanup(tmp.cleanup)
    return Path(tmp.name)


def repo(case: unittest.TestCase, files: dict[str, str]) -> Path:
    root = temp_dir(case)
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    return root


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.registry = temp_dir(self) / "registry.yaml"
        self.registry.write_text(REGISTRY)

    def run_tool(self, root: Path) -> tuple[int, str]:
        out = StringIO()
        with redirect_stdout(out), redirect_stderr(StringIO()):
            code = report.main(["--registry", str(self.registry), str(root)])
        return code, out.getvalue()

    def test_collects_all_three_tag_styles(self):
        root = repo(
            self,
            {
                "tests/test_a.py": f'# {REQ} FR-RUN-1\n{MARK}("NFR-1", "FR-X-9")\n',
                "crates/x/src/lib.rs": f"fn a() {{}}\n// {REQ} FR-RUN-1, NFR-1\n",
            },
        )
        self.assertEqual(
            report.collect_tags(root),
            {
                "FR-RUN-1": ["crates/x/src/lib.rs:2", "tests/test_a.py:1"],
                "NFR-1": ["crates/x/src/lib.rs:2", "tests/test_a.py:2"],
                "FR-X-9": ["tests/test_a.py:2"],
            },
        )

    def test_report_lists_coverage_and_unknown_ids(self):
        root = repo(self, {"t.py": f"# {REQ} FR-RUN-1\n# {REQ} FR-NOPE-3\n"})
        code, out = self.run_tool(root)
        self.assertEqual(code, 0)
        self.assertIn("3 requirements · 1 covered · 2 uncovered", out)
        self.assertIn("| **FR-RUN-1** Start a run \\| fast | R1 | `t.py:1` |", out)
        self.assertIn("| **FR-RUN-2** Stop a run | R1 | — |", out)
        self.assertIn("| **NFR-1** Correctness | — | — |", out)
        self.assertIn("- FR-NOPE-3: `t.py:2`", out)

    def test_untagged_mentions_do_not_count(self):
        root = repo(self, {"t.py": "FR-RUN-1 is mentioned in prose only\n"})
        self.assertEqual(report.collect_tags(root), {})

    def test_tool_files_do_not_tag_themselves(self):
        here = Path(__file__).parent
        files = {f.name: f.read_text() for f in (here / "report.py", here / "test_report.py")}
        self.assertEqual(report.collect_tags(repo(self, files)), {})

    def test_missing_registry_exits_2(self):
        self.registry.unlink()
        self.assertEqual(self.run_tool(repo(self, {"a": "b"}))[0], 2)


if __name__ == "__main__":
    unittest.main()
