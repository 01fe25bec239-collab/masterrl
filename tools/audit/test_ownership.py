import os
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path

import ownership

README = """# Plan
## 5. Other
| P09 | `crates/**` |
## 6. Path ownership summary
| Part | Owns (top level) |
|------|------------------|
| P09 | `justfile`, `tools/{audit,traceability}/**`, `AGENTS.md, .gitattributes` |
| P10 | `apps/**` |
## 7. Next
"""

WAVE = """# Wave 1
## Lane 1 — P09 Example
**Owns.**
- `deploy/local/{compose.yaml,otel/}`
- `crates/{a,b}/{Cargo.toml,src/lib.rs|main.rs}`, `python/<pkg>/src/*.py`

- `not/owned/after/blank.md`
## Lane 2 — P10 Other
**Owns.** `secret/**`
"""


def temp_dir(case: unittest.TestCase) -> Path:
    tmp = tempfile.TemporaryDirectory()
    case.addCleanup(tmp.cleanup)
    return Path(tmp.name)


def git(root: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True)


def commit(root: Path, files: list[str], message: str) -> None:
    for rel in files:
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(message)
    git(root, "add", "-A")
    identity = ["-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"]
    git(root, *identity, "commit", "-qm", message)


class OwnershipTest(unittest.TestCase):
    def setUp(self):
        self.root = temp_dir(self)
        git(self.root, "init", "-q")
        commit(self.root, ["README.md"], "base")
        git(self.root, "tag", "wave-00-done")
        self.plan = temp_dir(self)
        (self.plan / "README.md").write_text(README)
        (self.plan / "wave-01.md").write_text(WAVE)
        self.cwd = Path.cwd()
        os.chdir(self.root)

    def tearDown(self):
        os.chdir(self.cwd)

    def run_tool(self, *args: str) -> tuple[int, str]:
        out = StringIO()
        with redirect_stdout(out), redirect_stderr(StringIO()):
            code = ownership.main([*args, "--plan", str(self.plan)])
        return code, out.getvalue()

    def test_owned_paths_pass(self):
        commit(
            self.root,
            [
                "justfile",
                "tools/audit/x.py",
                ".gitattributes",
                "deploy/local/compose.yaml",
                "deploy/local/otel/config.yaml",
                "crates/b/Cargo.toml",
                "crates/a/src/main.rs",
                "python/mrl-sdk/src/x.py",
            ],
            "owned",
        )
        self.assertEqual(self.run_tool("PART=P09")[0], 0)

    def test_unowned_edit_is_reported(self):
        commit(self.root, ["justfile", "apps/web/x.ts", "crates/a/src/other.rs"], "mixed")
        code, out = self.run_tool("PART=P09", "BASE=wave-00-done")
        self.assertEqual(code, 1)
        self.assertIn("apps/web/x.ts", out)
        self.assertIn("crates/a/src/other.rs", out)
        self.assertNotIn("justfile", out)

    def test_owns_block_stops_at_blank_line_and_lane_end(self):
        commit(self.root, ["not/owned/after/blank.md", "secret/x"], "bad")
        _, out = self.run_tool("P09")
        self.assertIn("not/owned/after/blank.md", out)
        self.assertIn("secret/x", out)

    def test_local_only_paths_are_ignored(self):
        commit(self.root, ["docs/implementation/x.md", "memory.md"], "local")
        self.assertEqual(self.run_tool("PART=P09")[0], 0)

    def test_generated_files_and_lockfiles_are_ignored(self):
        files = ["Cargo.lock", "python/adapters/a/uv.lock", "guidance/build/catalog.rs"]
        commit(self.root, files, "gen")
        self.assertEqual(self.run_tool("PART=P09")[0], 0)

    def test_a_manifest_change_to_lock_pins_only_counts_as_generated(self):
        pin = "lockSha256: " + "a" * 64
        self.assertTrue(ownership.pins_only(f"--- a/m\n+++ b/m\n@@ -1 +1 @@\n-  {pin}\n+  {pin}\n"))
        quoted = 'lockSha256: "' + "b" * 64 + '"'
        self.assertTrue(ownership.pins_only(f"@@ -1 +1 @@\n-  {pin}\n+  {quoted}\n"))
        self.assertFalse(ownership.pins_only(f"@@ -1 +1 @@\n-  {pin}\n+  name: x\n"))
        self.assertFalse(ownership.pins_only(""))

    def test_default_base_is_latest_wave_tag(self):
        commit(self.root, ["apps/x.ts"], "earlier")
        git(self.root, "tag", "wave-01-done")
        commit(self.root, ["justfile"], "later")
        code, out = self.run_tool("PART=P09")
        self.assertEqual(code, 0)
        self.assertIn("wave-01-done", out)

    def test_main_merged_into_a_lane_is_not_counted(self):
        commit(self.root, ["apps/x.ts"], "a1 change on main")
        git(self.root, "update-ref", "refs/remotes/origin/main", "HEAD")
        commit(self.root, ["justfile"], "lane change")
        code, out = self.run_tool("PART=P09")
        self.assertEqual(code, 0)
        self.assertIn("origin/main", out)

    def test_unknown_part_is_an_error(self):
        self.assertEqual(self.run_tool("PART=P42")[0], 2)
        with self.assertRaises(SystemExit):
            ownership.parse_args(["PART=nope"])

    def test_expansion(self):
        self.assertEqual(
            ownership.expand_braces("a/{b,c/{d,e}}.x"), ["a/b.x", "a/c/d.x", "a/c/e.x"]
        )
        self.assertEqual(ownership.expand_bars("s/lib.rs|main.rs"), ["s/lib.rs", "s/main.rs"])
        rx = ownership.to_regex("a/**/z.md")
        self.assertTrue(rx.fullmatch("a/z.md") and rx.fullmatch("a/b/c/z.md"))
        self.assertFalse(ownership.to_regex("a/*.md").fullmatch("a/b/c.md"))
        self.assertTrue(ownership.to_regex("deploy/").fullmatch("deploy/x/y"))
        self.assertFalse(ownership.to_regex("justfile").fullmatch("sub/justfile"))


if __name__ == "__main__":
    unittest.main()
