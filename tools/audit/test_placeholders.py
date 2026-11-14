import subprocess
import tempfile
import unittest
from pathlib import Path

import placeholders

TODO = "todo" + "!()"


def repo(files: dict[str, str]) -> Path:
    root = Path(tempfile.mkdtemp())
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(text)
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    return root


class PlaceholdersTest(unittest.TestCase):
    def test_clean_repo_passes(self):
        self.assertEqual(placeholders.main([str(repo({"src/lib.rs": "fn main() {}\n"}))]), 0)

    def test_each_pattern_is_caught_with_file_and_line(self):
        for pattern in placeholders.PATTERNS:
            root = repo({"src/a.py": f"ok\nx = {pattern}\n"})
            self.assertEqual(placeholders.scan(root), [f"src/a.py:2: x = {pattern}"])
            self.assertEqual(placeholders.main([str(root)]), 1)

    def test_skips_tests_testkit_and_docs(self):
        root = repo({
            "tests/a.rs": TODO,
            "crates/x/tests/b.rs": TODO,
            "crates/x/testkit/c.rs": TODO,
            "crates/x/src/testkit/d.rs": TODO,
            "docs/e.md": TODO,
        })
        self.assertEqual(placeholders.scan(root), [])

    def test_untracked_and_binary_files_are_ignored(self):
        root = repo({"bin.dat": "\0" + TODO})
        (root / "new.rs").write_text(TODO)
        self.assertEqual(placeholders.scan(root), [])

    def test_does_not_flag_itself(self):
        source = Path(placeholders.__file__).read_text()
        self.assertFalse(any(p in source for p in placeholders.PATTERNS))


if __name__ == "__main__":
    unittest.main()
