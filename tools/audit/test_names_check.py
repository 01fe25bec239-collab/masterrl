import tempfile
import unittest
from pathlib import Path

import names_check

NAMES = """# Frozen names v1
## Tables
- `runs`
- `job_attempts`
## Agent commands
- `LaunchAttempt`
## Failure classes
- `OOM`
"""


def main_folder(plan: str) -> Path:
    root = Path(tempfile.mkdtemp())
    (root / "docs/architecture").mkdir(parents=True)
    (root / "docs/implementation").mkdir(parents=True)
    (root / "docs/architecture/names-frozen-v1.md").write_text(NAMES)
    (root / "docs/implementation/wave-01.md").write_text(plan)
    return root


class NamesCheckTest(unittest.TestCase):
    def test_frozen_names_pass(self):
        plan = "Writes the `runs` and `job_attempts` tables.\nSends the `LaunchAttempt` command.\n"
        self.assertEqual(names_check.main([str(main_folder(plan))]), 0)

    def test_unfrozen_table_and_command_are_flagged(self):
        plan = "Adds the `run_attempt` table.\nok line\nNew command `StartJob`.\n"
        problems = names_check.check(main_folder(plan))
        self.assertEqual(problems, [
            "docs/implementation/wave-01.md:1: table `run_attempt` is not frozen",
            "docs/implementation/wave-01.md:3: command `StartJob` is not frozen",
        ])
        self.assertEqual(names_check.main([str(main_folder(plan))]), 1)

    def test_names_without_table_or_command_context_are_ignored(self):
        plan = "The `data_prep` task uses `SecretStore`.\n"
        self.assertEqual(names_check.check(main_folder(plan)), [])


if __name__ == "__main__":
    unittest.main()
