#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml==6.0.2"]
# ///
"""Run the tests of every tool in tools/audit and tools/traceability."""

import sys
import unittest
from pathlib import Path

TOOLS = Path(__file__).resolve().parent.parent

if __name__ == "__main__":
    suite = unittest.TestSuite()
    for folder in ("audit", "traceability"):
        path = TOOLS / folder
        sys.path.insert(0, str(path))
        suite.addTests(unittest.defaultTestLoader.discover(str(path), top_level_dir=str(path)))
        sys.path.remove(str(path))
    result = unittest.TextTestRunner(verbosity=2, buffer=True).run(suite)
    sys.exit(0 if result.wasSuccessful() and result.testsRun else 1)
