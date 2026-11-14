"""Tests for the generated Python loader. Run: python3 -m unittest discover guidance/tests"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))

import catalog  # noqa: E402

FAILURE_CLASSES = [
    "USER_CONFIG", "DATA", "OOM", "NODE_LOST", "HARDWARE", "INFRA_TRANSIENT", "ENGINE_ERROR",
    "SANDBOX_VIOLATION", "PREEMPTED", "CANCELLED_BY_USER", "VERIFICATION_FAILED", "REWARD_INFRA",
    "DEPENDENCY_ENV", "MODEL_INCOMPATIBLE", "NUMERIC_DIVERGENCE", "STALLED", "HARNESS_ERROR",
    "SAFETY_HALT",
]
SUB_CODES = [
    "OOM/OOM_GPU", "OOM/OOM_HOST", "OOM/SHM_EXHAUSTED", "INFRA_TRANSIENT/DISK_FULL",
    "HARDWARE/GPU_CONTENDED", "NODE_LOST/NODE_REBOOTED",
]
MRL_CODES = (
    [f"MRL-VAL-{i:03}" for i in range(1, 4)]
    + [f"MRL-IDEM-{i:03}" for i in range(1, 3)]
    + [f"MRL-DATA-{i:03}" for i in range(1, 3)]
    + [f"MRL-MODEL-{i:03}" for i in range(1, 4)]
    + [f"MRL-CAP-{i:03}" for i in range(1, 24)]
    + [f"MRL-ENV-{i:03}" for i in range(1, 13)]
    + [f"MRL-LINK-{i:03}" for i in range(1, 7)]
)
REQUIRED_CODES = FAILURE_CLASSES + SUB_CODES + MRL_CODES


class CatalogTest(unittest.TestCase):
    def test_required_codes_exist(self):
        missing = [code for code in REQUIRED_CODES if catalog.get_error(code) is None]
        self.assertEqual(missing, [])

    def test_every_error_renders(self):
        for entry in catalog.CATALOG["errors"]:
            with self.subTest(code=entry["code"]):
                shown = catalog.explain_error(entry["code"])
                self.assertIs(shown, entry)
                self.assertTrue(shown["title"] and shown["why"] and shown["what_to_do"]["steps"])

    def test_unknown_code_falls_back(self):
        self.assertIsNone(catalog.get_error("NOT_A_CODE"))
        self.assertIs(catalog.explain_error("NOT_A_CODE"), catalog.CATALOG["error_fallback"])

    def test_counts(self):
        self.assertEqual(len(catalog.CATALOG["goals"]), 6)
        self.assertGreaterEqual(len(catalog.CATALOG["glossary"]), 60)
        recipe_ids = {r["id"] for r in catalog.CATALOG["recipes"]}
        self.assertEqual(
            recipe_ids, {"sft-lora", "sft-qlora", "sft-full", "dpo", "grpo-verifiable", "distillation"}
        )

    def test_lookups(self):
        self.assertEqual(catalog.get_term("reinforcement learning")["term"], "RL")
        self.assertEqual(catalog.get_goal("checkable-answers")["reward_type"], "verifiable")
        self.assertEqual(catalog.get_recipe("sft-qlora")["peft"], "qlora")
        self.assertEqual(catalog.get_hardware_profile("nvidia-32-80gb")["memory_gb"], {"min": 32, "max": 80})
        row = catalog.get_requirement("sft-lora", "windows-wsl2-nvidia")
        self.assertIn("wsl2", row["components"])
        self.assertTrue(row["estimated"])

    def test_every_task_has_a_row_per_platform(self):
        rows = catalog.CATALOG["requirements"]["rows"]
        tasks = {"sft-lora", "sft-qlora", "sft-full", "dpo", "grpo", "distillation", "playground", "mlx-sft"}
        platforms = {p["id"] for p in catalog.CATALOG["requirements"]["platforms"]}
        self.assertEqual({(r["task"], r["platform"]) for r in rows}, {(t, p) for t in tasks for p in platforms})


if __name__ == "__main__":
    unittest.main()
