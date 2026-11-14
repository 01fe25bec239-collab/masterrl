// Tests for the generated JS loader. Run: node --test guidance/tests
// (Node resolves the folder to this index.js.)
import assert from "node:assert/strict";
import test from "node:test";

import {
  catalog,
  explainError,
  getError,
  getGoal,
  getHardwareProfile,
  getRecipe,
  getRequirement,
  getTerm,
} from "../build/index.js";

test("every error renders what happened, why and what to do", () => {
  assert.ok(catalog.errors.length > 0);
  for (const entry of catalog.errors) {
    const shown = explainError(entry.code);
    assert.equal(shown, entry);
    assert.ok(shown.title && shown.why && shown.what_to_do.steps.length > 0, entry.code);
  }
});

test("unknown codes fall back to the generic message", () => {
  assert.equal(getError("NOT_A_CODE"), undefined);
  assert.equal(explainError("NOT_A_CODE"), catalog.error_fallback);
  assert.ok(catalog.error_fallback.what_to_do.steps.length > 0);
});

test("lookups find entries", () => {
  assert.equal(getError("OOM/OOM_GPU").title, "Training paused: the GPU ran out of memory.");
  assert.equal(getTerm("wsl").term, "WSL2");
  assert.equal(getTerm("LoRA").term, "LoRA");
  assert.equal(getGoal("team-preferences").recipes[0], "dpo");
  assert.equal(getRecipe("sft-lora").estimated, true);
  assert.equal(getHardwareProfile("apple-16gb").verified, false);
  assert.equal(getRequirement("mlx-sft", "linux-nvidia").supported, false);
});

test("requirement rows carry estimated download totals", () => {
  const row = getRequirement("grpo", "linux-nvidia");
  assert.ok(row.components.includes("vllm"));
  assert.equal(row.estimated, true);
  assert.ok(row.download_gb.min > 0 && row.download_gb.min <= row.download_gb.max);
});

test("the catalog is frozen", () => {
  assert.throws(() => {
    catalog.errors[0].title = "changed";
  }, TypeError);
});
