"""P28's settings check: the real engine's dry run on a CPU, with the probe's staged inputs and
job spec (the `launch_spec` keys the scheduler puts at the top), checks and estimates for the
GPU the run will get (`target_device`), not the machine it runs on."""

# pyright: reportMissingImports=false

from pathlib import Path

from helpers import FAST, LORA, attempt, issue_codes

from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb

PROBE = {"base_model_version_id": "mv_probe", "dataset_version_ids": ["dsv_probe"]}


def _target(bf16: bool) -> dict[str, object]:
    return {"kind": "cuda", "name": "Fake 24 GB", "memory_gib": 24.0, "bf16": bf16}


def test_the_probe_checks_and_estimates_for_its_target_gpu(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    staged = {"model": model_dir, "train": train_file}
    run = attempt(
        tmp_path, "sft", FAST, staged, common=LORA, dry_run=True,
        target_device=_target(bf16=True), **PROBE,
    )  # fmt: skip
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_DRY_RUN_OK
    assert run.prepared is not None
    assert run.prepared.ok, issue_codes(run)
    est = run.prepared.estimate
    assert 0 < est.gpu_mem_gib_lo <= est.gpu_mem_gib_hi < 24
    assert est.total_steps > 0

    # bf16 is checked against the target GPU: refused on one without it, named in the issue.
    bf16 = {**LORA, "precision": "bf16"}
    refused = attempt(
        tmp_path, "sft", FAST, staged, common=bf16, dry_run=True,
        target_device=_target(bf16=False), **PROBE,
    )  # fmt: skip
    assert refused.prepared is not None
    assert not refused.prepared.ok
    assert any("Fake 24 GB" in i.message for i in refused.prepared.issues), issue_codes(refused)
    allowed = attempt(
        tmp_path, "sft", FAST, staged, common=bf16, dry_run=True,
        target_device=_target(bf16=True), **PROBE,
    )  # fmt: skip
    assert allowed.prepared is not None
    assert not any("Fake 24 GB" in i.message for i in allowed.prepared.issues)
