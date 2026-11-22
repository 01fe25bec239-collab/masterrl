"""E tests on CPU with the tiny model: resume equivalence, pause/resume, cancel, LoRA,
exports, cpt, and the failures the engine classifies."""

# pyright: reportMissingImports=false

import json
import math
from pathlib import Path
from typing import Any

import pytest
import torch
from helpers import DATASETS, FAST, FULL, LORA, attempt, copy_model, issue_codes
from peft import PeftModel
from safetensors.numpy import load_file, save_file
from transformers import AutoModelForCausalLM, AutoTokenizer, GPT2Config, GPT2LMHeadModel

from mrl_runtime._generated.mrl.common.v1 import common_pb2 as common
from mrl_runtime._generated.mrl.worker.v1 import worker_control_pb2 as pb
from mrl_runtime.testkit.agent import FakeAgent
from mrl_runtime.testkit.runner import AttemptResult

pytestmark = pytest.mark.slow
STEPS = 10


def _losses(run: AttemptResult) -> dict[int, float]:
    return dict(run.series("train/loss"))


def _failure(run: AttemptResult) -> tuple[str, str]:
    report = run.exit_report
    assert report is not None
    assert report.HasField("failure"), report
    return common.FailureClass.Name(getattr(report.failure, "class")), report.failure.sub_code


def _at_step(step: int, message: pb.AgentMessage) -> Any:
    async def control(agent: FakeAgent, _proc: object) -> None:
        await agent.wait_for(lambda m: m.HasField("progress") and m.progress.step >= step, 60)
        await agent.send(message)

    return control


def _manifest_of(where: Path) -> dict[str, Any]:
    return json.loads((where / "checkpoint-manifest.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("common_config", [FULL, LORA], ids=["full", "lora"])
def test_pause_then_resume_continues_the_same_loss_series(
    tmp_path: Path, model_dir: Path, train_file: Path, common_config: dict[str, Any]
) -> None:
    staged = {"model": model_dir, "train": train_file}
    engine = {**FAST, "max_steps": STEPS, "save_steps": 100}
    reference = attempt(tmp_path, "sft", engine, staged, common=common_config)
    assert reference.exit_report is not None
    assert reference.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert sorted(_losses(reference)) == list(range(1, STEPS + 1))

    pause = pb.AgentMessage(pause=pb.PauseRequest(request_id="pause-1"))
    paused = attempt(
        tmp_path, "sft", engine, staged, common=common_config, control=_at_step(4, pause)
    )
    assert paused.exit_report is not None
    assert paused.exit_report.kind == pb.EXIT_KIND_PAUSED
    assert "pause-1" in paused.exit_report.acknowledged_requests
    [(_, step, where)] = paused.committed()
    assert 4 <= step < STEPS
    doc = _manifest_of(where)
    is_lora = common_config == LORA
    assert doc["kind"] == ("adapter_only" if is_lora else "full_resume")
    if torch.backends.mps.is_available() and not torch.cuda.is_available():
        assert (where / "rng_state_mps.pth").is_file()
    assert not doc["is_final"]
    roles = {f["role"] for f in doc["files"]}
    assert {
        "adapter" if is_lora else "weights",
        "optimizer",
        "scheduler",
        "rng",
        "trainer_state",
    } <= roles

    resumed = attempt(
        tmp_path, "sft", engine, {**staged, "resume_from": where}, common=common_config
    )
    assert resumed.exit_report is not None
    assert resumed.exit_report.kind == pb.EXIT_KIND_COMPLETED
    after = _losses(resumed)
    assert sorted(after) == list(range(step + 1, STEPS + 1))
    ref = _losses(reference)
    assert max(abs(after[s] - ref[s]) for s in after) < 1e-4
    assert _manifest_of(resumed.committed()[-1][2])["parent_checkpoint_id"] == doc["checkpoint_id"]


def test_cancel_stops_at_the_next_step_without_a_final_checkpoint(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    cancel = pb.AgentMessage(cancel=pb.CancelRequest(request_id="c1", grace_s=5))
    run = attempt(
        tmp_path,
        "sft",
        {**FAST, "max_steps": 200, "save_steps": 100},
        {"model": model_dir, "train": train_file},
        control=_at_step(2, cancel),
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_CANCELLED
    assert run.exit_report.final_step < 10
    assert not run.committed()


def test_on_demand_checkpoint_is_taken_at_the_next_step(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    ask = pb.AgentMessage(checkpoint_request=pb.CheckpointRequest(request_id="k1"))
    run = attempt(
        tmp_path,
        "sft",
        {**FAST, "max_steps": 8, "save_steps": 100},
        {"model": model_dir, "train": train_file},
        control=_at_step(2, ask),
    )
    steps = sorted(step for _, step, _ in run.committed())
    assert len(steps) == 2
    assert steps[0] < 8
    assert steps[1] == 8
    assert run.exit_report is not None
    assert "k1" in run.exit_report.acknowledged_requests


def test_lora_checkpoints_and_both_exports(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    staged = {"model": model_dir, "train": train_file}
    engine = {**FAST, "learning_rate": 0.01, "max_steps": 4, "save_steps": 100, "samples_every": 2}
    run = attempt(tmp_path, "sft", engine, staged, common=LORA)
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    [(_, step, where)] = run.committed()
    doc = _manifest_of(where)
    assert (step, doc["kind"], doc["is_final"]) == (4, "adapter_only", True)
    assert doc["adapter"]["method"] == "lora"
    assert doc["adapter"]["rank"] == 4
    assert "model.safetensors" not in {f["path"] for f in doc["files"]}
    samples = [s for m in run.agent.of("samples") for s in m.samples.samples]
    assert len(samples) == 4
    assert all(s.prompt for s in samples)

    adapter = attempt(
        tmp_path, "sft", {}, {"checkpoint": where}, job_kind="EXPORT",
        export={"format": "hf-peft-adapter"},
    )  # fmt: skip
    assert adapter.exit_report is not None
    assert adapter.exit_report.kind == pb.EXIT_KIND_COMPLETED
    out = adapter.agent.script.staging_dir / "outputs"
    assert (out / "adapter_model.safetensors").is_file()
    assert (out / "tokenizer.json").is_file()

    merged = attempt(
        tmp_path, "sft", {}, {"checkpoint": where, "model": model_dir}, job_kind="EXPORT",
        export={"format": "merged-safetensors"},
    )  # fmt: skip
    assert merged.exit_report is not None
    assert merged.exit_report.kind == pb.EXIT_KIND_COMPLETED
    _same_logits(model_dir, where, merged.agent.script.staging_dir / "outputs")


def _same_logits(base: Path, adapter: Path, merged: Path) -> None:
    ids = torch.tensor([[1, 5, 9, 33, 70]])
    with torch.no_grad():
        peft = PeftModel.from_pretrained(AutoModelForCausalLM.from_pretrained(base), adapter)
        want = peft(ids).logits
        got = AutoModelForCausalLM.from_pretrained(merged)(ids).logits
    assert torch.allclose(want, got, atol=1e-4)


def test_merge_without_a_base_model_is_a_config_failure(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    run = attempt(
        tmp_path, "sft", {**FAST, "max_steps": 1}, {"model": model_dir, "train": train_file},
        common=LORA,
    )  # fmt: skip
    [(_, _, where)] = run.committed()
    merged = attempt(
        tmp_path, "sft", {}, {"checkpoint": where}, job_kind="EXPORT",
        export={"format": "merged-safetensors"},
    )  # fmt: skip
    assert _failure(merged) == ("FAILURE_CLASS_USER_CONFIG", "MRL-VAL-002")


def test_hostile_resume_manifest_is_refused(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    staged = {"model": model_dir, "train": train_file}
    first = attempt(tmp_path, "sft", {**FAST, "max_steps": 1}, staged)
    [(_, _, where)] = first.committed()
    manifest = where / "checkpoint-manifest.json"
    for text, code in (("[1, 2", "MRL-CAP-251"), ('{"engine": "trl"}', "MRL-CAP-020")):
        manifest.write_text(text, encoding="utf-8")
        run = attempt(tmp_path, "sft", FAST, {**staged, "resume_from": where}, dry_run=True)
        assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", code)


def test_full_checkpoint_exports_hf_safetensors(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    run = attempt(
        tmp_path, "sft", {**FAST, "max_steps": 1}, {"model": model_dir, "train": train_file}
    )
    [(_, _, where)] = run.committed()
    out = attempt(
        tmp_path, "sft", {}, {"checkpoint": where}, job_kind="EXPORT",
        export={"format": "hf-safetensors"},
    )  # fmt: skip
    assert out.exit_report is not None
    assert out.exit_report.kind == pb.EXIT_KIND_COMPLETED
    files = {p.name for p in (out.agent.script.staging_dir / "outputs").iterdir()}
    assert {"model.safetensors", "config.json", "tokenizer.json"} <= files
    assert "optimizer.pt" not in files


def test_cpt_trains_on_raw_text(tmp_path: Path, model_dir: Path) -> None:
    text = DATASETS / "pretrain.text.v1.jsonl"
    run = attempt(tmp_path, "cpt", {**FAST, "max_steps": 2}, {"model": model_dir, "train": text})
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert sorted(_losses(run)) == [1, 2]
    assert run.prepared is not None
    estimate = run.prepared.estimate
    assert estimate.total_steps == 2
    assert 0 < estimate.gpu_mem_gib_lo <= estimate.gpu_mem_gib_hi


def test_prompt_completion_dataset_trains(tmp_path: Path, model_dir: Path) -> None:
    pairs = tmp_path / "pairs.jsonl"
    rows = [{"prompt": f"Repeat: word{i}", "completion": f" word{i}"} for i in range(16)]
    pairs.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
    run = attempt(tmp_path, "sft", {**FAST, "max_steps": 2}, {"model": model_dir, "train": pairs})
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED


@pytest.mark.parametrize("packing", [False, True])
def test_mixed_text_and_conversation_records_train(
    tmp_path: Path, model_dir: Path, packing: bool
) -> None:
    mixed = DATASETS / "sft.prompt_completion.v1.jsonl"
    run = attempt(
        tmp_path,
        "sft",
        {**FAST, "max_steps": 1, "packing": packing},
        {
            "model": model_dir,
            "train": mixed,
        },
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert len(_losses(run)) == 1


@pytest.mark.parametrize("train_on_messages", [True, False])
def test_eval_masks_use_eval_schema(
    tmp_path: Path,
    model_dir: Path,
    train_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    train_on_messages: bool,
) -> None:
    from mrl_adapter_trl.sft.trainer import SftTrainer  # noqa: PLC0415 (training test)

    pairs = DATASETS / "sft.prompt_completion.v1.jsonl"
    train, evaluation = (train_file, pairs) if train_on_messages else (pairs, train_file)
    masks = []
    prepare = SftTrainer._prepare_dataset

    def inspect_labels(self: Any, *args: Any, **kwargs: Any) -> Any:
        dataset = prepare(self, *args, **kwargs)
        if args[-1] == "eval":
            masks.extend(
                -100 in row["labels"] and any(x != -100 for x in row["labels"]) for row in dataset
            )
        return dataset

    monkeypatch.setattr(SftTrainer, "_prepare_dataset", inspect_labels)
    run = attempt(
        tmp_path,
        "sft",
        FAST,
        {"model": model_dir, "train": train, "eval": evaluation},
        dry_run=True,
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_DRY_RUN_OK
    assert masks
    assert all(masks)


@pytest.mark.parametrize("role", ["train", "eval"])
def test_l1_checks_template_for_conversational_pairs(
    tmp_path: Path, model_dir: Path, role: str
) -> None:
    model = copy_model(model_dir, tmp_path / "plain", chat_template=None)
    pairs = DATASETS / "sft.prompt_completion.v1.jsonl"
    plain = tmp_path / "plain.jsonl"
    plain.write_text(pairs.read_text(encoding="utf-8").splitlines()[0] + "\n", encoding="utf-8")
    staged = {"model": model, "train": plain, role: pairs}
    run = attempt(tmp_path, "sft", FAST, staged, dry_run=True)
    assert "MRL-CAP-016" in issue_codes(run)
    assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", "MRL-CAP-016")


def _small_gpt2(tokenizer: Any) -> Any:
    return GPT2LMHeadModel(
        GPT2Config(
            vocab_size=len(tokenizer),
            n_positions=64,
            n_embd=32,
            n_layer=1,
            n_head=2,
            bos_token_id=tokenizer.bos_token_id,
            eos_token_id=tokenizer.eos_token_id,
            tie_word_embeddings=False,
        )
    )


def test_long_prompt_sample_stays_inside_context(tmp_path: Path, model_dir: Path) -> None:
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = tmp_path / "gpt2"
    _small_gpt2(tokenizer).save_pretrained(model)
    tokenizer.save_pretrained(model)
    train = tmp_path / "train.jsonl"
    prompt = "apple " * 120
    train.write_text(json.dumps({"prompt": prompt, "completion": " yes"}) + "\n", encoding="utf-8")
    run = attempt(
        tmp_path,
        "sft",
        {
            **FAST,
            "max_steps": 1,
            "max_length": 64,
            "samples_every": 1,
            "completion_only_loss": False,
        },
        {"model": model, "train": train},
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert run.committed()
    [batch] = run.agent.of("samples")
    [sample] = batch.samples.samples
    assert sample.prompt != prompt
    assert sample.completion_tokens > 0


def test_nonfinite_eval_loss_fails_before_saving(tmp_path: Path, model_dir: Path) -> None:
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    sentinel = "[EVAL_ONLY_SENTINEL]"
    tokenizer.add_tokens([sentinel])
    token_id = tokenizer.convert_tokens_to_ids(sentinel)
    model = _small_gpt2(tokenizer)
    with torch.no_grad():
        model.get_input_embeddings().weight[token_id].fill_(float("nan"))
    checkpoint = tmp_path / "gpt2"
    model.save_pretrained(checkpoint)
    tokenizer.save_pretrained(checkpoint)
    train = tmp_path / "train.jsonl"
    evaluation = tmp_path / "eval.jsonl"
    train.write_text(json.dumps({"prompt": "apple", "completion": " yes"}) + "\n", encoding="utf-8")
    evaluation.write_text(
        json.dumps({"prompt": sentinel, "completion": " yes"}) + "\n", encoding="utf-8"
    )
    run = attempt(
        tmp_path,
        "sft",
        {**FAST, "max_steps": 1, "max_length": 32, "save_steps": 1},
        {"model": checkpoint, "train": train, "eval": evaluation},
    )
    assert math.isfinite(_losses(run)[1])
    assert _failure(run) == ("FAILURE_CLASS_NUMERIC_DIVERGENCE", "NAN_LOSS")
    assert not run.agent.of("checkpoint_staged")


@pytest.mark.parametrize("role", ["train", "eval"])
@pytest.mark.parametrize("field", ["tools", "images"])
def test_l1_rejects_unsupported_dataset_fields(
    tmp_path: Path, model_dir: Path, train_file: Path, role: str, field: str
) -> None:
    data = tmp_path / "unsupported.jsonl"
    row = json.loads(train_file.read_text(encoding="utf-8").splitlines()[0])
    row[field] = []
    data.write_text(json.dumps(row) + "\n", encoding="utf-8")
    run = attempt(
        tmp_path,
        "sft",
        FAST,
        {"model": model_dir, "train": train_file, role: data},
        dry_run=True,
    )
    assert run.prepared is not None
    assert not run.prepared.ok
    [issue] = [issue for issue in run.prepared.issues if issue.code == "MRL-DATA-001"]
    assert issue.path == f"/inputs/{role}"
    assert field in issue.message


# ---- validation and failures ----


def test_qlora_without_an_nvidia_gpu_is_refused(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    if torch.cuda.is_available():
        pytest.skip("this machine has an NVIDIA GPU")
    qlora = {"update": {"strategy": "qlora"}}
    run = attempt(
        tmp_path, "sft", FAST, {"model": model_dir, "train": train_file}, common=qlora, dry_run=True
    )
    assert "MRL-CAP-250" in issue_codes(run)
    assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", "MRL-CAP-250")


def test_model_without_chat_template_is_refused_for_conversations(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    bare = copy_model(model_dir, tmp_path / "bare", chat_template=None)
    run = attempt(tmp_path, "sft", FAST, {"model": bare, "train": train_file}, dry_run=True)
    assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", "MRL-CAP-016")


def test_template_without_generation_marks_fails_as_user_config(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    plain = "{% for m in messages %}{{ m['role'] }}: {{ m['content'] }}\n{% endfor %}"
    model = copy_model(model_dir, tmp_path / "plain", chat_template=plain)
    run = attempt(tmp_path, "sft", FAST, {"model": model, "train": train_file}, dry_run=True)
    assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", "MRL-CAP-016")


def test_invalid_records_fail_as_data(tmp_path: Path, model_dir: Path) -> None:
    bad = DATASETS / "sft.messages.v1-invalid.jsonl"
    run = attempt(tmp_path, "sft", FAST, {"model": model_dir, "train": bad}, dry_run=True)
    assert _failure(run) == ("FAILURE_CLASS_DATA", "MRL-DATA-001")


def test_cpt_refuses_a_conversation_dataset(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    run = attempt(tmp_path, "cpt", FAST, {"model": model_dir, "train": train_file}, dry_run=True)
    assert _failure(run) == ("FAILURE_CLASS_DATA", "MRL-DATA-001")


def test_pickle_model_is_refused(tmp_path: Path, model_dir: Path, train_file: Path) -> None:
    pickled = model_dir.parent / "tiny-llama-pickle"
    run = attempt(tmp_path, "sft", FAST, {"model": pickled, "train": train_file}, dry_run=True)
    assert "MRL-MODEL-001" in issue_codes(run)


def test_incomplete_resume_checkpoint_is_refused(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    staged = {"model": model_dir, "train": train_file}
    first = attempt(tmp_path, "sft", {**FAST, "max_steps": 1}, staged)
    [(_, _, where)] = first.committed()
    (where / "optimizer.pt").unlink()
    run = attempt(tmp_path, "sft", {**FAST, "max_steps": 2}, {**staged, "resume_from": where})
    assert _failure(run) == ("FAILURE_CLASS_USER_CONFIG", "MRL-CAP-251")


def test_nan_loss_ends_as_numeric_divergence(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    broken = copy_model(model_dir, tmp_path / "nan")
    weights = load_file(broken / "model.safetensors")
    weights["model.norm.weight"][:] = float("nan")
    save_file(weights, str(broken / "model.safetensors"), metadata={"format": "pt"})
    run = attempt(
        tmp_path, "sft", {**FAST, "max_steps": 1, "logging_steps": 10},
        {"model": broken, "train": train_file},
    )  # fmt: skip
    assert _failure(run) == ("FAILURE_CLASS_NUMERIC_DIVERGENCE", "NAN_LOSS")
    assert not run.committed()


@pytest.mark.parametrize("algorithm", ["sft", "cpt"])
def test_default_cadence_emits_required_metrics_on_a_short_run(
    tmp_path: Path, model_dir: Path, train_file: Path, algorithm: str
) -> None:
    train = train_file if algorithm == "sft" else DATASETS / "pretrain.text.v1.jsonl"
    run = attempt(
        tmp_path,
        algorithm,
        {"max_steps": 1, "max_length": 128},
        {"model": model_dir, "train": train},
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    for name in ("train/loss", "train/learning_rate"):
        assert [step for step, _ in run.series(name)] == [1]


def test_resume_at_budget_performs_no_more_steps(
    tmp_path: Path, model_dir: Path, train_file: Path
) -> None:
    staged = {"model": model_dir, "train": train_file}
    engine = {**FAST, "max_steps": 1}
    first = attempt(tmp_path, "sft", engine, staged, common=LORA)
    [(checkpoint_id, _, checkpoint)] = first.committed()
    resumed = attempt(tmp_path, "sft", engine, {**staged, "resume_from": checkpoint}, common=LORA)
    assert resumed.prepared is not None
    assert resumed.prepared.ok
    assert resumed.exit_report is not None
    assert resumed.exit_report.kind == pb.EXIT_KIND_COMPLETED
    assert resumed.exit_report.last_committed_checkpoint_id == checkpoint_id
    assert not resumed.agent.of("progress")
    assert not resumed.committed()


def test_long_chat_prompt_sample_keeps_the_assistant_turn(
    tmp_path: Path, model_dir: Path
) -> None:
    # ~117 tokens: trains whole within max_length 128; the sample prompt gets 128 - 48 = 80.
    turns = [{"role": "user", "content": "apple " * 20}, {"role": "assistant", "content": "ok"}]
    train = tmp_path / "long.jsonl"
    train.write_text(json.dumps({"messages": turns}) + "\n", encoding="utf-8")
    run = attempt(
        tmp_path,
        "sft",
        {**FAST, "max_steps": 1, "max_length": 128, "samples_every": 1},
        {"model": model_dir, "train": train},
    )
    assert run.exit_report is not None
    assert run.exit_report.kind == pb.EXIT_KIND_COMPLETED
    [batch] = run.agent.of("samples")
    [sample] = batch.samples.samples
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    full = tokenizer.apply_chat_template(turns[:1], tokenize=False, add_generation_prompt=True)
    assert sample.prompt != full
    assert sample.prompt.endswith(full[-24:])  # the assistant header survives truncation
