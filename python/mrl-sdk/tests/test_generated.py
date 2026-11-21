import importlib.util
from pathlib import Path

from mrl_sdk._generated import OPERATIONS

SCRIPT = Path(__file__).parents[1] / "scripts" / "generate.py"


def load_generator():
    spec = importlib.util.spec_from_file_location("mrl_sdk_generate", SCRIPT)
    assert spec
    assert spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generated_files_match_the_openapi_document() -> None:
    assert load_generator().stale() == [], "run python/mrl-sdk/scripts/generate.py"


def test_every_runs_and_jobs_operation_is_listed() -> None:
    for op in [
        "submit_run",
        "list_runs",
        "get_run",
        "cancel_run",
        "get_run_config",
        "list_run_attempts",
        "get_run_timeline",
        "create_experiment",
        "list_experiments",
        "get_experiment",
        "update_experiment",
        "list_jobs",
        "get_job",
        "cancel_job",
        "update_job",
    ]:
        assert op in OPERATIONS, op
    submit = OPERATIONS["submit_run"]
    assert (submit.method, submit.path) == ("POST", "/api/v1/projects/{prj}/runs")
    assert submit.headers == ("Idempotency-Key",)
    assert submit.has_body
    assert OPERATIONS["cancel_run"].path.endswith(":cancel")
