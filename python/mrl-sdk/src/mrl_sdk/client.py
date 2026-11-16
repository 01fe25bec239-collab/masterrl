"""A small client for the MasterRL API, on top of the generated operation table.

from mrl_sdk import Client
mrl = Client.from_env()                       # MRL_SERVER, MRL_TOKEN (or MRL_LAUNCH_SECRET)
run = mrl.submit_run("prj_...", {"name": "first", "algorithm_id": "sft",
                                 "base_model_version_id": "mv_..."})
if run["state"] == "REJECTED":
    for p in run["state_reason"]["problems"]:
        print(p["code"], p["message"], p["hint"])
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Mapping
from typing import Any, cast

from mrl_sdk._generated import OPERATIONS
from mrl_sdk._generated.models import (
    AttemptPage,
    Experiment,
    ExperimentPage,
    Job,
    JobPage,
    NewExperiment,
    Run,
    RunConfig,
    RunPage,
    RunSubmission,
    TimelineEntryPage,
)

LAUNCH_SECRET_HEADER = "x-mrl-launch-secret"


class MrlError(Exception):
    """The server refused a request. `code` is the catalogued code, like `MRL-STATE-170`."""

    def __init__(self, status: int, problem: dict[str, Any]) -> None:
        self.status = status
        self.problem = problem
        self.code: str = str(problem.get("code", f"HTTP-{status}"))
        self.detail: str = str(problem.get("detail", ""))
        super().__init__(f"{self.code}: {problem.get('title', '')} {self.detail}".strip())


class Client:
    """Signed-in access to one server."""

    def __init__(
        self,
        server: str,
        token: str | None = None,
        *,
        launch_secret: str | None = None,
        timeout: float = 60.0,
    ) -> None:
        if not token and not launch_secret:
            raise ValueError("pass a personal access token (or the solo launch secret)")
        self.server = server.rstrip("/")
        self.token = token
        self.launch_secret = launch_secret
        self.timeout = timeout

    @classmethod
    def from_env(cls) -> Client:
        """Reads `MRL_SERVER`, `MRL_TOKEN` and `MRL_LAUNCH_SECRET`."""
        server = os.environ.get("MRL_SERVER")
        if not server:
            raise ValueError("set MRL_SERVER to the server address")
        return cls(
            server,
            os.environ.get("MRL_TOKEN") or None,
            launch_secret=os.environ.get("MRL_LAUNCH_SECRET") or None,
        )

    def call(
        self,
        operation_id: str,
        *,
        path: dict[str, str] | None = None,
        query: Mapping[str, str | int | None] | None = None,
        headers: dict[str, str] | None = None,
        body: Any = None,
    ) -> Any:
        """Calls any operation by its OpenAPI `operationId` and returns the decoded JSON."""
        op = OPERATIONS[operation_id]
        url = op.path
        for name in op.path_params:
            value = (path or {})[name]
            url = url.replace("{" + name + "}", urllib.parse.quote(value, safe=""))
        unknown = set(query or {}) - set(op.query_params)
        if unknown:
            raise ValueError(f"{operation_id} takes no query parameter {sorted(unknown)}")
        params = {k: str(v) for k, v in (query or {}).items() if v is not None}
        if params:
            url += "?" + urllib.parse.urlencode(params)
        sent = {"accept": "application/json", **(headers or {})}
        if self.token:
            sent["authorization"] = f"Bearer {self.token}"
        if self.launch_secret:
            sent[LAUNCH_SECRET_HEADER] = self.launch_secret
        data = None
        if op.has_body:
            data = json.dumps(body if body is not None else {}).encode("utf-8")
            sent["content-type"] = "application/json"
        req = urllib.request.Request(self.server + url, data=data, headers=sent, method=op.method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as res:
                raw = res.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                problem = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                problem = {"detail": raw.decode("utf-8", "replace")}
            raise MrlError(e.code, problem if isinstance(problem, dict) else {}) from None
        return json.loads(raw) if raw else None

    # Runs

    def submit_run(
        self, project: str, submission: RunSubmission, *, idempotency_key: str | None = None
    ) -> Run:
        """Submits a run. Reuse `idempotency_key` to retry safely; a new one is made otherwise."""
        key = idempotency_key or str(uuid.uuid4())
        return cast(
            Run,
            self.call(
                "submit_run",
                path={"prj": project},
                headers={"idempotency-key": key},
                body=submission,
            ),
        )

    def list_runs(
        self,
        project: str,
        *,
        state: str | None = None,
        experiment: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> RunPage:
        """One page of a project's runs, newest first."""
        query = {"state": state, "experiment": experiment, "limit": limit, "cursor": cursor}
        return cast(RunPage, self.call("list_runs", path={"prj": project}, query=query))

    def get_run(self, run: str) -> Run:
        """One run."""
        return cast(Run, self.call("get_run", path={"run": run}))

    def cancel_run(self, run: str) -> Run:
        """Cancels a run; it is CANCELLING until its attempt stops, then CANCELLED."""
        return cast(Run, self.call("cancel_run", path={"run": run}))

    def get_run_config(self, run: str) -> RunConfig:
        """The run's immutable config snapshot."""
        return cast(RunConfig, self.call("get_run_config", path={"run": run}))

    def list_run_attempts(self, run: str, *, cursor: str | None = None) -> AttemptPage:
        """The run's attempts, oldest first."""
        query = {"cursor": cursor}
        return cast(AttemptPage, self.call("list_run_attempts", path={"run": run}, query=query))

    def get_run_timeline(self, run: str, *, cursor: str | None = None) -> TimelineEntryPage:
        """The run's state changes, oldest first."""
        query = {"cursor": cursor}
        page = self.call("get_run_timeline", path={"run": run}, query=query)
        return cast(TimelineEntryPage, page)

    # Experiments

    def create_experiment(self, project: str, experiment: NewExperiment) -> Experiment:
        """Creates an experiment."""
        made = self.call("create_experiment", path={"prj": project}, body=experiment)
        return cast(Experiment, made)

    def list_experiments(self, project: str, *, cursor: str | None = None) -> ExperimentPage:
        """One page of a project's experiments."""
        page = self.call("list_experiments", path={"prj": project}, query={"cursor": cursor})
        return cast(ExperimentPage, page)

    # Jobs

    def list_jobs(
        self,
        *,
        state: str | None = None,
        kind: str | None = None,
        project: str | None = None,
        cursor: str | None = None,
    ) -> JobPage:
        """One page of jobs you can see, newest first."""
        query = {"state": state, "kind": kind, "project": project, "cursor": cursor}
        return cast(JobPage, self.call("list_jobs", query=query))

    def get_job(self, job: str) -> Job:
        """One job."""
        return cast(Job, self.call("get_job", path={"job": job}))

    def cancel_job(self, job: str) -> Job:
        """Cancels a job."""
        return cast(Job, self.call("cancel_job", path={"job": job}))

    def set_job_priority(self, job: str, priority: int) -> Job:
        """Changes a job's priority (maintainers)."""
        body = {"priority": priority}
        return cast(Job, self.call("update_job", path={"job": job}, body=body))
