import json
import socketserver
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, ClassVar

import pytest

from mrl_sdk import Client, MrlError


class Fake(BaseHTTPRequestHandler):
    """Records requests; answers with the next queued (status, body, location)."""

    seen: ClassVar[list[dict[str, Any]]] = []
    answers: ClassVar[list[tuple[int, dict[str, Any], str | None]]] = []

    def handle_one(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        Fake.seen.append(
            {
                "port": self.server.server_address[1],
                "method": self.command,
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": json.loads(body) if body else None,
            }
        )
        status, answer, location = Fake.answers.pop(0)
        data = json.dumps(answer).encode("utf-8")
        self.send_response(status)
        if location:
            self.send_header("location", location)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PATCH = handle_one  # noqa: N815

    def log_message(self, format: str, *args: Any) -> None:
        return


class QuickServer(HTTPServer):
    """Skips the reverse DNS lookup `HTTPServer` does on bind (slow on some machines)."""

    def server_bind(self) -> None:
        socketserver.TCPServer.server_bind(self)
        self.server_name = "127.0.0.1"
        self.server_port = self.server_address[1]


def serve() -> tuple[QuickServer, str]:
    httpd = QuickServer(("127.0.0.1", 0), Fake)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, f"http://127.0.0.1:{httpd.server_port}"


@pytest.fixture
def server() -> Iterator[str]:
    Fake.seen.clear()
    Fake.answers.clear()
    httpd, url = serve()
    yield url
    httpd.shutdown()


def test_submit_sends_key_token_and_body(server: str) -> None:
    rejected = {
        "id": "run_1",
        "state": "REJECTED",
        "state_reason": {"code": "MRL-CAP-001", "problems": [{"code": "MRL-CAP-001"}]},
    }
    Fake.answers.append((201, rejected, None))
    mrl = Client(server, "tok")
    run = mrl.submit_run(
        "prj_1", {"name": "n", "algorithm_id": "sft", "base_model_version_id": "mv_1"}
    )
    assert run["state"] == "REJECTED"
    req = Fake.seen[0]
    assert (req["method"], req["path"]) == ("POST", "/api/v1/projects/prj_1/runs")
    assert req["headers"]["authorization"] == "Bearer tok"
    assert len(req["headers"]["idempotency-key"]) == 36
    assert req["body"]["algorithm_id"] == "sft"

    Fake.answers.append((201, rejected, None))
    mrl.submit_run(
        "prj_1",
        {"name": "n", "algorithm_id": "sft", "base_model_version_id": "mv_1"},
        idempotency_key="again",
    )
    assert Fake.seen[1]["headers"]["idempotency-key"] == "again"


def test_queries_paths_and_problems(server: str) -> None:
    Fake.answers.append((200, {"items": [], "next_cursor": None}, None))
    problem = {"code": "MRL-STATE-170", "title": "No.", "detail": "finished"}
    Fake.answers.append((409, problem, None))
    Fake.answers.append((200, {"id": "job_1", "priority": 3}, None))
    mrl = Client(server, launch_secret="s")
    assert mrl.list_runs("prj_1", state="QUEUED", limit=5)["items"] == []
    assert Fake.seen[0]["path"] == "/api/v1/projects/prj_1/runs?state=QUEUED&limit=5"
    assert Fake.seen[0]["headers"]["x-mrl-launch-secret"] == "s"
    assert "authorization" not in Fake.seen[0]["headers"]
    with pytest.raises(MrlError) as err:
        mrl.cancel_run("run_1")
    assert (err.value.code, err.value.status, err.value.detail) == (
        "MRL-STATE-170",
        409,
        "finished",
    )
    assert Fake.seen[1]["path"] == "/api/v1/runs/run_1:cancel"
    assert mrl.set_job_priority("job_1", 3)["priority"] == 3
    assert (Fake.seen[2]["method"], Fake.seen[2]["body"]) == ("PATCH", {"priority": 3})


def test_bad_calls_fail_before_sending() -> None:
    with pytest.raises(ValueError, match="token"):
        Client("http://x")
    with pytest.raises(ValueError, match="no query parameter"):
        Client("http://x", "t").call("get_run", path={"run": "r"}, query={"bogus": 1})


def test_redirects_stay_on_the_server(server: str) -> None:
    other, other_url = serve()
    try:
        mrl = Client(server, "tok", launch_secret="secret")
        # Another origin: refused before anything is sent there.
        Fake.answers.append((302, {}, f"{other_url}/api/v1/jobs/job_1"))
        with pytest.raises(MrlError) as err:
            mrl.get_job("job_1")
        assert err.value.code == "MRL-ENV-170"
        assert [r["port"] for r in Fake.seen] == [int(server.rsplit(":", 1)[1])]
        # The same origin: followed, credentials kept.
        Fake.seen.clear()
        Fake.answers.append((307, {}, "/api/v1/jobs/job_2"))
        Fake.answers.append((200, {"id": "job_2"}, None))
        assert mrl.get_job("job_1")["id"] == "job_2"
        assert [r["path"] for r in Fake.seen] == ["/api/v1/jobs/job_1", "/api/v1/jobs/job_2"]
        for r in Fake.seen:
            assert r["headers"]["authorization"] == "Bearer tok"
            assert r["headers"]["x-mrl-launch-secret"] == "secret"
    finally:
        other.shutdown()
