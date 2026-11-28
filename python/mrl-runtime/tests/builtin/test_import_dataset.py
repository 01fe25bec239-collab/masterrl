"""The import_dataset task against a stand-in Hub on this machine, its refusals, and (with
MRL_NETWORK_TESTS=1) the real Hugging Face Hub."""

import hashlib
import http.server
import json
import os
import threading
import traceback
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from mrl_runtime.builtin import import_dataset
from mrl_runtime.builtin.import_dataset import IMPORT_DATASET, Fetcher, Policy, safe_name
from mrl_runtime.builtin.tasks import TaskContext
from mrl_runtime.errors import MrlError

SHA = "a" * 40
FILES = {
    "data/train.jsonl": b'{"text": "hello"}\n',
    "data/test.jsonl": b'{"text": "held out"}\n',
    "dataset_infos.json": b"{}",
    "README.md": b"# card",
}


class _Hub(http.server.BaseHTTPRequestHandler):
    seen_auth: list[str | None] = []  # noqa: RUF012 (shared by the test)

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def do_GET(self) -> None:
        _Hub.seen_auth.append(self.headers.get("Authorization"))
        if self.path.startswith("/api/datasets/acme/qa/revision/"):
            sib: list[dict[str, Any]] = [{"rfilename": n} for n in [*FILES, "../evil.jsonl"]][
                : len(FILES) if "v2" not in self.path else len(FILES) + 1
            ]
            if "bad-sha" in self.path:
                sib[0]["lfs"] = {"sha256": "0" * 64}
            return self._send(json.dumps({"sha": SHA, "siblings": sib}).encode())
        if self.path.startswith("/signed.jsonl?"):
            return self._send(FILES["data/train.jsonl"])
        if self.path == "/short.jsonl":
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.write(b'{"text":"first"}\n')
            return None
        if self.path.startswith("/redirect"):
            self.send_response(302)
            self.send_header("Location", "http://169.254.169.254/latest/meta-data")
            self.end_headers()
            return None
        prefix = f"/datasets/acme/qa/resolve/{SHA}/"
        if self.path.startswith(prefix) and self.path[len(prefix) :] in FILES:
            return self._send(FILES[self.path[len(prefix) :]])
        self.send_error(404)
        return None

    def _send(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def hub() -> Iterator[str]:
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Hub)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    _Hub.seen_auth.clear()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


@pytest.fixture
def mirror(hub: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(import_dataset, "HUB", hub)
    monkeypatch.setattr(import_dataset, "Policy", lambda: Policy(allow_private=True))


def _run(tmp: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    assert IMPORT_DATASET.validate(inputs, {}) == []
    out = tmp / "outputs"
    list(IMPORT_DATASET.run(TaskContext(inputs, {}, out, lambda: False)))
    return json.loads((out / "import.json").read_text("utf-8"))


@pytest.mark.usefixtures("mirror")
def test_remote_url_stays_out_of_reports_and_errors(tmp_path: Path, hub: str) -> None:
    query = "?signature=private-marker"
    doc = _run(tmp_path, {"source": "remote", "url": f"{hub}/signed.jsonl{query}"})
    assert doc["files"][0]["path"] == "signed.jsonl"
    assert "private-marker" not in json.dumps(doc)

    with pytest.raises(MrlError, match="answered 404") as error:
        _run(tmp_path, {"source": "remote", "url": f"{hub}/missing.jsonl{query}"})
    assert "private-marker" not in "".join(traceback.format_exception(error.value))


@pytest.mark.usefixtures("mirror")
def test_hub_import_pins_the_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MRL_CREDENTIAL_HF", "hf_secret")
    doc = _run(
        tmp_path,
        {
            "source": "hub",
            "repo": "acme/qa",
            "revision": "main",
            "config": "data",
            "split": "train",
            "token_env": "MRL_CREDENTIAL_HF",
        },
    )
    assert doc["sha"] == SHA
    body = FILES["data/train.jsonl"]
    assert doc["files"] == [
        {"path": "data/train.jsonl", "size": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    ]
    assert (tmp_path / "outputs/files/data/train.jsonl").read_bytes() == body
    assert not (tmp_path / "outputs/files/README.md").exists()
    assert not (tmp_path / "outputs/files/dataset_infos.json").exists()
    assert set(_Hub.seen_auth) == {"Bearer hf_secret"}


@pytest.mark.usefixtures("mirror")
def test_hub_file_names_outside_the_folder_are_refused(tmp_path: Path) -> None:
    inputs = {
        "source": "hub",
        "repo": "acme/qa",
        "revision": "v2",
    }
    with pytest.raises(MrlError) as err:
        _run(tmp_path, inputs)
    assert err.value.code == "MRL-DATA-272"


@pytest.mark.usefixtures("mirror")
def test_size_limit_counts_real_bytes(tmp_path: Path) -> None:
    inputs = {
        "source": "hub",
        "repo": "acme/qa",
        "max_bytes": 5,
    }
    with pytest.raises(MrlError, match="larger than 5 bytes"):
        _run(tmp_path, inputs)


@pytest.mark.usefixtures("mirror")
def test_interrupted_response_never_publishes_partial_file(tmp_path: Path, hub: str) -> None:
    with pytest.raises(MrlError, match="ended after"):
        _run(tmp_path, {"source": "remote", "url": f"{hub}/short.jsonl"})
    assert not (tmp_path / "outputs/import.json").exists()
    assert not (tmp_path / "outputs/files/short.jsonl").exists()


@pytest.mark.usefixtures("mirror")
def test_hub_files_must_match_the_upstream_digest(tmp_path: Path) -> None:
    with pytest.raises(MrlError, match="does not match"):
        _run(tmp_path, {"source": "hub", "repo": "acme/qa", "revision": "bad-sha"})
    assert not (tmp_path / "outputs/import.json").exists()


def test_private_addresses_and_plain_http_are_refused(tmp_path: Path, hub: str) -> None:
    with pytest.raises(MrlError, match="only https"):
        _run(tmp_path, {"source": "remote", "url": f"{hub}/x.jsonl"})
    with pytest.raises(MrlError, match="private or local"):
        Policy().check("https://127.0.0.1/x.jsonl")
    with pytest.raises(MrlError, match="user name"):
        Policy().check("https://user:pw@example.com/x.jsonl")


class _OnlyLoopback(Policy):
    """Lets the stand-in Hub through and nothing else, so a redirect away is refused."""

    def check(self, url: str) -> None:
        if not url.startswith("http://127.0.0.1:"):
            raise MrlError("MRL-DATA-272", "refused", detail=url)


def test_redirects_are_checked_again(tmp_path: Path, hub: str) -> None:
    fetch = Fetcher(_OnlyLoopback(), "tok", 1000, lambda: False)
    with pytest.raises(MrlError) as err:
        fetch.download(f"{hub}/redirect/x.jsonl", tmp_path / "x.jsonl")
    assert err.value.detail == "http://169.254.169.254/latest/meta-data"


@pytest.mark.parametrize("name", ["../x", "/x", "a\\b", ".git/config", ""])
def test_safe_names(name: str) -> None:
    with pytest.raises(MrlError):
        safe_name(name)


def test_inputs_are_checked() -> None:
    paths = {i.path for i in IMPORT_DATASET.validate({"source": "hub", "repo": "x"}, {})}
    assert "/inputs/repo" in paths
    assert IMPORT_DATASET.validate({"source": "ftp"}, {})[0].path == "/inputs/source"
    bad_env = {"source": "remote", "url": "https://x/y.jsonl", "token_env": "PATH"}
    assert IMPORT_DATASET.validate(bad_env, {})[0].path == "/inputs/token_env"


@pytest.mark.skipif(os.environ.get("MRL_NETWORK_TESTS") != "1", reason="needs the internet")
def test_real_hub_stores_the_commit_sha(tmp_path: Path) -> None:
    doc = _run(
        tmp_path, {"source": "hub", "repo": "openai/gsm8k", "revision": "main", "max_files": 10}
    )
    assert len(doc["sha"]) == 40
    assert doc["files"]
    for f in doc["files"]:
        path = tmp_path / "outputs/files" / f["path"]
        assert hashlib.sha256(path.read_bytes()).hexdigest() == f["sha256"]
