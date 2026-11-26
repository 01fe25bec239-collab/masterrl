"""The `import_dataset` task (IMPORT job, `fetch` sandbox: network, writes only to staging).

It downloads the files of a dataset and nothing else; a DATA_PREP job checks them after.

- `source: "hub"`: `repo` (`owner/name`) at `revision` (a branch, tag or commit) from the
  Hugging Face Hub. The revision is resolved to its commit SHA first and every file is
  fetched at that SHA, so the import is exactly one snapshot. Only data files and archives
  are fetched (README and other files are skipped). A private repo needs a token: the
  server grants the job a secret and names its variable in `token_env`.
- `source: "remote"`: one `https://` `url`.

Rules: only `https://` to public addresses (every redirect is checked again, and the token
is never sent to another host); at most `max_files` files and `max_bytes` bytes, counted as
they arrive; file names must stay inside the folder. Output: `files/<path>` and
`import.json` (what was fetched, with each file's size and sha256 and the Hub commit SHA).
`allow_private_hosts: true` lifts the address rule for tests on this machine; the server
never sets it.
"""

import hashlib
import ipaddress
import json
import os
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, Any, cast, override

from mrl_runtime.adapter.events import Progress
from mrl_runtime.adapter.types import Issue
from mrl_runtime.builtin.data_prep import ARCHIVE_SUFFIXES
from mrl_runtime.builtin.tasks import BuiltinTask, TaskContext, TaskEvent, TaskOutput
from mrl_runtime.data.readers import SUFFIXES
from mrl_runtime.errors import TASK_INPUT_INVALID, MrlError
from mrl_runtime.model._json import arr, integer, obj, text

# Code this part owns (guidance/fragments/errors/p27.yaml).
DOWNLOAD_REFUSED = "MRL-DATA-272"

HUB = "https://huggingface.co"
IMPORT_FILE = "import.json"
FILES_DIR = "files"
MAX_FILES = 1000
MAX_BYTES = 20 * 1024**3
TIMEOUT_S = 60
_CHUNK = 1024 * 1024
_REPO = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9][A-Za-z0-9._-]*$")
_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$")
_TOKEN_ENV = re.compile(r"^MRL_CREDENTIAL_[A-Z0-9_]{1,64}$")
_SHA = re.compile(r"^[0-9a-f]{40}$")


def _refused(why: str) -> MrlError:
    return MrlError(DOWNLOAD_REFUSED, "The app could not download this dataset.", detail=why)


@dataclass(frozen=True)
class Policy:
    """Where downloads may go."""

    allow_private: bool = False

    def check(self, url: str) -> None:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme != "https" and not (self.allow_private and parts.scheme == "http"):
            raise _refused(f"only https:// links are allowed, not {parts.scheme or 'none'}://")
        if not parts.hostname or parts.username or parts.password:
            raise _refused("the link needs a host name and no user name or password")
        if self.allow_private:
            return
        try:
            infos = socket.getaddrinfo(parts.hostname, parts.port or 443, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise _refused(f"{parts.hostname} could not be found: {exc}") from exc
        for info in infos:
            ip = ipaddress.ip_address(info[4][0])
            if not ip.is_global or ip.is_multicast:
                raise _refused(f"{parts.hostname} is a private or local address ({ip})")


class _Redirects(urllib.request.HTTPRedirectHandler):
    def __init__(self, policy: Policy) -> None:
        super().__init__()
        self.policy = policy

    @override
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        # The token is an unredirected header, so no redirect ever carries it.
        self.policy.check(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


@dataclass
class Fetcher:
    policy: Policy
    token: str | None
    max_bytes: int
    cancelled: Callable[[], bool]
    fetched: int = 0

    def _open(self, url: str) -> Any:
        self.policy.check(url)
        req = urllib.request.Request(url, headers={"User-Agent": "masterrl-import/1"})
        if self.token:
            req.add_unredirected_header("Authorization", f"Bearer {self.token}")
        opener = urllib.request.build_opener(_Redirects(self.policy))
        try:
            return opener.open(req, timeout=TIMEOUT_S)
        except urllib.error.HTTPError as exc:
            hint = " (is the repo private? it needs a token)" if exc.code in (401, 403) else ""
            raise _refused(f"{url} answered {exc.code}{hint}") from exc
        except OSError as exc:
            raise _refused(f"{url}: {exc}") from exc

    def json(self, url: str) -> object:
        with self._open(url) as resp:
            body = resp.read(16 * 1024 * 1024 + 1)
        if len(body) > 16 * 1024 * 1024:
            raise _refused(f"{url} answered with too much data")
        try:
            return json.loads(body)
        except ValueError as exc:
            raise _refused(f"{url} did not answer with JSON") from exc

    def download(self, url: str, dest: Path) -> tuple[int, str]:
        dest.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with self._open(url) as resp, dest.open("xb") as f:
            while chunk := resp.read(_CHUNK):
                if self.cancelled():
                    raise _refused("the import was cancelled")
                size += len(chunk)
                self.fetched += len(chunk)
                if self.fetched > self.max_bytes:
                    raise _refused(f"the dataset is larger than {self.max_bytes} bytes")
                digest.update(chunk)
                f.write(chunk)
        return size, digest.hexdigest()


def safe_name(path: str) -> str:
    """A downloaded file's path inside `files/`, or a refusal."""
    p = PurePosixPath(path)
    if (
        not path
        or "\\" in path
        or p.is_absolute()
        or any(part in ("", ".", "..") or part.startswith(".") for part in p.parts)
    ):
        raise _refused(f"the file name {path!r} would land outside the import folder")
    return p.as_posix()


def wanted(path: str) -> bool:
    lower = path.lower()
    return PurePosixPath(lower).suffix in SUFFIXES or lower.endswith(ARCHIVE_SUFFIXES)


def _int(inputs: Mapping[str, Any], key: str, default: int) -> int:
    n = integer(inputs.get(key))
    return n if n is not None and n > 0 else default


def _validate(inputs: Mapping[str, Any], staged: Mapping[str, Path]) -> list[Issue]:
    del staged
    issues: list[Issue] = []

    def bad(path: str, message: str) -> None:
        issues.append(Issue(TASK_INPUT_INVALID, message, path=f"/inputs/{path}"))

    source = inputs.get("source")
    if source == "hub":
        if not _REPO.match(text(inputs.get("repo")) or ""):
            bad("repo", "Name the Hub dataset as owner/name.")
        if not _REVISION.match(text(inputs.get("revision", "main")) or ""):
            bad("revision", "The revision must be a branch, tag or commit name.")
    elif source == "remote":
        url = text(inputs.get("url")) or ""
        if not url.startswith(("https://", "http://")):
            bad("url", "Give an https:// link to the dataset file.")
    else:
        bad("source", "`source` must be hub or remote.")
    token_env = inputs.get("token_env")
    if token_env is not None and not _TOKEN_ENV.match(text(token_env) or ""):
        bad("token_env", "`token_env` must name a granted credential (MRL_CREDENTIAL_…).")
    return issues


def _hub(ctx: TaskContext, fetch: Fetcher, out: Path) -> Iterator[TaskEvent]:
    endpoint = (text(ctx.inputs.get("endpoint")) or HUB).rstrip("/")
    repo = str(ctx.inputs["repo"])
    revision = str(ctx.inputs.get("revision", "main"))
    quoted = urllib.parse.quote(revision, safe="")
    info = obj(fetch.json(f"{endpoint}/api/datasets/{repo}/revision/{quoted}")) or {}
    sha = text(info.get("sha")) or ""
    if not _SHA.match(sha):
        raise _refused(f"the Hub did not give a commit for {repo}@{revision}")
    names = [text((obj(s) or {}).get("rfilename")) or "" for s in arr(info.get("siblings")) or []]
    chosen = sorted(n for n in names if n and wanted(n))
    if not chosen:
        raise _refused(f"{repo}@{sha} has no data files")
    limit = _int(ctx.inputs, "max_files", MAX_FILES)
    if len(chosen) > limit:
        raise _refused(f"{repo} has {len(chosen)} data files; the limit is {limit}")
    files: list[dict[str, Any]] = []
    for i, name in enumerate(chosen, 1):
        rel = safe_name(name)
        url = f"{endpoint}/datasets/{repo}/resolve/{sha}/{urllib.parse.quote(rel)}"
        size, digest = fetch.download(url, out / FILES_DIR / rel)
        files.append({"path": rel, "size": size, "sha256": digest})
        yield Progress(step=i, total_steps=len(chosen), phase="download")
    _write(out, {"source": "hub", "repo": repo, "revision": revision, "sha": sha, "files": files})


def _remote(ctx: TaskContext, fetch: Fetcher, out: Path) -> Iterator[TaskEvent]:
    url = str(ctx.inputs["url"])
    base = PurePosixPath(urllib.parse.unquote(urllib.parse.urlsplit(url).path)).name
    rel = safe_name(base or "download")
    if not wanted(rel):
        raise _refused(f"{rel} is not a dataset file type the app reads")
    size, digest = fetch.download(url, out / FILES_DIR / rel)
    yield Progress(step=1, total_steps=1, phase="download")
    files = [{"path": rel, "size": size, "sha256": digest}]
    _write(out, {"source": "remote", "url": url, "files": files})


def _write(out: Path, doc: dict[str, Any]) -> None:
    (out / IMPORT_FILE).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


def _run(ctx: TaskContext) -> Iterator[TaskEvent]:
    token_env = text(ctx.inputs.get("token_env"))
    fetch = Fetcher(
        Policy(allow_private=ctx.inputs.get("allow_private_hosts") is True),
        os.environ.get(token_env) if token_env else None,
        _int(ctx.inputs, "max_bytes", MAX_BYTES),
        ctx.cancelled,
    )
    out = ctx.output_dir
    out.mkdir(parents=True, exist_ok=True)
    yield from (_hub if ctx.inputs.get("source") == "hub" else _remote)(ctx, fetch, out)
    doc = cast(dict[str, Any], json.loads((out / IMPORT_FILE).read_text(encoding="utf-8")))
    for f in doc["files"]:
        yield TaskOutput(f"{FILES_DIR}/{f['path']}", "data")
    yield TaskOutput(IMPORT_FILE, "manifest", "json")


IMPORT_DATASET = BuiltinTask(
    name="import_dataset",
    job_kind="IMPORT",
    artifact_kind="dataset",
    validate=_validate,
    run=_run,
)
