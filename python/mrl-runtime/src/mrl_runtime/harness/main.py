"""`mrl-worker`: the process the agent launches inside the adapter's environment.

The per-attempt nonce comes from the MRL_WORKER_NONCE environment variable, never argv, so it
does not show up in process listings.
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from mrl_runtime.adapter.manifest import load_engine, resolve_engine
from mrl_runtime.errors import MrlError
from mrl_runtime.harness.worker import EXIT_FAILED, EXIT_NO_SESSION, Harness, HarnessOptions

NONCE_ENV = "MRL_WORKER_NONCE"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mrl-worker", description="Run one training attempt for the agent."
    )
    p.add_argument(
        "--engine",
        required=True,
        help="manifest directory, mrl-engine.yaml, or installed engine name",
    )
    p.add_argument("--socket", required=True, help="agent socket path, or host:port")
    p.add_argument("--attempt-id", required=True)
    p.add_argument("--role-id", default="trainer")
    p.add_argument(
        "--secondary", action="store_true", help="not the primary role (no metrics or checkpoints)"
    )
    p.add_argument("--spool-dir", type=Path, default=None)
    p.add_argument(
        "--no-capture", action="store_true", help="do not capture stdout/stderr into LogChunks"
    )
    p.add_argument("--environment-digest", default=os.environ.get("MRL_ENVIRONMENT_DIGEST", ""))
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="mrl-worker: %(message)s")
    nonce = os.environ.get(NONCE_ENV, "")
    if not nonce:
        print(
            f"mrl-worker: {NONCE_ENV} is not set; the agent must pass the attempt nonce",
            file=sys.stderr,
        )
        return EXIT_NO_SESSION
    try:
        manifest = resolve_engine(args.engine)
        engine = load_engine(manifest)
    except MrlError as exc:
        print(f"mrl-worker: {exc}", file=sys.stderr)
        return EXIT_FAILED
    options = HarnessOptions(
        attempt_id=args.attempt_id,
        nonce=nonce,
        role_id=args.role_id,
        primary=not args.secondary,
        spool_dir=args.spool_dir,
        capture_fds=not args.no_capture,
        handle_signals=True,
        environment_digest=args.environment_digest,
    )
    code = asyncio.run(Harness(engine, manifest, args.socket, options).run())
    sys.stdout.flush()
    sys.stderr.flush()
    # The engine thread may still be running (it ignored a cancel); the agent reaps the rest.
    os._exit(code)


if __name__ == "__main__":
    sys.exit(main())
