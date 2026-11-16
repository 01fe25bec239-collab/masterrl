"""Adapter testkit: FakeEngine, an in-process fake agent, and the TC1-TC7 conformance suite."""

import os

# The testkit starts worker subprocesses while gRPC threads run; gRPC's fork handlers only
# log noise for that (the child execs right away). Must be set before grpc is imported.
os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "0")
