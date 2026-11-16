# mrl-reward-exec: runs reward components and serves RewardService on a Unix socket.
# Build from the repo root:  docker build -f images/reward-exec/Dockerfile -t mrl-reward-exec .
# Run read-only, without a network, as the sandbox launcher does:
#   docker run --rm --read-only --network none -v <bundle>:/bundle:ro -v <dir>:/run/reward \
#     mrl-reward-exec --bundle /bundle --listen unix:/run/reward/r.sock \
#     --reward-version-id rwdv_... --nonce-fd 3
# Built-in components only: user code in a bundle is refused until the sandbox launcher (P29).

ARG PYTHON_IMAGE=python:3.12.13-slim-trixie@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_NO_CACHE=1 UV_PYTHON_DOWNLOADS=never
WORKDIR /src
COPY pyproject.toml uv.lock ./
COPY python/mrl-runtime/pyproject.toml python/mrl-runtime/pyproject.toml
COPY python/mrl-sdk/pyproject.toml python/mrl-sdk/pyproject.toml
COPY python/mrl-rewards python/mrl-rewards
# Exact versions and hashes from uv.lock; wheels only, so no compiler is ever needed.
RUN uv export --frozen --package mrl-rewards --no-dev --no-emit-workspace -o /tmp/requirements.txt \
 && uv venv /opt/venv --python /usr/local/bin/python3.12 \
 && uv pip install --python /opt/venv/bin/python --only-binary :all: --require-hashes -r /tmp/requirements.txt \
 && uv pip install --python /opt/venv/bin/python --no-deps ./python/mrl-rewards \
 && /opt/venv/bin/python -c "import mrl_rewards.exec.server, mrl_rewards.registry as r; r.builtin_registry()"

FROM ${PYTHON_IMAGE}
# No package installers or download tools in the runtime image.
RUN rm -rf /usr/local/lib/python3.12/site-packages/pip* /usr/local/lib/python3.12/ensurepip \
      /usr/local/bin/pip* /usr/local/bin/idle* /usr/local/bin/pydoc* \
      /usr/bin/apt /usr/bin/apt-* /usr/lib/apt /etc/apt \
 && useradd --uid 10001 --user-group --no-create-home --home-dir /nonexistent \
      --shell /usr/sbin/nologin reward
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER 10001:10001
WORKDIR /nonexistent
ENTRYPOINT ["/opt/venv/bin/mrl-reward-exec"]
