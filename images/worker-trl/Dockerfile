# mrl-worker-trl: the TRL engine's trainer process (`mrl-worker --engine trl`).
# Build from the repo root:
#   GPU (CUDA 13 wheels from uv.lock; run with the NVIDIA container toolkit):
#     docker build -f images/worker-trl/Dockerfile -t mrl-worker-trl:gpu .
#   CPU (runtimes/trainer.lock, CPU-only torch):
#     docker build -f images/worker-trl/Dockerfile --build-arg VARIANT=cpu -t mrl-worker-trl:cpu .
# The agent runs it without a network, with the staging folder, the read-only inputs and the
# worker socket folder mounted, and passes the attempt nonce in MRL_WORKER_NONCE.

ARG PYTHON_IMAGE=python:3.12.13-slim-trixie@sha256:229a2c5bfa27522db7815ea81f9bed70af17ccb9de9fc7ad142b1877b5830d36
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS build
ARG VARIANT=gpu
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_NO_CACHE=1 UV_PYTHON_DOWNLOADS=never
WORKDIR /src
COPY python/mrl-runtime python/mrl-runtime
COPY python/adapters/mrl-adapter-trl python/adapters/mrl-adapter-trl
WORKDIR /src/python/adapters/mrl-adapter-trl
# Exact versions and hashes from the locks; wheels only, so no compiler is ever needed.
RUN set -eu; \
    case "$VARIANT" in \
      gpu) uv export --frozen --no-dev --no-emit-project --no-emit-local -o /tmp/requirements.txt; backend="" ;; \
      cpu) cp runtimes/trainer.lock /tmp/requirements.txt; backend="--torch-backend cpu" ;; \
      *) echo "VARIANT must be gpu or cpu" >&2; exit 1 ;; \
    esac; \
    uv venv /opt/venv --python /usr/local/bin/python3.12; \
    uv pip install --python /opt/venv/bin/python --only-binary :all: --require-hashes $backend -r /tmp/requirements.txt; \
    uv pip install --python /opt/venv/bin/python --no-deps ../../mrl-runtime .; \
    /opt/venv/bin/python -c "from mrl_runtime.adapter import load_engine, resolve_engine; load_engine(resolve_engine('trl')); from mrl_runtime.data.validate import find_schema_dir; find_schema_dir()"

FROM ${PYTHON_IMAGE}
# No package installers or download tools in the runtime image.
RUN rm -rf /usr/local/lib/python3.12/site-packages/pip* /usr/local/lib/python3.12/ensurepip \
      /usr/local/bin/pip* /usr/local/bin/idle* /usr/local/bin/pydoc* \
      /usr/bin/apt /usr/bin/apt-* /usr/lib/apt /etc/apt \
 && useradd --uid 10001 --user-group --no-create-home --home-dir /nonexistent \
      --shell /usr/sbin/nologin worker
COPY --from=build /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HOME=/tmp/hf \
    XDG_CACHE_HOME=/tmp/cache
USER 10001:10001
WORKDIR /tmp
CMD ["/opt/venv/bin/mrl-worker", "--engine", "trl"]
