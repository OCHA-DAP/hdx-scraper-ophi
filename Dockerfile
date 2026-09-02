# --- Stage 1: Build & Install ---
FROM public.ecr.aws/unocha/python:3.13-stable AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Use the base /srv directory to inherit proper non-root permissions
WORKDIR /srv

# Ensures uv copies files instead of hardlinking to a cache that won't exist in Stage 2
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# 1. Install System Dependencies
RUN --mount=type=cache,target=/var/cache/apk \
    apk add --upgrade \
    git

# 2. Copy only dependency locks first for layer caching
COPY pyproject.toml uv.lock ./

# 3. Install Dependencies (without project code)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# 4. Copy the rest of the codebase
COPY . .

# 5. Build and install the project non-editably into the .venv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# 6. Delete the raw source tree to prevent shadowing the installed package
RUN rm -rf src/ .git/ tests/

# --- Stage 2: Dagster gRPC code location ---
# Built and pushed separately with `docker build --target dagster-grpc` - not the
# default target, so the plain `docker build .` used by publish.yaml is unaffected.
# Runs as its own code location against the shared dagster-azure webserver/daemon
# deployment (see dagster-azure/AZURE_SETUP_PLAN.md).
FROM public.ecr.aws/unocha/python:3.13-stable AS dagster-grpc

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /srv
COPY --from=builder /srv /srv
ENV PATH="/srv/.venv/bin:${PATH}"

EXPOSE 4000
CMD ["dagster", "api", "grpc", "-m", "hdx.scraper.ophi.dagster_defs.definitions", "-a", "defs", "-h", "0.0.0.0", "-p", "4000"]

# --- Stage 3: Final Runtime (default target) ---
FROM public.ecr.aws/unocha/python:3.13-stable AS runtime

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Align with Stage 1
WORKDIR /srv

# 1. Copy the entire working directory (includes .venv and root scripts like run.py)
COPY --from=builder /srv /srv

# 2. Prepend the Virtual Environment to the PATH
ENV PATH="/srv/.venv/bin:${PATH}"

CMD ["python3", "run.py"]
