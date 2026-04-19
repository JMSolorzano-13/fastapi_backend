# FastAPI API (port 8001) + optional ACA worker target (Service Bus consumer).
# Build: docker build --target prod -t fastapi-backend .
# Worker: docker build --target prod-worker -t fastapi-worker .

FROM python:3.12-slim-bookworm AS base
WORKDIR /app

# Poetry 2+ required: pyproject.toml uses [tool.poetry.requires-plugins] (invalid in Poetry 1.x).
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl wkhtmltopdf \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "poetry>=2.0.0"

ENV POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-ansi --only main --no-root

COPY . .

# ---------------------------------------------------------------------------
# API — uvicorn (ACA ingress target port 8001)
# ---------------------------------------------------------------------------
FROM base AS prod
EXPOSE 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001", "--proxy-headers", "--forwarded-allow-ips", "*"]

FROM prod AS dev

# ---------------------------------------------------------------------------
# Worker — Service Bus consumer (echo by default; FASTAPI_SB_WORKER_ECHO=0 + queue list → SAT dispatch, see worker/sat_sb_dispatch.py)
# ---------------------------------------------------------------------------
FROM base AS prod-worker
CMD ["python", "-m", "worker.service_bus_worker"]

FROM prod-worker AS dev-worker

# ---------------------------------------------------------------------------
# ACA scheduled job — daily SAT enqueue (Go ``/cron --job all`` parity).
# Use with ``terraform_azure_siigofiscal`` ``compute.sat_sync_entrypoint = "fastapi_python"``.
# ---------------------------------------------------------------------------
FROM base AS prod-sat-sync-job
CMD ["python", "-m", "scripts.sat.aca_daily_sat_sync"]

FROM prod-sat-sync-job AS dev-sat-sync-job
