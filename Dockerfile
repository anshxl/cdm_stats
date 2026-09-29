# Stage 1: build the React frontend (src/api/types.ts is checked in, so no Python here).
FROM node:24-slim AS frontend
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: FastAPI app served by uvicorn.
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ ./src/
RUN uv sync --frozen --no-dev

# api/deps.py resolves paths from the repo root (/app): data/cdl.db unless DB_PATH is set,
# and api/app.py serves frontend/dist.
COPY data/cdl.db ./data/cdl.db
COPY --from=frontend /app/frontend/dist ./frontend/dist

CMD exec uvicorn cdm_stats.api.app:app --host 0.0.0.0 --port ${PORT:-8000}
