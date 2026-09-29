"""FastAPI app: basic auth on every path, /api routers, built frontend + SPA fallback.

Run: uv run uvicorn cdm_stats.api.app:app --reload
"""
import base64
import hmac
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse

from cdm_stats.api import elo, h2h, scrims, teams
from cdm_stats.api.deps import ROOT, get_conn  # noqa: F401  (get_conn: override point for tests)

DEFAULT_DIST = ROOT / "frontend" / "dist"


# --- Basic auth ----------------------------------------------------------------
# Active only when DASHBOARD_PASSWORD is set; DASHBOARD_USER defaults to "cdm".

def _check(user: str, password: str) -> bool:
    expected_user = os.environ.get("DASHBOARD_USER", "cdm")
    expected_password = os.environ["DASHBOARD_PASSWORD"]
    return hmac.compare_digest(user.encode(), expected_user.encode()) and hmac.compare_digest(
        password.encode(), expected_password.encode()
    )


def _credentials(header: str | None) -> tuple[str, str] | None:
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "basic":
        return None
    try:
        user, _, password = base64.b64decode(token, validate=True).decode().partition(":")
    except (ValueError, UnicodeDecodeError):
        return None
    return user, password


async def _basic_auth(request: Request, call_next):
    creds = _credentials(request.headers.get("authorization"))
    if creds is None or not _check(*creds):
        return PlainTextResponse(
            "Authentication required.", 401, {"WWW-Authenticate": 'Basic realm="CDM Stats"'},
        )
    return await call_next(request)


def _mount_spa(app: FastAPI, dist: Path) -> None:
    root = dist.resolve()

    @app.get("/{path:path}", include_in_schema=False)
    def spa(path: str):
        if path == "api" or path.startswith("api/"):
            raise HTTPException(404)
        target = (root / path).resolve()
        if path and target.is_file() and target.is_relative_to(root):
            return FileResponse(target)
        return FileResponse(root / "index.html")


def create_app(dist_dir: Path | None = DEFAULT_DIST) -> FastAPI:
    app = FastAPI(title="CDM Stats")
    if os.environ.get("DASHBOARD_PASSWORD"):
        app.middleware("http")(_basic_auth)
    for module in (teams, h2h, scrims, elo):
        app.include_router(module.router, prefix="/api")
    # Only serve the frontend when it has been built, so tests and API-only dev work.
    if dist_dir is not None and Path(dist_dir).is_dir():
        _mount_spa(app, Path(dist_dir))
    return app


app = create_app()
