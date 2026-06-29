"""HTTP Basic Auth gate for the dashboard.

Activates only when DASHBOARD_PASSWORD is set in the environment, so local dev
runs without a password while the Railway deployment is protected. Set both
DASHBOARD_USER (optional, defaults to "cdm") and DASHBOARD_PASSWORD as Railway
service variables; rotate the password instead of the URL if it ever leaks.
"""

import hmac
import os

from flask import Flask, Response, request


def _check(user: str, password: str) -> bool:
    expected_user = os.environ.get("DASHBOARD_USER", "cdm")
    expected_password = os.environ["DASHBOARD_PASSWORD"]
    return hmac.compare_digest(user, expected_user) and hmac.compare_digest(
        password, expected_password
    )


def init_auth(server: Flask) -> None:
    """Install a Basic Auth gate on the Flask server when a password is configured."""
    if not os.environ.get("DASHBOARD_PASSWORD"):
        return

    @server.before_request
    def require_basic_auth():
        auth = request.authorization
        if auth is None or not _check(auth.username or "", auth.password or ""):
            return Response(
                "Authentication required.",
                401,
                {"WWW-Authenticate": 'Basic realm="CDM Stats"'},
            )
