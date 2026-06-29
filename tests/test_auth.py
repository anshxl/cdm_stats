import base64

from flask import Flask

from cdm_stats.dashboard.auth import init_auth


def _make_app() -> Flask:
    server = Flask(__name__)

    @server.route("/")
    def index():
        return "ok"

    init_auth(server)
    return server


def _basic_header(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def test_disabled_without_password(monkeypatch):
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    client = _make_app().test_client()
    assert client.get("/").status_code == 200


def test_blocks_without_credentials(monkeypatch):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    client = _make_app().test_client()
    resp = client.get("/")
    assert resp.status_code == 401
    assert "Basic" in resp.headers["WWW-Authenticate"]


def test_blocks_wrong_credentials(monkeypatch):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    client = _make_app().test_client()
    assert client.get("/", headers=_basic_header("cdm", "wrong")).status_code == 401


def test_allows_correct_credentials(monkeypatch):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    client = _make_app().test_client()
    assert client.get("/", headers=_basic_header("cdm", "secret")).status_code == 200


def test_custom_username(monkeypatch):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("DASHBOARD_USER", "coach")
    client = _make_app().test_client()
    assert client.get("/", headers=_basic_header("coach", "secret")).status_code == 200
    assert client.get("/", headers=_basic_header("cdm", "secret")).status_code == 401
