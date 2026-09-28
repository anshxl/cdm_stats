import base64

from fastapi.testclient import TestClient

from cdm_stats.api.app import create_app


def _basic(user: str, password: str) -> dict:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


# --- auth (ports dashboard/auth.py) -------------------------------------------

def test_auth_disabled_without_password(client):
    assert client.get("/api/scope").status_code == 200


def test_auth_401_without_credentials(monkeypatch, api_db_path):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    resp = TestClient(create_app(dist_dir=None)).get("/api/scope")
    assert resp.status_code == 401
    assert resp.headers["WWW-Authenticate"] == 'Basic realm="CDM Stats"'
    assert resp.text == "Authentication required."


def test_auth_wrong_credentials_and_malformed_header(monkeypatch):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    c = TestClient(create_app(dist_dir=None))
    assert c.get("/api/scope", headers=_basic("cdm", "wrong")).status_code == 401
    assert c.get("/api/scope", headers={"Authorization": "Basic !!!"}).status_code == 401
    assert c.get("/api/scope", headers={"Authorization": "Bearer x"}).status_code == 401


def test_auth_200_with_credentials_and_custom_user(monkeypatch, client):
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    monkeypatch.setenv("DASHBOARD_USER", "coach")
    from cdm_stats.api.app import get_conn
    app = create_app(dist_dir=None)
    app.dependency_overrides[get_conn] = client.app.dependency_overrides[get_conn]
    c = TestClient(app)
    assert c.get("/api/scope", headers=_basic("coach", "secret")).status_code == 200
    assert c.get("/api/scope", headers=_basic("cdm", "secret")).status_code == 401


def test_auth_applies_to_static_paths(monkeypatch, tmp_path):
    (tmp_path / "index.html").write_text("<html>spa</html>")
    monkeypatch.setenv("DASHBOARD_PASSWORD", "secret")
    c = TestClient(create_app(dist_dir=tmp_path))
    assert c.get("/team").status_code == 401
    assert c.get("/team", headers=_basic("cdm", "secret")).status_code == 200


# --- static mount + SPA fallback ---------------------------------------------

def test_spa_fallback_serves_files_and_index(monkeypatch, tmp_path):
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    (tmp_path / "index.html").write_text("<html>spa</html>")
    (tmp_path / "assets").mkdir()
    (tmp_path / "assets" / "app.js").write_text("console.log(1)")
    c = TestClient(create_app(dist_dir=tmp_path))
    assert c.get("/").text == "<html>spa</html>"
    assert c.get("/h2h").text == "<html>spa</html>"
    assert c.get("/elo/deep/link").text == "<html>spa</html>"
    assert c.get("/assets/app.js").text == "console.log(1)"
    # Unknown API paths stay 404 JSON, never the SPA.
    resp = c.get("/api/nope")
    assert resp.status_code == 404 and "spa" not in resp.text


def test_no_static_mount_without_dist(monkeypatch, tmp_path):
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    c = TestClient(create_app(dist_dir=tmp_path / "missing"))
    assert c.get("/team").status_code == 404


def test_spa_fallback_does_not_escape_dist(monkeypatch, tmp_path):
    monkeypatch.delenv("DASHBOARD_PASSWORD", raising=False)
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>spa</html>")
    (tmp_path / "secret.txt").write_text("nope")
    c = TestClient(create_app(dist_dir=dist))
    assert "nope" not in c.get("/..%2Fsecret.txt").text


# --- shared filter params -----------------------------------------------------

def test_invalid_event_and_dates_are_422(client):
    assert client.get("/api/scope?event=winter").status_code == 422
    assert client.get("/api/scope?start=not-a-date").status_code == 422
    assert client.get("/api/scope?end=2026-13-01").status_code == 422
    assert client.get("/api/scope?event=spring&start=2026-01-01&end=2026-12-31").status_code == 200
