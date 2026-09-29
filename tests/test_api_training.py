"""/api/training: proxy to the Discord bot's read-only endpoint. Never touches the network."""
import io
import socket
from datetime import datetime
from urllib.error import HTTPError, URLError
from zoneinfo import ZoneInfo

import pytest

from cdm_stats.api import training

BODY = {
    "month": "2026-09",
    "months": ["2026-08", "2026-09"],
    "players": [
        {"username": "ace", "sessions": 5, "days_trained": 3},
        {"username": "bex", "sessions": 2, "days_trained": 2},
    ],
    "daily": [
        {"date": "2026-09-01", "username": "ace", "sessions": 2},
        {"date": "2026-09-02", "username": "bex", "sessions": 1},
    ],
}


@pytest.fixture(autouse=True)
def _fresh_cache():
    training._cache.clear()
    yield
    training._cache.clear()


@pytest.fixture
def fake(monkeypatch):
    """Replace the bot fetch; records the months it was asked for."""
    calls = []

    def _fetch(month):
        calls.append(month)
        return {**BODY, "month": month}

    monkeypatch.setattr(training, "fetch_training", _fetch)
    return calls


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(training, "_now", lambda: now[0])
    return now


@pytest.fixture
def bot_env(monkeypatch):
    monkeypatch.setenv("TRAINING_API_URL", "http://bot.test")
    monkeypatch.setenv("TRAINING_API_TOKEN", "s3cret")


def test_pass_through_shape(client, fake):
    resp = client.get("/api/training?month=2026-09")
    assert resp.status_code == 200
    assert resp.json() == BODY
    assert fake == ["2026-09"]


def test_default_month_is_current_month_in_new_york(client, fake):
    expected = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m")
    assert client.get("/api/training").json()["month"] == expected
    assert fake == [expected]


def test_cache_hit_within_ttl(client, fake, clock):
    client.get("/api/training?month=2026-09")
    clock[0] += training.TTL_SECONDS - 1
    assert client.get("/api/training?month=2026-09").status_code == 200
    assert fake == ["2026-09"]


def test_cache_is_per_month(client, fake, clock):
    client.get("/api/training?month=2026-09")
    client.get("/api/training?month=2026-08")
    assert fake == ["2026-09", "2026-08"]


def test_refetch_after_ttl(client, fake, clock):
    client.get("/api/training?month=2026-09")
    clock[0] += training.TTL_SECONDS + 1
    client.get("/api/training?month=2026-09")
    assert fake == ["2026-09", "2026-09"]


@pytest.mark.parametrize("month", ["2026-13", "2026-9", "sept", "2026-00", "2026-09-01"])
def test_invalid_month_422(client, fake, month):
    assert client.get(f"/api/training?month={month}").status_code == 422
    assert fake == []


# --- the real fetch_training, with urlopen replaced ---------------------------

class _Resp(io.BytesIO):
    status = 200


def test_fetch_sends_bearer_and_month(bot_env, monkeypatch):
    seen = {}

    def _urlopen(req, timeout):
        seen.update(url=req.full_url, auth=req.get_header("Authorization"), timeout=timeout)
        return _Resp(b'{"month": "2026-09", "months": [], "players": [], "daily": []}')

    monkeypatch.setattr(training, "urlopen", _urlopen)
    assert training.fetch_training("2026-09")["month"] == "2026-09"
    assert seen == {"url": "http://bot.test/training?month=2026-09",
                    "auth": "Bearer s3cret", "timeout": 5}


@pytest.mark.parametrize("missing", ["TRAINING_API_URL", "TRAINING_API_TOKEN"])
def test_503_when_env_unset(client, bot_env, monkeypatch, missing):
    monkeypatch.delenv(missing)
    monkeypatch.setattr(training, "urlopen", lambda *a, **k: pytest.fail("no fetch"))
    resp = client.get("/api/training?month=2026-09")
    assert resp.status_code == 503
    assert "not configured" in resp.json()["detail"]


@pytest.mark.parametrize("exc, text", [
    (URLError("connection refused"), "unreachable"),
    (socket.timeout("timed out"), "timed out"),
    (TimeoutError("timed out"), "timed out"),
    (HTTPError("http://bot.test/training", 401, "Unauthorized", {}, None), "401"),
    (HTTPError("http://bot.test/training", 500, "Server Error", {}, None), "500"),
])
def test_503_on_bot_failure(client, bot_env, monkeypatch, exc, text):
    def _urlopen(req, timeout):
        raise exc

    monkeypatch.setattr(training, "urlopen", _urlopen)
    resp = client.get("/api/training?month=2026-09")
    assert resp.status_code == 503
    assert text in resp.json()["detail"]


def test_503_on_non_200_success_status(client, bot_env, monkeypatch):
    resp_obj = _Resp(b"")
    resp_obj.status = 204
    monkeypatch.setattr(training, "urlopen", lambda req, timeout: resp_obj)
    resp = client.get("/api/training?month=2026-09")
    assert resp.status_code == 503
    assert "204" in resp.json()["detail"]


def test_errors_are_not_cached(client, bot_env, monkeypatch):
    def _down(req, timeout):
        raise URLError("down")

    monkeypatch.setattr(training, "urlopen", _down)
    assert client.get("/api/training?month=2026-09").status_code == 503
    monkeypatch.setattr(training, "urlopen", lambda req, timeout: _Resp(
        b'{"month": "2026-09", "months": ["2026-09"], "players": [], "daily": []}'))
    assert client.get("/api/training?month=2026-09").status_code == 200


def test_503_on_invalid_json(client, bot_env, monkeypatch):
    monkeypatch.setattr(training, "urlopen", lambda req, timeout: _Resp(b"<html>oops"))
    resp = client.get("/api/training?month=2026-09")
    assert resp.status_code == 503
    assert "invalid" in resp.json()["detail"]
