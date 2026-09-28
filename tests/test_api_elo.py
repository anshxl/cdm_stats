import sqlite3

import pytest


def _series(body):
    return {s["team"]["abbreviation"]: s for s in body["trajectory"]}


def test_elo_points_filter_points_not_ratings(api_db_path):
    from cdm_stats.db.queries import get_team_id_by_abbr
    from cdm_stats.db.queries_views import elo_points
    from cdm_stats.metrics.elo import get_elo_history
    from cdm_stats.metrics.filters import MatchFilter
    conn = sqlite3.connect(api_db_path)
    gl = get_team_id_by_abbr(conn, "GL")
    pts = elo_points(conn, gl, MatchFilter())
    assert [p["elo"] for p in pts] == [h["elo_after"] for h in get_elo_history(conn, gl)]
    assert pts[0] == {"match_date": "2026-01-01", "elo": pts[0]["elo"], "opponent": "DVS",
                      "result": "W", "label": "CDM Spring"}
    summer = elo_points(conn, gl, MatchFilter.from_event("summer"))
    assert summer == pts[-4:]
    assert elo_points(conn, gl, MatchFilter(start="2026-07-03", end="2026-07-03")) == [pts[-2]]
    conn.close()


def test_elo_trajectory_default(client):
    resp = client.get("/api/elo")
    assert resp.status_code == 200
    body = resp.json()
    assert body["view"] == "trajectory" and body["seed"] == 1000.0 and body["current"] == []
    series = _series(body)
    assert list(series) == ["ALU", "DVS", "ELV", "GL", "OUG"]  # hidden Felines absent
    assert len(series["GL"]["points"]) == 12 and len(series["DVS"]["points"]) == 7
    assert series["GL"]["low_confidence"] is True
    assert series["GL"]["team"]["primary_color"] == "#F1C61B"
    assert set(series["GL"]["points"][0]) == {"match_date", "elo", "opponent", "result", "label"}


def test_elo_trajectory_filter_chooses_points_and_teams(client):
    full = _series(client.get("/api/elo").json())
    summer = _series(client.get("/api/elo?event=summer").json())
    assert list(summer) == ["DVS", "GL", "OUG"]
    assert summer["GL"]["points"] == full["GL"]["points"][-4:]  # ratings use every competition
    assert list(_series(client.get("/api/elo?event=regionals").json())) == ["ALU", "ELV"]


def test_elo_current(client):
    body = client.get("/api/elo?view=current").json()
    assert body["view"] == "current" and body["trajectory"] == []
    elos = [c["elo"] for c in body["current"]]
    assert elos == sorted(elos, reverse=True)
    assert {c["team"]["abbreviation"] for c in body["current"]} == {"ALU", "DVS", "ELV", "GL", "OUG"}
    gl_all = next(c for c in body["current"] if c["team"]["abbreviation"] == "GL")
    summer = client.get("/api/elo?view=current&event=summer").json()["current"]
    assert {c["team"]["abbreviation"] for c in summer} == {"DVS", "GL", "OUG"}
    gl_summer = next(c for c in summer if c["team"]["abbreviation"] == "GL")
    assert gl_summer == gl_all and set(gl_all) == {"team", "elo", "low_confidence"}
    assert gl_all["elo"] == pytest.approx(_series(client.get("/api/elo").json())["GL"]["points"][-1]["elo"])


def test_elo_invalid_view_422(client):
    assert client.get("/api/elo?view=table").status_code == 422
