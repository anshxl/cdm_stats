import pytest


def test_scrims_shape(client):
    resp = client.get("/api/scrims")
    assert resp.status_code == 200
    body = resp.json()
    assert body["overall"] == {"wins": 3, "losses": 1, "total": 4, "win_pct": 75.0}
    assert [m["mode"] for m in body["by_mode"]] == ["SnD", "HP", "Control"]
    assert body["by_mode"][0] == {"mode": "SnD", "wins": 1, "losses": 1, "total": 2, "win_pct": 50.0}
    assert [m["map_name"] for m in body["maps"]] == ["Tunisia", "Summit", "Raid"]
    tun = body["maps"][0]
    assert (tun["played"], tun["wins"], tun["losses"], tun["win_pct"]) == (2, 1, 1, 50.0)
    assert tun["avg_margin"] == pytest.approx(-0.5)
    assert tun["flag"] == {"kind": "low_sample", "label": "n=2"}
    assert [r["opponent"] for r in tun["recent"]] == ["OUG", "DVS"]
    assert set(tun["recent"][0]) == {"date", "week", "opponent", "our_score", "opp_score", "result"}
    assert body["trend"] == [
        {"match_date": "2026-02-25", "played": 2, "wins": 2, "win_pct": 100.0},
        {"match_date": "2026-03-03", "played": 2, "wins": 1, "win_pct": 50.0},
    ]


def test_scrims_mode_map_opponent_filters(client):
    snd = client.get("/api/scrims?mode=SnD").json()
    assert snd["overall"]["total"] == 2 and [m["map_name"] for m in snd["maps"]] == ["Tunisia"]
    summit = client.get("/api/scrims?map=Summit").json()
    assert summit["overall"]["total"] == 1 and len(summit["trend"]) == 1
    assert len(summit["maps"]) == 3  # as in Dash, the map table ignores the map filter
    oug = client.get("/api/scrims?opponent=OUG").json()
    assert oug["overall"] == {"wins": 1, "losses": 1, "total": 2, "win_pct": 50.0}


def test_scrims_filter_by_date_only(client):
    late = client.get("/api/scrims?start=2026-03-01").json()
    assert late["overall"]["total"] == 2
    assert client.get("/api/scrims?event=regionals").json() == client.get("/api/scrims").json()


def test_scrims_errors(client):
    assert client.get("/api/scrims?mode=TDM").status_code == 422
    assert client.get("/api/scrims?opponent=ZZZ").status_code == 404
    assert client.get("/api/scrims?start=bad").status_code == 422


def test_scrim_options(client):
    resp = client.get("/api/scrims/options")
    assert resp.status_code == 200
    body = resp.json()
    assert body["maps"] == ["Raid", "Summit", "Tunisia"]
    assert [t["abbreviation"] for t in body["opponents"]] == ["DVS", "OUG"]
    assert body["opponents"][0]["team_name"] == "Diavolos"
    assert client.get("/api/scrims/options?mode=SnD").json()["maps"] == ["Tunisia"]
    late = client.get("/api/scrims/options?start=2026-03-01").json()
    assert [t["abbreviation"] for t in late["opponents"]] == ["OUG"]
    assert client.get("/api/scrims/options?mode=x").status_code == 422
