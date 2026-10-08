import pytest


def test_scrims_shape(client):
    resp = client.get("/api/scrims")
    assert resp.status_code == 200
    body = resp.json()
    assert body["overall"] == {"wins": 3, "losses": 1, "total": 4, "win_pct": 0.75}
    assert [m["mode"] for m in body["by_mode"]] == ["SnD", "HP", "Control"]
    assert body["by_mode"][0] == {"mode": "SnD", "wins": 1, "losses": 1, "total": 2, "win_pct": 0.5}
    assert [m["map_name"] for m in body["maps"]] == ["Tunisia", "Summit", "Raid"]
    tun = body["maps"][0]
    assert (tun["played"], tun["wins"], tun["losses"], tun["win_pct"]) == (2, 1, 1, 0.5)
    assert tun["avg_margin"] == pytest.approx(-0.5)
    assert tun["flag"] == {"kind": "low_sample", "label": "n=2"}
    assert [r["opponent"] for r in tun["recent"]] == ["OUG", "DVS"]
    assert set(tun["recent"][0]) == {"date", "week", "opponent", "our_score", "opp_score", "result"}
    assert body["trend"] == [
        {"match_date": "2026-02-25", "played": 2, "wins": 2, "win_pct": 1.0},
        {"match_date": "2026-03-03", "played": 2, "wins": 1, "win_pct": 0.5},
    ]


def test_scrims_trend_by_mode(client):
    rows = client.get("/api/scrims").json()["trend_by_mode"]
    assert [(r["mode"], r["match_date"], r["played"], r["wins"]) for r in rows] == [
        ("SnD", "2026-02-25", 1, 1), ("SnD", "2026-03-03", 1, 0),
        ("HP", "2026-02-25", 1, 1), ("Control", "2026-03-03", 1, 1),
    ]
    assert {r["mode"] for r in client.get("/api/scrims?mode=HP").json()["trend_by_mode"]} == {"HP"}


def test_scrims_mode_map_opponent_filters(client):
    snd = client.get("/api/scrims?mode=SnD").json()
    assert snd["overall"]["total"] == 2 and [m["map_name"] for m in snd["maps"]] == ["Tunisia"]
    summit = client.get("/api/scrims?map=Summit").json()
    assert summit["overall"]["total"] == 1 and len(summit["trend"]) == 1
    assert len(summit["maps"]) == 3  # as in Dash, the map table ignores the map filter
    oug = client.get("/api/scrims?opponent=OUG").json()
    assert oug["overall"] == {"wins": 1, "losses": 1, "total": 2, "win_pct": 0.5}


def test_scrims_player_maps(client):
    rows = client.get("/api/scrims").json()["player_maps"]
    assert [(r["date"], r["opponent"], r["map_name"], r["player_name"], r["kills"], r["op_kills"], r["op_pulls"])
            for r in rows] == [
        ("2026-02-25", "DVS", "Tunisia", "Alpha", 10, None, None),
        ("2026-02-25", "DVS", "Summit", "Alpha", 30, 6, 4),
        ("2026-03-03", "OUG", "Raid", "Alpha", 20, None, None),
        ("2026-03-03", "OUG", "Raid", "Bravo", 15, None, None),
    ]
    summit = rows[1]
    assert (summit["mode"], summit["result"], summit["our_score"], summit["opp_score"]) == ("HP", "W", 250, 200)
    assert [r["map_name"] for r in client.get("/api/scrims?mode=SnD").json()["player_maps"]] == ["Tunisia"]
    assert [r["map_name"] for r in client.get("/api/scrims?map=Summit").json()["player_maps"]] == ["Summit"]
    assert len(client.get("/api/scrims?opponent=OUG").json()["player_maps"]) == 2
    assert len(client.get("/api/scrims?start=2026-03-01").json()["player_maps"]) == 2


def test_scrims_filter_by_date_only(client):
    late = client.get("/api/scrims?start=2026-03-01").json()
    assert late["overall"]["total"] == 2


@pytest.mark.parametrize("path", ["/api/scrims", "/api/scrims/options"])
def test_scrim_endpoints_take_dates_not_event(client, path):
    params = client.get("/openapi.json").json()["paths"][path]["get"]["parameters"]
    names = {p["name"] for p in params}
    assert {"start", "end"} <= names
    assert "event" not in names


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
