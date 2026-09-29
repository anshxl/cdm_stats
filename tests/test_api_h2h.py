def _modes(body):
    return {m["mode"]: m for m in body["modes"]}


def _row(mode, name):
    return next(r for r in mode["maps"] if r["map_name"] == name)


def test_h2h_shape_header_elo_and_recent(client):
    resp = client.get("/api/h2h?team=GL&opp=DVS")
    assert resp.status_code == 200
    body = resp.json()
    assert body["team"]["abbreviation"] == "GL" and body["opp"]["abbreviation"] == "DVS"
    assert set(body["elo"]["team"]) == {"elo", "low_confidence"}
    assert isinstance(body["elo"]["opp"]["elo"], float)
    recent = body["opp_recent_series"]
    assert len(recent) == 7  # 6 vs GL + 1 vs Felines
    assert recent[0] == {"match_date": "2026-07-03", "opponent": "GL", "result": "L",
                         "score": "1-3", "family": "CDM Summer", "label": "CDM Summer"}
    assert [m["mode"] for m in body["modes"]] == ["SnD", "HP", "Control"]
    assert [len(m["maps"]) for m in body["modes"]] == [5, 5, 3]


def test_h2h_map_row_fields(client):
    snd = _modes(client.get("/api/h2h?team=GL&opp=DVS").json())["SnD"]
    tun = _row(snd, "Tunisia")
    assert tun["h2h"] == {"wins": 6, "losses": 0}
    assert tun["your_wl"] == {"wins": 12, "losses": 0}
    assert tun["opp_wl"] == {"wins": 1, "losses": 6}
    assert tun["your_rate"] == {"value": 1.0, "n": 12, "flag": None}
    assert tun["opp_rate"]["n"] == 7
    assert set(tun) >= {"map_id", "delta", "your_strength", "opp_strength", "your_pick_wl",
                        "your_defend_wl", "opp_pick_wl", "opp_defend_wl", "opp_bans",
                        "opp_bans_h2h", "opp_picks", "tags"}
    hac = _row(_modes(client.get("/api/h2h?team=GL&opp=DVS").json())["HP"], "Hacienda")
    assert hac["opp_bans"] == {"count": 6, "total": 6}
    assert hac["opp_bans_h2h"] == {"count": 6, "total": 6}
    assert hac["opp_rate"] == {"value": None, "n": 0, "flag": {"kind": "low_sample", "label": "n=0"}}


def test_h2h_tags(client):
    modes = _modes(client.get("/api/h2h?team=GL&opp=DVS").json())
    tags = {m: {r["map_name"]: r["tags"] for r in modes[m]["maps"] if r["tags"]} for m in modes}
    assert tags["SnD"] == {"Slums": ["pick"]}          # 1.00 vs 0.00 beats Tunisia 1.00 vs 0.14
    assert tags["HP"] == {"Summit": ["ban"], "Hacienda": ["they_ban"]}
    assert tags["Control"] == {"Raid": ["pick"]}
    assert not any(m["not_enough_data"] for m in modes.values())


def test_h2h_event_filter_and_small_samples(client):
    regionals = client.get("/api/h2h?team=GL&opp=DVS&event=regionals").json()
    assert regionals["opp_recent_series"] == []
    assert all(m["not_enough_data"] for m in regionals["modes"])
    late = _modes(client.get("/api/h2h?team=GL&opp=DVS&start=2026-07-02").json())
    assert all(m["not_enough_data"] for m in late.values())
    assert _row(late["SnD"], "Tunisia")["your_rate"]["flag"] == {"kind": "low_sample", "label": "n=3"}


def test_h2h_errors(client):
    assert client.get("/api/h2h?team=ZZZ&opp=DVS").status_code == 404
    assert client.get("/api/h2h?team=GL&opp=ZZZ").status_code == 404
    assert client.get("/api/h2h?team=GL&opp=GL").status_code == 422
    assert client.get("/api/h2h?team=GL").status_code == 422
    assert client.get("/api/h2h?team=GL&opp=DVS&event=x").status_code == 422


def test_h2h_series_record(client):
    assert client.get("/api/h2h?team=GL&opp=DVS").json()["h2h_series"] == {"wins": 6, "losses": 0}
    assert client.get("/api/h2h?team=DVS&opp=GL").json()["h2h_series"] == {"wins": 0, "losses": 6}
    assert client.get("/api/h2h?team=GL&opp=DVS&start=2026-07-02").json()["h2h_series"] == {"wins": 1, "losses": 0}
    assert client.get("/api/h2h?team=GL&opp=DVS&event=regionals").json()["h2h_series"] == {"wins": 0, "losses": 0}
