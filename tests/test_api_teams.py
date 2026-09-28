import pytest


# --- profile -------------------------------------------------------------------

def _maps(body):
    return {m["map_name"]: m for m in body["maps"]}


def test_profile_shape_header_and_record(client):
    resp = client.get("/api/teams/GL/profile")
    assert resp.status_code == 200
    body = resp.json()
    assert body["team"]["abbreviation"] == "GL" and body["team"]["logo"] == "/logos/gl.png"
    assert isinstance(body["elo"], float)
    assert body["low_confidence"] is True  # only 4 matches in GL's latest season
    assert body["record"] == {"series_wins": 12, "series_losses": 0,
                              "map_wins": 36, "map_losses": 12}
    assert body["overall_map_win_rate"] == pytest.approx(0.75)


def test_profile_map_rows_carry_map_flags_and_splits(client):
    maps = _maps(client.get("/api/teams/GL/profile").json())
    tun = maps["Tunisia"]
    assert (tun["mode"], tun["wins"], tun["losses"]) == ("SnD", 12, 0)
    assert tun["win_rate"] == {"value": 1.0, "n": 12, "flag": {"kind": "up", "label": "+25 vs avg"}}
    assert (tun["pick_wins"], tun["pick_losses"], tun["defend_wins"], tun["defend_losses"]) == (12, 0, 0, 0)
    assert set(tun["strength"]) == {"rating", "weighted_sample", "total_played", "low_confidence"}
    assert tun["banned"] == {"count": 0, "total": 12}
    assert tun["picked"] == {"count": 12, "total": 12}
    assert len(tun["history"]) == 12
    assert set(tun["history"][0]) == {"match_date", "opponent", "score", "pick_context",
                                      "picked_by", "result", "family", "label"}
    assert tun["history"][0]["match_date"] == "2026-07-04"
    assert maps["Summit"]["win_rate"]["flag"] == {"kind": "down", "label": "-75 vs avg"}


def test_profile_includes_banned_but_unplayed_maps(client):
    maps = _maps(client.get("/api/teams/GL/profile").json())
    so = maps["Standoff"]
    assert (so["wins"], so["losses"], so["history"]) == (0, 0, [])
    assert so["win_rate"] == {"value": None, "n": 0, "flag": {"kind": "low_sample", "label": "n=0"}}
    assert so["strength"]["rating"] is None
    assert so["banned"] == {"count": 12, "total": 12}
    assert maps["Hacienda"]["opp_banned"] == {"count": 6, "total": 6}  # series with opp ban data


def test_profile_ban_summary_per_mode(client):
    body = client.get("/api/teams/GL/profile").json()
    assert body["ban_summary"] == [
        {"mode": "Control", "map_name": "Standoff",
         "flag": {"kind": "down", "label": "Banned 100%"}},
    ]


def test_profile_event_filter_changes_results(client):
    summer = client.get("/api/teams/GL/profile?event=summer").json()
    assert summer["record"] == {"series_wins": 4, "series_losses": 0, "map_wins": 12, "map_losses": 4}
    assert _maps(summer)["Tunisia"]["win_rate"]["n"] == 4
    regionals = client.get("/api/teams/GL/profile?event=regionals").json()
    assert regionals["record"]["series_wins"] == 0
    assert regionals["maps"] == [] and regionals["overall_map_win_rate"] is None
    assert regionals["ban_summary"] == []


def test_profile_low_sample_flag(client):
    maps = _maps(client.get("/api/teams/GL/profile?start=2026-07-02").json())  # 3 series
    assert maps["Tunisia"]["win_rate"]["flag"] == {"kind": "low_sample", "label": "n=3"}


def test_profile_404_and_422(client):
    assert client.get("/api/teams/ZZZ/profile").status_code == 404
    assert client.get("/api/teams/GL/profile?event=nope").status_code == 422


# --- players ---------------------------------------------------------------------

def _tiles(body):
    return {t["player_name"]: t for t in body["tiles"]}


def test_players_shape_and_form_flag(client):
    resp = client.get("/api/teams/GL/players")
    assert resp.status_code == 200
    body = resp.json()
    assert body["players"] == ["Alpha", "Bravo"]
    alpha, bravo = _tiles(body)["Alpha"], _tiles(body)["Bravo"]
    # Default series=last10: GL's newest 10 series; Alpha is hot in the last 5.
    assert (alpha["kills"], alpha["deaths"], alpha["games"]) == (600, 400, 40)
    assert alpha["kd"] == {"value": 1.5, "n": 10, "flag": {"kind": "up", "label": "Hot · 2.00 L5"}}
    assert bravo["kd"] == {"value": 1.0, "n": 10, "flag": None}
    assert alpha["op_kills_per_pull"] == {"value": 2.0, "n": 10, "flag": None}
    assert bravo["op_kills_per_pull"]["value"] is None
    assert set(alpha) >= {"assists", "avg_pos_eng_pct", "op_kills", "op_pulls"}
    assert set(body["kd_trend"][0]) == {"player_name", "match_date", "kills", "deaths", "kd"}
    assert set(body["ops_trend"][0]) == {"player_name", "match_date", "op_kills", "op_pulls",
                                         "maps", "kills_per_pull"}
    s = body["recent_series"][0]
    assert (s["match_date"], s["opponent"], s["our_maps"], s["their_maps"]) == ("2026-07-04", "OUG", 3, 1)
    assert set(s["maps"][0]) == {"result_id", "slot", "map_name", "mode", "won",
                                 "our_score", "their_score", "players"}
    assert set(s["maps"][0]["players"][0]) == {"player_name", "kills", "deaths", "assists",
                                               "op_kills", "op_pulls"}


@pytest.mark.parametrize("series, alpha_kills, n", [("last10", 600, 10), ("all", 680, 12), ("DVS", 320, 6)])
def test_players_series_drives_tiles_trends_and_recent(client, series, alpha_kills, n):
    body = client.get(f"/api/teams/GL/players?series={series}").json()
    alpha = _tiles(body)["Alpha"]
    assert alpha["kills"] == alpha_kills and alpha["kd"]["n"] == n
    assert len([p for p in body["kd_trend"] if p["player_name"] == "Alpha"]) == n
    assert len(body["ops_trend"]) == n
    assert len(body["recent_series"]) == n


def test_players_player_mode_and_event(client):
    body = client.get("/api/teams/GL/players?player=Alpha&mode=SnD&series=all").json()
    assert list(_tiles(body)) == ["Alpha"]
    assert _tiles(body)["Alpha"]["games"] == 24
    assert {m["mode"] for s in body["recent_series"] for m in s["maps"]} == {"SnD"}
    summer = client.get("/api/teams/GL/players?event=summer").json()
    assert _tiles(summer)["Alpha"]["kd"] == {"value": 2.0, "n": 4, "flag": None}  # < 5 series


def test_players_errors(client):
    assert client.get("/api/teams/ZZZ/players").status_code == 404
    assert client.get("/api/teams/DVS/players").status_code == 404  # player stats: GL only
    assert client.get("/api/teams/GL/players?series=ZZZ").status_code == 422
    assert client.get("/api/teams/GL/players?mode=TDM").status_code == 422
