def test_team_colors_live_outside_dashboard():
    from cdm_stats.team_colors import TEAM_COLORS, team_colors
    assert TEAM_COLORS["GL"] == ("#F1C61B", "#000000")
    assert team_colors("ZZZ", "#123456") == ("#123456", "#123456")


def test_scope_shape_and_hidden_teams_excluded(client):
    resp = client.get("/api/scope")
    assert resp.status_code == 200
    teams = resp.json()
    abbrs = [t["abbreviation"] for t in teams]
    assert abbrs == ["ALU", "DVS", "ELV", "GL", "OUG"]  # Felines played but is hidden
    gl = next(t for t in teams if t["abbreviation"] == "GL")
    assert gl == {"abbreviation": "GL", "team_name": "GodLike",
                  "primary_color": "#F1C61B", "secondary_color": "#000000",
                  "logo": "/logos/gl.png"}


def test_scope_logo_is_actual_filename_or_null(client):
    teams = {t["abbreviation"]: t for t in client.get("/api/scope").json()}
    assert teams["ALU"]["logo"] == "/logos/alu.svg"  # mixed extensions


def test_logo_lookup_case_insensitive_and_missing():
    from cdm_stats.api.deps import logo_url
    assert logo_url("ETs") == "/logos/ets.png"
    assert logo_url("NoSuchTeam") is None


def test_scope_follows_event_filter(client):
    def abbrs(q):
        return [t["abbreviation"] for t in client.get(f"/api/scope?{q}").json()]
    assert abbrs("event=regionals") == ["ALU", "ELV"]
    assert abbrs("event=summer") == ["DVS", "GL", "OUG"]
    assert abbrs("event=all&start=2026-07-03") == ["ALU", "DVS", "ELV", "GL", "OUG"]
    assert abbrs("start=2026-12-01") == []


def test_opponents(client):
    resp = client.get("/api/teams/GL/opponents")
    assert resp.status_code == 200
    assert [t["abbreviation"] for t in resp.json()] == ["DVS", "OUG"]
    assert set(resp.json()[0]) == {"abbreviation", "team_name", "primary_color",
                                   "secondary_color", "logo"}
    dvs = [t["abbreviation"] for t in client.get("/api/teams/DVS/opponents").json()]
    assert dvs == ["GL"]  # Felines hidden
    assert client.get("/api/teams/GL/opponents?event=regionals").json() == []


def test_opponents_unknown_team_404(client):
    assert client.get("/api/teams/ZZZ/opponents").status_code == 404
