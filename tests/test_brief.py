"""/api/h2h/brief: facts -> prompt -> Claude. The model is never called; fakes replace it."""
import json

import pytest

from cdm_stats.api import brief


def _map(name, tags=(), our=(0.5, 8), their=(0.5, 8), bans=(0, 10)):
    return {"map_name": name, "tags": list(tags),
            "your_rate": {"value": our[0], "n": our[1], "flag": None},
            "opp_rate": {"value": their[0], "n": their[1], "flag": None},
            "opp_bans": {"count": bans[0], "total": bans[1]}}


def _h2h(modes, team_low=False, opp_low=False, recent=6):
    return {
        "team": {"abbreviation": "OPT", "team_name": "OpTic Texas"},
        "opp": {"abbreviation": "FAZ", "team_name": "FaZe"},
        "elo": {"team": {"elo": 1084.4, "low_confidence": team_low},
                "opp": {"elo": 1031.6, "low_confidence": opp_low}},
        "h2h_series": {"wins": 2, "losses": 1},
        "opp_recent_series": [{"result": "W", "score": f"3-{i % 3}", "opponent": f"T{i}"}
                              for i in range(recent)],
        "modes": [{"mode": m, "maps": maps} for m, maps in modes],
    }


def test_facts_header_elo_series_and_recent():
    f = brief.brief_facts(_h2h([]), "summer")
    assert f["team"] == "OpTic Texas (OPT)" and f["opp"] == "FaZe (FAZ)"
    assert f["event"] == "summer"
    assert f["elo"] == {"team": 1084, "opp": 1032, "low_confidence": False}
    assert f["h2h_series"] == "2-1"
    assert f["opp_recent"] == ["W 3-0 vs T0", "W 3-1 vs T1", "W 3-2 vs T2",
                               "W 3-0 vs T3", "W 3-1 vs T4"]


@pytest.mark.parametrize("team_low,opp_low,want", [
    (False, False, False), (True, False, True), (False, True, True)])
def test_facts_low_confidence_if_either_side(team_low, opp_low, want):
    assert brief.brief_facts(_h2h([], team_low, opp_low), "all")["elo"]["low_confidence"] is want


def test_facts_only_tagged_maps_with_their_own_numbers():
    snd = [_map("Rewind", ["pick"], our=(5 / 7, 7), their=(0.4, 5), bans=(1, 11)),
           _map("Hacienda", ["they_ban"], our=(0.5, 6), their=(0.5, 6), bans=(5, 11)),
           _map("Protocol")]
    f = brief.brief_facts(_h2h([("SnD", snd)]), "all")
    assert f["modes"] == [{"mode": "SnD", "status": "tagged", "maps": [
        {"map": "Rewind", "tags": ["pick"],
         "our_win_rate": "71% (n=7)", "their_win_rate": "40% (n=5)"},
        {"map": "Hacienda", "tags": ["they_ban"], "their_ban_rate": "45% (5 of 11 series)"},
    ]}]


def test_facts_map_with_ban_and_they_ban_carries_both():
    hp = [_map("Summit", ["ban", "they_ban"], our=(0.25, 4), their=(0.75, 4), bans=(3, 6))]
    m = brief.brief_facts(_h2h([("HP", hp)]), "all")["modes"][0]["maps"][0]
    assert m == {"map": "Summit", "tags": ["ban", "they_ban"],
                 "our_win_rate": "25% (n=4)", "their_win_rate": "75% (n=4)",
                 "their_ban_rate": "50% (3 of 6 series)"}


def test_facts_rates_round_half_up_like_the_page():
    snd = [_map("Rewind", ["pick"], our=(5 / 8, 8), their=(1 / 8, 8))]
    m = brief.brief_facts(_h2h([("SnD", snd)]), "all")["modes"][0]["maps"][0]
    assert m["our_win_rate"] == "63% (n=8)"  # the page's toFixed(0); round() would give 62
    assert m["their_win_rate"] == "13% (n=8)"


def test_facts_mode_status():
    modes = [
        ("SnD", [_map("Rewind", ["pick"])]),
        ("HP", [_map("Summit", our=(0.5, 4), their=(0.5, 4))]),   # enough data, no tags
        ("Control", [_map("Raid", our=(1.0, 9), their=(0.0, 3)),  # one side below MIN_N
                     _map("Hacienda", our=(0.0, 2), their=(1.0, 7))]),
    ]
    f = brief.brief_facts(_h2h(modes), "all")
    assert [(m["mode"], m["status"], m["maps"]) for m in f["modes"]][1:] == [
        ("HP", "no_clear_edge", []), ("Control", "not_enough_data", [])]
    assert f["modes"][0]["status"] == "tagged"


# --- prompt --------------------------------------------------------------------

def test_prompt_user_is_the_facts_as_stable_json():
    a = {"team": "X", "modes": [], "elo": {"opp": 1, "team": 2}}
    b = {"elo": {"team": 2, "opp": 1}, "modes": [], "team": "X"}
    system, user = brief.brief_prompt(a)
    assert json.loads(user) == a
    assert brief.brief_prompt(b) == (system, user)  # key order never changes the prompt


# --- model call ------------------------------------------------------------------

class _Block:
    def __init__(self, type, text=""):
        self.type, self.text = type, text


class _Resp:
    def __init__(self, blocks, stop_reason="end_turn"):
        self.content, self.stop_reason = blocks, stop_reason
        self.usage = type("U", (), {"input_tokens": 10, "output_tokens": 5})()


@pytest.fixture
def sdk(monkeypatch):
    """Replace anthropic.Anthropic; `sdk["resp"]` or `sdk["error"]` sets the outcome."""
    state = {"resp": _Resp([_Block("text", "ok")]), "error": None, "kwargs": None, "clients": 0}

    class _Messages:
        def create(self, **kwargs):
            state["kwargs"] = kwargs
            if state["error"]:
                raise state["error"]
            return state["resp"]

    class _Client:
        def __init__(self, **_):
            state["clients"] += 1
            self.messages = _Messages()

    monkeypatch.setattr(brief.anthropic, "Anthropic", _Client)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    return state


def test_generate_joins_text_blocks_and_skips_thinking(sdk):
    sdk["resp"] = _Resp([_Block("thinking"), _Block("text", " Rewind suggests "),
                         _Block("text", "an edge. ")])
    assert brief.generate_brief("SYS", "USER") == "Rewind suggests an edge."
    kw = sdk["kwargs"]
    assert kw["model"] == brief.MODEL and kw["output_config"] == {"effort": "low"}
    assert kw["system"] == "SYS" and kw["messages"] == [{"role": "user", "content": "USER"}]


@pytest.mark.parametrize("resp", [
    _Resp([_Block("text", "partial")], stop_reason="refusal"),
    _Resp([_Block("text", "partial")], stop_reason="max_tokens"),
    _Resp([_Block("thinking")]),
])
def test_generate_rejects_refusal_truncation_and_empty(sdk, resp):
    sdk["resp"] = resp
    with pytest.raises(brief.BriefUnavailable):
        brief.generate_brief("SYS", "USER")


def test_generate_wraps_sdk_errors(sdk):
    import httpx2
    sdk["error"] = brief.anthropic.APIConnectionError(
        request=httpx2.Request("POST", "https://api.anthropic.com/v1/messages"))
    with pytest.raises(brief.BriefUnavailable):
        brief.generate_brief("SYS", "USER")


def test_generate_without_key_never_builds_a_client(sdk, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY")
    with pytest.raises(brief.BriefUnavailable):
        brief.generate_brief("SYS", "USER")
    assert sdk["clients"] == 0


# --- route -----------------------------------------------------------------------

GENERAL_503 = "Brief unavailable. Try again later."


@pytest.fixture(autouse=True)
def _fresh_state():
    brief._cache.clear()
    brief._calls.clear()
    yield
    brief._cache.clear()
    brief._calls.clear()


class _FakeGenerate:
    """Stands in for generate_brief; records (system, user) and returns or raises `outcome`."""

    def __init__(self):
        self.calls, self.outcome = [], "Slums suggests an edge."

    def __call__(self, system, user):
        self.calls.append((system, user))
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


@pytest.fixture
def fake(monkeypatch):
    f = _FakeGenerate()
    monkeypatch.setattr(brief, "generate_brief", f)
    return f


def test_route_returns_brief_built_from_the_h2h_facts(client, fake):
    resp = client.post("/api/h2h/brief?team=GL&opp=DVS")
    assert resp.status_code == 200 and resp.json() == {"text": "Slums suggests an edge."}
    [(system, user)] = fake.calls
    facts = json.loads(user)
    assert system == brief.SYSTEM
    assert facts["team"].endswith("(GL)") and facts["event"] == "all"
    snd = next(m for m in facts["modes"] if m["mode"] == "SnD")
    assert [(m["map"], m["tags"]) for m in snd["maps"]] == [("Slums", ["pick"])]


def test_route_caches_per_view(client, fake):
    for _ in range(2):
        assert client.post("/api/h2h/brief?team=GL&opp=DVS").status_code == 200
    assert len(fake.calls) == 1
    client.post("/api/h2h/brief?team=GL&opp=DVS&event=spring")
    client.post("/api/h2h/brief?team=DVS&opp=GL")
    assert len(fake.calls) == 3  # event and side are part of the key


def test_route_ignores_date_range_so_it_cannot_bypass_the_cache(client, fake):
    client.post("/api/h2h/brief?team=GL&opp=DVS")
    client.post("/api/h2h/brief?team=GL&opp=DVS&start=2026-01-01&end=2026-03-01")
    client.post("/api/h2h/brief?team=GL&opp=DVS&start=2026-01-02")
    assert len(fake.calls) == 1


def test_route_failure_is_general_503_and_not_cached(client, fake):
    fake.outcome = brief.BriefUnavailable("APIConnectionError at https://secret.example")
    resp = client.post("/api/h2h/brief?team=GL&opp=DVS")
    assert resp.status_code == 503 and resp.json() == {"detail": GENERAL_503}
    fake.outcome = "Now it works."
    assert client.post("/api/h2h/brief?team=GL&opp=DVS").json() == {"text": "Now it works."}
    assert len(fake.calls) == 2


def test_route_without_key_is_503(client, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    resp = client.post("/api/h2h/brief?team=GL&opp=DVS")
    assert resp.status_code == 503 and resp.json() == {"detail": GENERAL_503}


def test_route_daily_limit_counts_failed_calls(client, fake, monkeypatch):
    monkeypatch.setattr(brief, "DAILY_LIMIT", 1)
    fake.outcome = brief.BriefUnavailable("boom")
    assert client.post("/api/h2h/brief?team=GL&opp=DVS").status_code == 503
    fake.outcome = "fine"
    resp = client.post("/api/h2h/brief?team=DVS&opp=GL")
    assert resp.status_code == 503 and resp.json() == {"detail": GENERAL_503}
    assert len(fake.calls) == 1  # the limit stopped the second call before the model


def test_route_limit_does_not_block_cached_views(client, fake, monkeypatch):
    monkeypatch.setattr(brief, "DAILY_LIMIT", 1)
    client.post("/api/h2h/brief?team=GL&opp=DVS")
    assert client.post("/api/h2h/brief?team=GL&opp=DVS").status_code == 200


def test_route_bad_teams_match_h2h(client, fake):
    assert client.post("/api/h2h/brief?team=NOPE&opp=DVS").status_code == 404
    assert client.post("/api/h2h/brief?team=GL&opp=GL").status_code == 422
    assert fake.calls == []
