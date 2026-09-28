# Parity tests: delete together with src/cdm_stats/dashboard/
"""Each db/queries_views.py function must match the old Dash helper it replaces."""
import io

import pytest

from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
from cdm_stats.metrics.elo import update_elo
from cdm_stats.metrics.filters import MatchFilter
from cdm_stats.db import queries_views as qv

MATCHES_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2
2026-01-20,GL,DVS,GL,1,Tunisia,GL,6,4
2026-01-20,GL,DVS,GL,2,Summit,DVS,250,240
2026-01-20,GL,DVS,GL,3,Raid,GL,3,2
2026-01-20,GL,DVS,GL,4,Slums,DVS,6,5
2026-01-20,GL,DVS,GL,5,Hacienda,GL,250,200
2026-02-01,GL,OUG,OUG,1,Slums,OUG,6,1
2026-02-01,GL,OUG,OUG,2,Summit,OUG,250,100
2026-02-01,GL,OUG,OUG,3,Raid,OUG,3,0"""

SCRIM_CSV = """Date,Opponent,Map,Score
2026-02-25,DVS,Tunisia,6-3
2026-02-25,DVS,Summit,250-200
2026-03-03,OUG,Raid,3-1"""

FILTERS = [
    MatchFilter(),
    MatchFilter(start="2026-01-18"),
    MatchFilter(families=frozenset({"CDM Spring"}), end="2026-01-31"),
    MatchFilter(families=frozenset({"Regionals"})),
]


@pytest.fixture
def rich_db(db_with_tournament_match):
    db = db_with_tournament_match
    ingest_csv(db, io.StringIO(MATCHES_CSV))
    for (mid,) in db.execute("SELECT match_id FROM matches ORDER BY match_date").fetchall():
        update_elo(db, mid)
    ingest_scrims_team(db, io.StringIO(SCRIM_CSV))
    rid = db.execute("SELECT result_id FROM map_results LIMIT 1").fetchone()[0]
    db.executemany(
        """INSERT INTO tournament_player_stats (result_id, week, player_name, kills, deaths, assists)
           VALUES (?, 1, ?, 10, 10, 1)""",
        [(rid, "Zulu"), (rid, "Alpha")],
    )
    db.commit()
    return db


def _tid(db, abbr):
    return db.execute("SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)).fetchone()[0]


def _mid(db, name):
    return db.execute("SELECT map_id FROM maps WHERE map_name = ?", (name,)).fetchone()[0]


TEAMS = ["GL", "DVS", "ELV", "Q9"]  # Q9: no matches at all
MAPS = ["Tunisia", "Summit", "Hacienda"]


# --- team_profile -----------------------------------------------------------

@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("abbr", TEAMS)
def test_team_record(rich_db, f, abbr):
    from cdm_stats.dashboard.tabs.team_profile import _build_team_record
    tid = _tid(rich_db, abbr)
    assert qv.team_record(rich_db, tid, f) == _build_team_record(rich_db, tid, f)


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("abbr", TEAMS)
def test_map_record_data(rich_db, f, abbr):
    from cdm_stats.dashboard.tabs.team_profile import _build_map_record_data
    tid = _tid(rich_db, abbr)
    new = qv.map_record_data(rich_db, tid, f)
    assert new == _build_map_record_data(rich_db, tid, f)
    if abbr == "GL" and f == MatchFilter():
        assert new  # non-trivial comparison


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("abbr", TEAMS)
@pytest.mark.parametrize("map_name", MAPS)
def test_map_results_detail(rich_db, f, abbr, map_name):
    from cdm_stats.dashboard.tabs.team_profile import _build_map_results_detail
    tid, mid = _tid(rich_db, abbr), _mid(rich_db, map_name)
    assert qv.map_results_detail(rich_db, tid, mid, f) == _build_map_results_detail(rich_db, tid, mid, f)


# --- head_to_head -----------------------------------------------------------

PAIRS = [("GL", "DVS"), ("DVS", "OUG"), ("ELV", "ALU"), ("GL", "Q9")]


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("pair", PAIRS)
@pytest.mark.parametrize("map_name", MAPS)
def test_head_to_head(rich_db, f, pair, map_name):
    from cdm_stats.dashboard.tabs.head_to_head import _head_to_head
    a, b, mid = _tid(rich_db, pair[0]), _tid(rich_db, pair[1]), _mid(rich_db, map_name)
    assert qv.head_to_head(rich_db, a, b, mid, f) == _head_to_head(rich_db, a, b, mid, f)


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("abbr", TEAMS)
@pytest.mark.parametrize("map_name", MAPS)
def test_team_map_wl(rich_db, f, abbr, map_name):
    from cdm_stats.dashboard.tabs.head_to_head import _team_map_wl
    tid, mid = _tid(rich_db, abbr), _mid(rich_db, map_name)
    assert qv.team_map_wl(rich_db, tid, mid, f) == _team_map_wl(rich_db, tid, mid, f)


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("abbr", TEAMS)
@pytest.mark.parametrize("limit", [10, 1])
def test_recent_series(rich_db, f, abbr, limit):
    from cdm_stats.dashboard.tabs.head_to_head import _build_recent_series
    tid = _tid(rich_db, abbr)
    assert qv.recent_series(rich_db, tid, f, limit) == _build_recent_series(rich_db, tid, f, limit)


@pytest.mark.parametrize("f", FILTERS)
@pytest.mark.parametrize("pair", PAIRS)
def test_matchup_data(rich_db, f, pair):
    from cdm_stats.dashboard.tabs.head_to_head import _build_matchup_data
    a, b = _tid(rich_db, pair[0]), _tid(rich_db, pair[1])
    assert qv.matchup_data(rich_db, a, b, f) == _build_matchup_data(rich_db, a, b, f)


# --- scrim_performance -------------------------------------------------------

@pytest.mark.parametrize("mode", [None, "All", "SnD", "HP", "Control"])
def test_scrim_maps_for_mode(rich_db, monkeypatch, mode):
    import cdm_stats.dashboard.tabs.scrim_performance as sp

    class _NoCloseConn:
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        def close(self):
            pass

    callbacks = {}

    class _FakeApp:
        def callback(self, *a, **k):
            def deco(fn):
                callbacks[fn.__name__] = fn
                return fn
            return deco

    monkeypatch.setattr(sp, "get_db", lambda: _NoCloseConn(rich_db))
    sp.register_callbacks(_FakeApp())
    old = callbacks["update_map_options"](mode)
    new = qv.scrim_maps_for_mode(rich_db, mode)
    assert old == [{"label": "All", "value": "All"}] + [{"label": m, "value": m} for m in new]


# --- player_section ----------------------------------------------------------

def test_available_players(rich_db):
    from cdm_stats.dashboard.components.player_section import _get_available_players
    new = qv.available_players(rich_db)
    assert new == _get_available_players(rich_db)
    assert new == ["Alpha", "Zulu"]


def test_player_opponents(rich_db):
    from cdm_stats.dashboard.components.player_section import _get_opponents
    new = qv.player_opponents(rich_db)
    assert new == _get_opponents(rich_db)
    assert new == ["DVS", "OUG"]
