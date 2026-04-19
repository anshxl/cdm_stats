import io
import sqlite3

import pytest

from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.playoff_loader import ingest_playoffs
from cdm_stats.metrics.elo import get_current_elo


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# ---- Bo5 fixtures ----

# Bo5 sweep, DVS won die roll, sweeps OUG 3-0.
# Picks: slot 1 = DVS (A), slot 2 = OUG (B), slot 3 = NULL (forced control).
BO5_SWEEP = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,3,Raid,DVS,4,1,,,"""

# Bo5 full 5-map, DVS won die roll, OUG wins 3-2.
# Picks: 1=DVS, 2=OUG, 3=NULL, 4=OUG, 5=DVS.
BO5_FULL = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,2,Summit,OUG,250,200,OUG,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,3,Raid,OUG,4,2,,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,4,Slums,DVS,6,4,OUG,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,5,Hacienda,OUG,250,230,DVS,OUG,"""

# ---- Bo7 fixtures ----

# Bo7 sweep, DVS won die roll, sweeps 4-0.
# Picks: 1=DVS, 2=OUG, 3=DVS, 4=OUG.
BO7_SWEEP = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-10,Grand Final,Bo7,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-10,Grand Final,Bo7,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-10,Grand Final,Bo7,DVS,OUG,DVS,3,Raid,DVS,4,1,DVS,,
2026-05-10,Grand Final,Bo7,DVS,OUG,DVS,4,Slums,DVS,6,2,OUG,,"""

# Bo7 full 7-map, DVS won die roll, DVS wins 4-3.
# Picks: 1=DVS, 2=OUG, 3=DVS, 4=OUG, 5=DVS, 6=OUG, 7=DVS.
BO7_FULL = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,2,Summit,OUG,250,200,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,3,Raid,OUG,4,2,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,4,Slums,DVS,6,4,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,5,Hacienda,OUG,250,230,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,6,Standoff,DVS,4,3,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,7,Firing Range,DVS,6,5,DVS,DVS,"""


# ---- Happy-path tests ----

def test_bo5_sweep_loads(db):
    results = ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    assert len(results) == 1 and results[0]["status"] == "ok"
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 3


def test_bo5_sweep_match_format_is_playoff_bo5(db):
    ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    fmt = db.execute("SELECT match_format FROM matches").fetchone()[0]
    assert fmt == "CDL_PLAYOFF_BO5"


def test_bo5_sweep_round_stored(db):
    ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    rnd = db.execute("SELECT round FROM matches").fetchone()[0]
    assert rnd == "Upper QF"


def test_bo5_sweep_die_roll_winner_in_two_v_two(db):
    ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    winner = db.execute("SELECT two_v_two_winner_id FROM matches").fetchone()[0]
    assert winner == dvs_id


def test_bo5_full_picks_match_pattern(db):
    """Bo5 5-map: pickers should be A, B, NULL, B, A given DVS=A."""
    ingest_playoffs(db, io.StringIO(BO5_FULL))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='OUG'").fetchone()[0]
    rows = db.execute(
        "SELECT slot, picked_by_team_id FROM map_results ORDER BY slot"
    ).fetchall()
    assert rows == [(1, dvs_id), (2, oug_id), (3, None), (4, oug_id), (5, dvs_id)]


def test_bo5_slot3_pick_context_is_coin_toss(db):
    ingest_playoffs(db, io.StringIO(BO5_FULL))
    ctx = db.execute("SELECT pick_context FROM map_results WHERE slot=3").fetchone()[0]
    assert ctx == "Coin-Toss"


def test_bo5_full_series_winner_override(db):
    """OUG wins 3-2 via the explicit series_winner override on the final row."""
    ingest_playoffs(db, io.StringIO(BO5_FULL))
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='OUG'").fetchone()[0]
    winner = db.execute("SELECT series_winner_id FROM matches").fetchone()[0]
    assert winner == oug_id


def test_bo7_sweep_loads(db):
    results = ingest_playoffs(db, io.StringIO(BO7_SWEEP))
    assert len(results) == 1 and results[0]["status"] == "ok"
    fmt = db.execute("SELECT match_format FROM matches").fetchone()[0]
    assert fmt == "CDL_PLAYOFF_BO7"
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 4


def test_bo7_full_picks_match_pattern(db):
    """Bo7 7-map: pickers alternate A,B,A,B,A,B,A given DVS=A."""
    ingest_playoffs(db, io.StringIO(BO7_FULL))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='OUG'").fetchone()[0]
    rows = db.execute(
        "SELECT slot, picked_by_team_id FROM map_results ORDER BY slot"
    ).fetchall()
    assert rows == [
        (1, dvs_id), (2, oug_id), (3, dvs_id), (4, oug_id),
        (5, dvs_id), (6, oug_id), (7, dvs_id),
    ]


def test_playoff_match_uses_k40(db):
    """A playoff sweep should produce K=40 Elo deltas (vs K=32 for regular season)."""
    ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    # Sanity: Elo moved by more than the K=32 maximum (which would be 0.5*32 = 16 max)
    # Actual value depends on margin weighting, but should clearly exceed K=32 ceiling.
    delta = dvs_elo - 1000
    assert delta > 16  # > K=32 max possible swing


# ---- Validation failure tests ----

INVALID_PICK_PATTERN = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,OUG,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,3,Raid,DVS,4,1,,,"""


def test_invalid_pick_pattern_returns_error(db):
    """Slot 1 must be picked by die-roll winner."""
    results = ingest_playoffs(db, io.StringIO(INVALID_PICK_PATTERN))
    assert results[0]["status"] == "error"
    assert any("pick" in e.lower() for e in results[0]["errors"])
    # Nothing inserted
    assert db.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0


BO5_SLOT3_HAS_PICKER = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,3,Raid,DVS,4,1,DVS,,"""


def test_bo5_slot3_with_picker_returns_error(db):
    results = ingest_playoffs(db, io.StringIO(BO5_SLOT3_HAS_PICKER))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0


INCONSISTENT_ROUND = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper SF,Bo5,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,3,Raid,DVS,4,1,,,"""


def test_inconsistent_series_columns_returns_error(db):
    results = ingest_playoffs(db, io.StringIO(INCONSISTENT_ROUND))
    assert results[0]["status"] == "error"
    assert any("round" in e.lower() or "consistent" in e.lower() for e in results[0]["errors"])


SLOT_COUNT_MISMATCH = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,"""


def test_slot_count_no_winner_returns_error(db):
    """Bo5 with only 2 maps: no team reached 3 wins."""
    results = ingest_playoffs(db, io.StringIO(SLOT_COUNT_MISMATCH))
    assert results[0]["status"] == "error"


INVALID_FORMAT = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo9,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper QF,Bo9,DVS,OUG,DVS,2,Summit,DVS,250,200,OUG,,
2026-05-01,Upper QF,Bo9,DVS,OUG,DVS,3,Raid,DVS,4,1,,,"""


def test_invalid_format_returns_error(db):
    results = ingest_playoffs(db, io.StringIO(INVALID_FORMAT))
    assert results[0]["status"] == "error"
    assert any("format" in e.lower() for e in results[0]["errors"])


# ---- DQ ----

BO5_WITH_DQ = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,2,Summit,OUG,250,200,OUG,,1
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,3,Raid,DVS,4,1,,,
2026-05-01,Upper QF,Bo5,DVS,OUG,DVS,4,Slums,DVS,6,2,OUG,,"""


def test_dq_flag_persists(db):
    ingest_playoffs(db, io.StringIO(BO5_WITH_DQ))
    rows = db.execute("SELECT slot, dq FROM map_results ORDER BY slot").fetchall()
    assert rows == [(1, 0), (2, 1), (3, 0), (4, 0)]


# ---- Duplicate handling ----

def test_duplicate_match_skipped(db):
    ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    results = ingest_playoffs(db, io.StringIO(BO5_SWEEP))
    assert results[0]["status"] == "skipped"
    assert db.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1
