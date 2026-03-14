import sqlite3
import csv
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


FOUR_MAP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""

SWEEP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-14,MIN,TOR,MIN,1,Terminal,MIN,6,2
2026-01-14,MIN,TOR,MIN,2,Highrise,MIN,250,180
2026-01-14,MIN,TOR,MIN,3,Karachi,MIN,3,0"""

FIVE_MAP_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-16,BOS,SEA,BOS,1,Invasion,BOS,6,4
2026-01-16,BOS,SEA,BOS,2,Karachi,SEA,250,200
2026-01-16,BOS,SEA,BOS,3,Highrise,BOS,3,2
2026-01-16,BOS,SEA,BOS,4,Skidrow,SEA,6,3
2026-01-16,BOS,SEA,BOS,5,Invasion,BOS,250,230"""

INVALID_TEAM_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-17,FAKE,LAT,FAKE,1,Terminal,FAKE,6,3
2026-01-17,FAKE,LAT,FAKE,2,Highrise,LAT,250,220
2026-01-17,FAKE,LAT,FAKE,3,Karachi,FAKE,3,1"""

INVALID_NO_WINNER_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-17,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-17,ATL,LAT,ATL,2,Highrise,LAT,250,220"""


def test_ingest_sweep_3_0_creates_3_map_results(db):
    """A 3-0 sweep should produce exactly 3 map_results rows."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 3


def test_ingest_sweep_3_0_winner_has_one_pick(db):
    """In a 3-0 sweep, the sweeping team (2v2 winner) has exactly 1 pick (slot 1)."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    min_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'MIN'").fetchone()[0]
    picks = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id = ?", (min_id,)
    ).fetchone()[0]
    assert picks == 1


def test_ingest_sweep_3_0_swept_team_has_two_picks(db):
    """In a 3-0 where MIN won 2v2 and all 3 maps, TOR (loser of each map) picks slots 2 and 3."""
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    tor_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'TOR'").fetchone()[0]
    picks = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id = ?", (tor_id,)
    ).fetchone()[0]
    assert picks == 2


def test_ingest_sweep_3_0_series_winner_correct(db):
    ingest_csv(db, io.StringIO(SWEEP_CSV))
    min_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'MIN'").fetchone()[0]
    winner = db.execute("SELECT series_winner_id FROM matches").fetchone()[0]
    assert winner == min_id


def test_ingest_invalid_team_returns_error(db):
    results = ingest_csv(db, io.StringIO(INVALID_TEAM_CSV))
    assert results[0]["status"] == "error"
    assert any("FAKE" in e for e in results[0]["errors"])


def test_ingest_no_winner_returns_error(db):
    """Only 2 maps played, no team reaches 3 wins."""
    results = ingest_csv(db, io.StringIO(INVALID_NO_WINNER_CSV))
    assert results[0]["status"] == "error"
    assert any("3 wins" in e for e in results[0]["errors"])


def test_ingest_sweep_creates_match(db):
    results = ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    assert len(results) == 1
    match = db.execute("SELECT * FROM matches").fetchone()
    assert match is not None


def test_ingest_sweep_creates_4_map_results(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 4


def test_ingest_sweep_series_winner_is_correct(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    match = db.execute("SELECT series_winner_id FROM matches").fetchone()
    atl_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'ATL'"
    ).fetchone()[0]
    assert match[0] == atl_id


def test_ingest_sweep_slot1_pick_context_is_opener(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    ctx = db.execute(
        "SELECT pick_context FROM map_results WHERE slot = 1"
    ).fetchone()[0]
    assert ctx == "Opener"


def test_ingest_sweep_picked_by_slot1_is_2v2_winner(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    atl_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'ATL'"
    ).fetchone()[0]
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 1"
    ).fetchone()[0]
    assert picker == atl_id


def test_ingest_sweep_slot2_picked_by_loser_of_slot1(db):
    """ATL won slot 1, so LAT (loser) picks slot 2."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    lat_id = db.execute(
        "SELECT team_id FROM teams WHERE abbreviation = 'LAT'"
    ).fetchone()[0]
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 2"
    ).fetchone()[0]
    assert picker == lat_id


def test_ingest_sweep_series_scores_accumulate(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    rows = db.execute(
        "SELECT slot, team1_score_before, team2_score_before FROM map_results ORDER BY slot"
    ).fetchall()
    # Slot 1: 0-0, Slot 2: 1-0 (ATL won s1), Slot 3: 1-1 (LAT won s2), Slot 4: 2-1 (ATL won s3)
    assert rows[0][1:] == (0, 0)  # slot 1
    assert rows[1][1:] == (1, 0)  # slot 2
    assert rows[2][1:] == (1, 1)  # slot 3
    assert rows[3][1:] == (2, 1)  # slot 4


def test_ingest_five_map_slot5_picked_by_is_null(db):
    ingest_csv(db, io.StringIO(FIVE_MAP_CSV))
    picker = db.execute(
        "SELECT picked_by_team_id FROM map_results WHERE slot = 5"
    ).fetchone()[0]
    assert picker is None


def test_ingest_five_map_slot5_is_coin_toss(db):
    ingest_csv(db, io.StringIO(FIVE_MAP_CSV))
    ctx = db.execute(
        "SELECT pick_context FROM map_results WHERE slot = 5"
    ).fetchone()[0]
    assert ctx == "Coin-Toss"


def test_ingest_duplicate_match_is_rejected(db):
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    results = ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    assert len(results) == 1
    assert results[0]["status"] == "skipped"
    count = db.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    assert count == 1


def test_ingest_picking_team_score_when_picker_wins(db):
    """Slot 1: ATL picked, ATL won 6-3. picking_team_score=6, non=3."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (6, 3)


def test_ingest_picking_team_score_when_picker_loses(db):
    """Slot 2: LAT picked, LAT won 250-220. picking_team_score=250, non=220."""
    ingest_csv(db, io.StringIO(FOUR_MAP_CSV))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 2"
    ).fetchone()
    assert row == (250, 220)
