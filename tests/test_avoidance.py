import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.avoidance import (
    pick_win_loss,
    defend_win_loss,
    avoidance_index,
    target_index,
    pick_context_distribution,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# ATL picks Terminal (slot 1, SnD), wins 6-3
# LAT picks Highrise (slot 2, HP), LAT wins 250-220
# ATL picks Karachi Control (slot 3), ATL wins 3-1
# LAT picks Karachi SnD (slot 4), ATL wins 6-2
MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


def _get_ids(db):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    terminal = db.execute("SELECT map_id FROM maps WHERE map_name = 'Terminal' AND mode = 'SnD'").fetchone()[0]
    karachi_snd = db.execute("SELECT map_id FROM maps WHERE map_name = 'Karachi' AND mode = 'SnD'").fetchone()[0]
    highrise_hp = db.execute("SELECT map_id FROM maps WHERE map_name = 'Highrise' AND mode = 'HP'").fetchone()[0]
    return atl, lat, terminal, karachi_snd, highrise_hp


def test_pick_win_loss_atl_terminal(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, terminal, _, _ = _get_ids(db)
    result = pick_win_loss(db, atl, terminal)
    assert result == {"wins": 1, "losses": 0}


def test_defend_win_loss_lat_terminal(db):
    """LAT didn't pick Terminal, ATL did. LAT's defend record on Terminal."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    _, lat, terminal, _, _ = _get_ids(db)
    result = defend_win_loss(db, lat, terminal)
    assert result == {"wins": 0, "losses": 1}


def test_pick_win_loss_no_data(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, _, highrise_hp = _get_ids(db)
    result = pick_win_loss(db, atl, highrise_hp)
    assert result == {"wins": 0, "losses": 0}


def test_avoidance_index_basic(db):
    """ATL had 1 SnD pick opportunity (slot 1) and picked Terminal, not Karachi SnD.
    So ATL's avoidance of Karachi SnD = 1/1 = 100%."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, karachi_snd, _ = _get_ids(db)
    result = avoidance_index(db, atl, karachi_snd)
    assert result["ratio"] == 1.0
    assert result["opportunities"] == 1


def test_target_index_basic(db):
    """LAT is the opponent. LAT had 1 SnD pick (slot 4) against ATL and chose Karachi SnD.
    So target index for ATL on Karachi SnD = 0/1 = 0% (opponents DO pick it against ATL)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, _, karachi_snd, _ = _get_ids(db)
    result = target_index(db, atl, karachi_snd)
    assert result["ratio"] == 0.0
    assert result["opportunities"] == 1


def test_pick_context_distribution(db):
    """ATL picked Terminal in slot 1 (Opener context)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    atl, _, terminal, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, atl, terminal)
    assert dist["Opener"] == 1
    assert dist.get("Neutral", 0) == 0
    assert dist.get("Must-Win", 0) == 0
