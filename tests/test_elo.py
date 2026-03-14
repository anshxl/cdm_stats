import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo, get_elo_history

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id


def test_update_elo_inserts_two_rows(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 2


def test_elo_winner_goes_up_loser_goes_down(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    atl_elo = get_current_elo(db, atl_id)
    lat_elo = get_current_elo(db, lat_id)
    assert atl_elo > 1000  # winner
    assert lat_elo < 1000  # loser


def test_elo_changes_sum_to_zero(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    lat_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'LAT'").fetchone()[0]
    atl_elo = get_current_elo(db, atl_id)
    lat_elo = get_current_elo(db, lat_id)
    assert abs((atl_elo - 1000) + (lat_elo - 1000)) < 0.001


def test_get_current_elo_no_matches(db):
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    assert get_current_elo(db, atl_id) == 1000.0


def test_get_elo_history(db_with_match):
    db, match_id = db_with_match
    update_elo(db, match_id)
    atl_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    history = get_elo_history(db, atl_id)
    assert len(history) == 1
    assert history[0]["elo_after"] > 1000
