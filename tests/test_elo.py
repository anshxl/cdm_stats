import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo, get_elo_history

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


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
    # ingest_csv already calls update_elo, so rows should exist
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 2


def test_elo_winner_goes_up_loser_goes_down(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    oug_elo = get_current_elo(db, oug_id)
    assert dvs_elo > 1000  # winner
    assert oug_elo < 1000  # loser


def test_elo_changes_sum_to_zero(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    oug_elo = get_current_elo(db, oug_id)
    assert abs((dvs_elo - 1000) + (oug_elo - 1000)) < 0.001


def test_get_current_elo_no_matches(db):
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    assert get_current_elo(db, dvs_id) == 1000.0


def test_get_elo_history(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    history = get_elo_history(db, dvs_id)
    assert len(history) == 1
    assert history[0]["elo_after"] > 1000
