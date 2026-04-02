import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo, get_elo_history, normalize_margin, MODE_MAX_MARGINS

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


def test_mode_max_margins_values():
    assert MODE_MAX_MARGINS == {"SnD": 9, "HP": 250, "Control": 4}


def test_normalize_margin_snd():
    # 6-3 SnD = margin 3, normalized = 3/9 = 0.333...
    result = normalize_margin(6, 3, "SnD")
    assert abs(result - 3 / 9) < 0.001


def test_normalize_margin_hp():
    # 250-80 HP = margin 170, normalized = 170/250 = 0.68
    result = normalize_margin(250, 80, "HP")
    assert abs(result - 170 / 250) < 0.001


def test_normalize_margin_control():
    # 4-0 Control = margin 4, normalized = 4/4 = 1.0
    result = normalize_margin(4, 0, "Control")
    assert result == 1.0


def test_normalize_margin_close_game():
    # 250-248 HP = margin 2, normalized = 2/250 = 0.008
    result = normalize_margin(250, 248, "HP")
    assert abs(result - 2 / 250) < 0.001


BLOWOUT_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-02-10,DVS,ELV,DVS,1,Tunisia,DVS,9,0
2026-02-10,DVS,ELV,DVS,2,Summit,DVS,250,80
2026-02-10,DVS,ELV,DVS,3,Raid,DVS,4,0"""

CLOSE_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-02-15,DVS,Q9,DVS,1,Tunisia,DVS,6,5
2026-02-15,DVS,Q9,DVS,2,Summit,DVS,250,248
2026-02-15,DVS,Q9,DVS,3,Raid,DVS,4,3"""


def test_blowout_moves_elo_more_than_close_win(db):
    """A 3-0 blowout should produce a larger Elo gain than a 3-0 nail-biter."""
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]

    # Ingest blowout match
    ingest_csv(db, io.StringIO(BLOWOUT_CSV))
    blowout_elo = get_current_elo(db, dvs_id)
    blowout_gain = blowout_elo - 1000

    # Reset: delete elo rows for clean comparison
    db.execute("DELETE FROM team_elo")
    db.execute("DELETE FROM map_results")
    db.execute("DELETE FROM matches")

    # Ingest close match
    ingest_csv(db, io.StringIO(CLOSE_CSV))
    close_elo = get_current_elo(db, dvs_id)
    close_gain = close_elo - 1000

    assert blowout_gain > close_gain
    assert blowout_gain > 0
    assert close_gain > 0


def test_elo_winner_always_gains(db):
    """Even when losses are more lopsided than wins, the series winner should not lose Elo."""
    # DVS wins 3-2 but loses maps by larger margins than wins
    mixed_csv = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-03-01,DVS,OUG,DVS,1,Tunisia,DVS,6,5
2026-03-01,DVS,OUG,DVS,2,Summit,OUG,250,100
2026-03-01,DVS,OUG,DVS,3,Raid,DVS,4,3
2026-03-01,DVS,OUG,DVS,4,Slums,OUG,9,1
2026-03-01,DVS,OUG,DVS,5,Hacienda,DVS,250,240"""
    ingest_csv(db, io.StringIO(mixed_csv))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    assert dvs_elo >= 1000  # must not lose Elo for winning
