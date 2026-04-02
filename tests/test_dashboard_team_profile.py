import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_build_map_record_data(db):
    from cdm_stats.dashboard.tabs.team_profile import _build_map_record_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    records = _build_map_record_data(db, dvs_id)
    played_maps = {r["map_name"] for r in records if r["wins"] + r["losses"] > 0}
    assert "Tunisia" in played_maps
    assert "Summit" in played_maps
    tunisia = next(r for r in records if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 0
    assert "pick_wins" in tunisia
    assert "defend_wins" in tunisia


def test_build_avoidance_target_data(db):
    from cdm_stats.dashboard.tabs.team_profile import _build_avoidance_target_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    data = _build_avoidance_target_data(db, dvs_id)
    assert len(data) > 0
    assert "map_name" in data[0]
    assert "avoid_ratio" in data[0]
    assert "target_ratio" in data[0]
    assert "avoid_n" in data[0]
    assert "target_n" in data[0]
