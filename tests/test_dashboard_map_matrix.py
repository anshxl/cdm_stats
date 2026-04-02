import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


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
    yield conn
    conn.close()


def test_build_matrix_data(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db)
    assert len(teams) > 0
    assert len(maps) > 0
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia_id = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia'").fetchone()[0]
    cell = matrix.get((dvs_id, tunisia_id))
    assert cell is not None
    assert cell["wins"] == 1
    assert cell["losses"] == 0


def test_build_matrix_data_mode_filter(db):
    from cdm_stats.dashboard.tabs.map_matrix import _build_matrix_data
    teams, maps, matrix = _build_matrix_data(db, mode_filter="SnD")
    for _, _, mode in maps:
        assert mode == "SnD"
