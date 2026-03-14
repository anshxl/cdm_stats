import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo
from cdm_stats.charts.heatmap import chart_avoidance_target, chart_elo_trajectory


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
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    yield conn
    conn.close()


def test_chart_avoidance_target_creates_file(db, tmp_path):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    output_path = tmp_path / "heatmap_DVS.png"
    chart_avoidance_target(db, dvs, str(output_path))
    assert output_path.exists()


def test_chart_elo_trajectory_creates_file(db, tmp_path):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    output_path = tmp_path / "elo_DVS.png"
    chart_elo_trajectory(db, dvs, str(output_path))
    assert output_path.exists()
