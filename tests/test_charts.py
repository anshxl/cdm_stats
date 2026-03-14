import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo
from cdm_stats.charts.heatmap import chart_avoidance_target, chart_elo_trajectory


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
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    # Note: Elo update may already be done by ingest_csv (which now auto-calls update_elo).
    # Check if there are already elo rows before calling update_elo again.
    elo_count = conn.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    if elo_count == 0:
        update_elo(conn, match_id)
    yield conn
    conn.close()


def test_chart_avoidance_target_creates_file(db, tmp_path):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    output_path = tmp_path / "heatmap_ATL.png"
    chart_avoidance_target(db, atl, str(output_path))
    assert output_path.exists()


def test_chart_elo_trajectory_creates_file(db, tmp_path):
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    output_path = tmp_path / "elo_ATL.png"
    chart_elo_trajectory(db, atl, str(output_path))
    assert output_path.exists()
