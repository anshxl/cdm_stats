import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, SEED_ELO


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


def test_build_elo_traces(db):
    from cdm_stats.dashboard.tabs.elo_tracker import _build_elo_traces
    traces = _build_elo_traces(db)
    teams_with_data = {t["abbr"] for t in traces if len(t["elos"]) > 1}
    assert "DVS" in teams_with_data
    assert "OUG" in teams_with_data
    dvs_trace = next(t for t in traces if t["abbr"] == "DVS")
    assert dvs_trace["elos"][0] == SEED_ELO
    assert dvs_trace["elos"][1] > SEED_ELO
