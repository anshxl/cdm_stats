import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo
from cdm_stats.ingestion.backfill import backfill_elo

TWO_MATCHES_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,ATL,LAT,ATL,1,Terminal,ATL,6,3
2026-01-15,ATL,LAT,ATL,2,Highrise,LAT,250,220
2026-01-15,ATL,LAT,ATL,3,Karachi,ATL,3,1
2026-01-15,ATL,LAT,ATL,4,Karachi,ATL,6,2
2026-01-16,BOS,SEA,BOS,1,Invasion,BOS,6,4
2026-01-16,BOS,SEA,BOS,2,Karachi,SEA,250,200
2026-01-16,BOS,SEA,BOS,3,Highrise,BOS,3,2
2026-01-16,BOS,SEA,BOS,4,Skidrow,SEA,6,3
2026-01-16,BOS,SEA,BOS,5,Invasion,BOS,250,230"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(TWO_MATCHES_CSV))
    yield conn
    conn.close()


def test_backfill_elo_populates_all_teams(db):
    backfill_elo(db)
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 4  # 2 matches × 2 teams each


def test_backfill_elo_is_idempotent(db):
    backfill_elo(db)
    atl = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ATL'").fetchone()[0]
    elo_first = get_current_elo(db, atl)
    backfill_elo(db)
    elo_second = get_current_elo(db, atl)
    assert elo_first == elo_second


def test_backfill_elo_chronological_order(db):
    backfill_elo(db)
    rows = db.execute("SELECT match_date FROM team_elo ORDER BY elo_id").fetchall()
    dates = [r[0] for r in rows]
    assert dates == sorted(dates)
