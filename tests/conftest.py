# tests/conftest.py
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    """Fresh in-memory DB with schema and seed data."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    """DB with one ingested match (DVS 3-1 OUG)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id
