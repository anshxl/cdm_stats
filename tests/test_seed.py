import sqlite3
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps, TEAMS, MAPS


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


def test_seed_teams_inserts_14_teams(db):
    seed_teams(db)
    cursor = db.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 14


def test_seed_teams_abbreviations_are_unique(db):
    seed_teams(db)
    cursor = db.execute("SELECT abbreviation FROM teams ORDER BY abbreviation")
    abbrs = [row[0] for row in cursor.fetchall()]
    assert len(abbrs) == len(set(abbrs))


def test_seed_maps_inserts_13_maps(db):
    seed_maps(db)
    cursor = db.execute("SELECT COUNT(*) FROM maps")
    assert cursor.fetchone()[0] == 13


def test_seed_maps_correct_mode_counts(db):
    seed_maps(db)
    cursor = db.execute("SELECT mode, COUNT(*) FROM maps GROUP BY mode ORDER BY mode")
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    assert counts == {"Control": 3, "HP": 5, "SnD": 5}


def test_seed_is_idempotent(db):
    seed_teams(db)
    seed_teams(db)  # should not raise or duplicate
    cursor = db.execute("SELECT COUNT(*) FROM teams")
    assert cursor.fetchone()[0] == 14
