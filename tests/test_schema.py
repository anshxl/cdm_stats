import sqlite3
import pytest
from cdm_stats.db.schema import create_tables


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


def test_create_tables_creates_all_tables(db):
    create_tables(db)
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]


def test_create_tables_is_idempotent(db):
    create_tables(db)
    create_tables(db)  # should not raise
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]
