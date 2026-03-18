import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def raw_db():
    """Fresh in-memory DB with OLD schema (pre-migration) — no seeds, no migrate."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    yield conn
    conn.close()


def test_create_tables_creates_all_tables(db):
    create_tables(db)
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_bans", "map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]


def test_create_tables_is_idempotent(db):
    create_tables(db)
    create_tables(db)  # should not raise
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == ["map_bans", "map_results", "maps", "matches", "team_elo", "team_map_notes", "teams"]


def test_migrate_adds_match_format_column(raw_db):
    """After migration, matches table has match_format column."""
    migrate(raw_db)
    row = raw_db.execute("PRAGMA table_info(matches)").fetchall()
    col_names = [r[1] for r in row]
    assert "match_format" in col_names


def test_migrate_adds_map_bans_table(raw_db):
    """After migration, map_bans table exists."""
    migrate(raw_db)
    tables = [r[0] for r in raw_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    assert "map_bans" in tables


def test_migrate_allows_slot_7(raw_db):
    """After migration, map_results accepts slot values up to 7."""
    migrate(raw_db)
    info = raw_db.execute("SELECT sql FROM sqlite_master WHERE name='map_results'").fetchone()[0]
    assert "BETWEEN 1 AND 7" in info


def test_migrate_allows_nullable_two_v_two_winner(raw_db):
    """After migration, two_v_two_winner_id can be NULL."""
    migrate(raw_db)
    info = raw_db.execute("PRAGMA table_info(matches)").fetchall()
    two_v_two_col = [r for r in info if r[1] == "two_v_two_winner_id"][0]
    assert two_v_two_col[3] == 0  # notnull = 0 means nullable


def test_migrate_preserves_existing_data(raw_db):
    """Migration preserves existing match and map_results rows."""
    from cdm_stats.ingestion.seed import seed_teams, seed_maps
    seed_teams(raw_db)
    seed_maps(raw_db)
    raw_db.execute(
        "INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id, series_winner_id) "
        "VALUES ('2026-01-01', 1, 2, 1, 1)"
    )
    raw_db.execute(
        "INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id, "
        "picking_team_score, non_picking_team_score, team1_score_before, team2_score_before, pick_context) "
        "VALUES (1, 1, 1, 1, 1, 6, 3, 0, 0, 'Opener')"
    )
    raw_db.commit()
    migrate(raw_db)
    match = raw_db.execute("SELECT match_format FROM matches WHERE match_id = 1").fetchone()
    assert match[0] == "CDL_BO5"
    mr = raw_db.execute("SELECT slot FROM map_results WHERE match_id = 1").fetchone()
    assert mr[0] == 1


def test_migrate_is_idempotent(raw_db):
    """Running migrate twice does not error or duplicate data."""
    migrate(raw_db)
    migrate(raw_db)  # should not raise
