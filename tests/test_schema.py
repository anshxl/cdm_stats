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
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("""CREATE TABLE teams (
        team_id INTEGER PRIMARY KEY, team_name TEXT NOT NULL, abbreviation TEXT NOT NULL UNIQUE)""")
    conn.execute("""CREATE TABLE maps (
        map_id INTEGER PRIMARY KEY, map_name TEXT NOT NULL,
        mode TEXT NOT NULL CHECK(mode IN ('SnD', 'HP', 'Control')), UNIQUE(map_name, mode))""")
    conn.execute("""CREATE TABLE matches (
        match_id INTEGER PRIMARY KEY, match_date DATE NOT NULL,
        team1_id INTEGER NOT NULL REFERENCES teams(team_id),
        team2_id INTEGER NOT NULL REFERENCES teams(team_id),
        two_v_two_winner_id INTEGER NOT NULL REFERENCES teams(team_id),
        series_winner_id INTEGER NOT NULL REFERENCES teams(team_id),
        CHECK(team1_id != team2_id))""")
    conn.execute("""CREATE TABLE map_results (
        result_id INTEGER PRIMARY KEY,
        match_id INTEGER NOT NULL REFERENCES matches(match_id),
        slot INTEGER NOT NULL CHECK(slot BETWEEN 1 AND 5),
        map_id INTEGER NOT NULL REFERENCES maps(map_id),
        picked_by_team_id INTEGER REFERENCES teams(team_id),
        winner_team_id INTEGER NOT NULL REFERENCES teams(team_id),
        picking_team_score INTEGER NOT NULL, non_picking_team_score INTEGER NOT NULL,
        team1_score_before INTEGER NOT NULL, team2_score_before INTEGER NOT NULL,
        pick_context TEXT NOT NULL CHECK(pick_context IN ('Opener','Neutral','Must-Win','Close-Out','Coin-Toss')),
        UNIQUE(match_id, slot))""")
    conn.execute("""CREATE TABLE team_elo (
        elo_id INTEGER PRIMARY KEY, team_id INTEGER NOT NULL REFERENCES teams(team_id),
        match_id INTEGER NOT NULL REFERENCES matches(match_id),
        elo_after REAL NOT NULL, match_date DATE NOT NULL, UNIQUE(team_id, match_id))""")
    conn.execute("""CREATE TABLE team_map_notes (
        note_id INTEGER PRIMARY KEY, team_id INTEGER NOT NULL REFERENCES teams(team_id),
        map_id INTEGER NOT NULL REFERENCES maps(map_id),
        note TEXT NOT NULL, created_at DATE NOT NULL)""")
    conn.commit()
    yield conn
    conn.close()


def test_create_tables_creates_all_tables(db):
    create_tables(db)
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == [
        "map_bans", "map_results", "maps", "matches",
        "scrim_maps", "scrim_player_stats",
        "team_elo", "team_map_notes", "teams",
        "tournament_player_stats",
    ]


def test_create_tables_is_idempotent(db):
    create_tables(db)
    create_tables(db)  # should not raise
    cursor = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert tables == [
        "map_bans", "map_results", "maps", "matches",
        "scrim_maps", "scrim_player_stats",
        "team_elo", "team_map_notes", "teams",
        "tournament_player_stats",
    ]


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


def test_scrim_maps_table_exists(db):
    """scrim_maps table should be created by create_tables."""
    create_tables(db)
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='scrim_maps'"
    ).fetchall()
    assert len(rows) == 1


def test_scrim_player_stats_table_exists(db):
    """scrim_player_stats table should be created by create_tables."""
    create_tables(db)
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='scrim_player_stats'"
    ).fetchall()
    assert len(rows) == 1


def test_tournament_player_stats_table_exists():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(tournament_player_stats)").fetchall()]
    assert cols == ["stat_id", "result_id", "week", "player_name", "kills", "deaths", "assists"]

    # Unique (result_id, player_name)
    idx_rows = conn.execute("PRAGMA index_list(tournament_player_stats)").fetchall()
    unique_indexes = [r for r in idx_rows if r[2] == 1]
    assert len(unique_indexes) >= 1
    conn.close()


def test_schema_version_is_5():
    import sqlite3
    from cdm_stats.db.schema import create_tables, SCHEMA_VERSION

    assert SCHEMA_VERSION == 5
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 5
    conn.close()


def test_map_results_has_dq_column():
    import sqlite3
    from cdm_stats.db.schema import create_tables

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
    assert "dq" in cols
    conn.close()


def test_migration_v4_to_v5_adds_dq_column():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    conn.execute("ALTER TABLE map_results DROP COLUMN dq")
    conn.execute("PRAGMA user_version = 4")
    conn.commit()

    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(map_results)").fetchall()]
    assert "dq" in cols
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 5
    conn.close()


def test_migration_v3_to_v4_adds_tournament_player_stats():
    import sqlite3
    from cdm_stats.db.schema import create_tables, migrate, SCHEMA_VERSION

    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    # Simulate old DB at v3
    conn.execute("DROP TABLE tournament_player_stats")
    conn.execute("PRAGMA user_version = 3")
    conn.commit()

    migrate(conn)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(tournament_player_stats)").fetchall()]
    assert "result_id" in cols
    assert "week" in cols
    assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    conn.close()
