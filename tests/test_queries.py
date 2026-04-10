import sqlite3
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


def test_get_team_id_by_abbr(db):
    team_id = get_team_id_by_abbr(db, "DVS")
    assert team_id is not None
    assert isinstance(team_id, int)


def test_get_team_id_by_abbr_invalid(db):
    assert get_team_id_by_abbr(db, "INVALID") is None


def test_get_map_id(db):
    map_id = get_map_id(db, "Tunisia", "SnD")
    assert map_id is not None


def test_get_map_id_wrong_mode(db):
    assert get_map_id(db, "Tunisia", "HP") is None


def test_get_mode_for_slot():
    assert get_mode_for_slot(1) == "SnD"
    assert get_mode_for_slot(2) == "HP"
    assert get_mode_for_slot(3) == "Control"
    assert get_mode_for_slot(4) == "SnD"
    assert get_mode_for_slot(5) == "HP"


def test_insert_match(db):
    atl = get_team_id_by_abbr(db, "DVS")
    lat = get_team_id_by_abbr(db, "OUG")
    match_id = insert_match(db, "2026-01-15", atl, lat, atl, atl)
    assert match_id is not None
    row = db.execute("SELECT * FROM matches WHERE match_id = ?", (match_id,)).fetchone()
    assert row is not None


def test_insert_map_result_stores_dq(db):
    from cdm_stats.db.queries import insert_match, insert_map_result
    dvs = get_team_id_by_abbr(db, "DVS")
    oug = get_team_id_by_abbr(db, "OUG")
    tunisia = get_map_id(db, "Tunisia", "SnD")

    match_id = insert_match(db, "2026-01-15", dvs, oug, dvs, dvs)
    insert_map_result(db, match_id, 1, tunisia, dvs, dvs, 6, 3, 0, 0, "Opener", dq=1)

    row = db.execute(
        "SELECT dq FROM map_results WHERE match_id = ? AND slot = 1", (match_id,)
    ).fetchone()
    assert row[0] == 1


def test_insert_map_result_dq_defaults_to_zero(db):
    from cdm_stats.db.queries import insert_match, insert_map_result
    dvs = get_team_id_by_abbr(db, "DVS")
    oug = get_team_id_by_abbr(db, "OUG")
    tunisia = get_map_id(db, "Tunisia", "SnD")

    match_id = insert_match(db, "2026-01-15", dvs, oug, dvs, dvs)
    insert_map_result(db, match_id, 1, tunisia, dvs, dvs, 6, 3, 0, 0, "Opener")

    row = db.execute(
        "SELECT dq FROM map_results WHERE match_id = ? AND slot = 1", (match_id,)
    ).fetchone()
    assert row[0] == 0
