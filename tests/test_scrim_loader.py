import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-10,1,DVS,Raid,Control,3-1,W
2026-03-10,1,OUG,Hacienda,HP,180-250,L"""


def test_ingest_scrims_team_basic(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    results = ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    assert len(results) == 4
    assert all(r["status"] == "ok" for r in results)

    rows = db.execute("SELECT * FROM scrim_maps").fetchall()
    assert len(rows) == 4


def test_ingest_scrims_team_scores(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))

    row = db.execute(
        "SELECT our_score, opponent_score, result FROM scrim_maps WHERE map_name = 'Tunisia'"
    ).fetchone()
    assert row == (6, 3, "W")

    row = db.execute(
        "SELECT our_score, opponent_score, result FROM scrim_maps WHERE map_name = 'Hacienda'"
    ).fetchone()
    assert row == (180, 250, "L")


def test_ingest_scrims_team_opponent_fk(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))

    row = db.execute(
        """SELECT t.abbreviation FROM scrim_maps s
           JOIN teams t ON s.opponent_id = t.team_id
           WHERE s.map_name = 'Tunisia'"""
    ).fetchone()
    assert row[0] == "DVS"


def test_ingest_scrims_team_idempotent(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    results = ingest_scrims_team(db, io.StringIO(TEAM_CSV))
    assert all(r["status"] == "skipped" for r in results)

    rows = db.execute("SELECT * FROM scrim_maps").fetchall()
    assert len(rows) == 4


def test_ingest_scrims_team_game_number(db):
    """Same map+mode+opponent+date played twice gets sequential game_numbers."""
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Tunisia,SnD,4-6,L"""
    ingest_scrims_team(db, io.StringIO(csv))

    rows = db.execute(
        "SELECT game_number, our_score, result FROM scrim_maps ORDER BY game_number"
    ).fetchall()
    assert rows == [(1, 6, "W"), (2, 4, "L")]


def test_ingest_scrims_team_bad_opponent(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,BADTEAM,Tunisia,SnD,6-3,W"""
    results = ingest_scrims_team(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert "opponent" in results[0]["errors"].lower()


def test_ingest_scrims_team_score_result_mismatch(db):
    from cdm_stats.ingestion.scrim_loader import ingest_scrims_team
    csv = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,3-6,W"""
    results = ingest_scrims_team(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert "result" in results[0]["errors"].lower() or "score" in results[0]["errors"].lower()
