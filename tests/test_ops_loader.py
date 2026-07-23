import io
import sqlite3
from datetime import date as _date
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.ops_loader import ingest_ops_kills


OPS_CSV = """Date,Week,Opponent,Map,Player,OpKills,OpPulls,FootageMin,OpKillsPerMin,OpKillsPerPull
2026-02-15,1,OUG,Tunisia,Alpha,4,4,11.83,0.338,1.000
2026-02-15,1,OUG,Tunisia,Bravo,5,3,11.83,0.423,1.667
2026-02-15,1,OUG,Summit,Alpha,9,4,9.47,0.951,2.250
2026-02-15,1,OUG,Summit,Bravo,0,2,9.47,0.000,0.000"""


@pytest.fixture
def db_with_match():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)

    dvs_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number)
           VALUES ('2026-02-15', ?, ?, ?, ?, 'CDL_BO5', 1)""",
        (dvs_id, oug_id, dvs_id, dvs_id),
    )
    match_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    tunisia_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'"
    ).fetchone()[0]
    summit_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'"
    ).fetchone()[0]

    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 1, ?, ?, ?, 6, 3, 0, 0, 'Opener')""",
        (match_id, tunisia_id, dvs_id, dvs_id),
    )
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 2, ?, ?, ?, 250, 200, 1, 0, 'Neutral')""",
        (match_id, summit_id, oug_id, dvs_id),
    )
    conn.commit()
    yield conn
    conn.close()


def test_ingest_ops_inserts_rows(db_with_match):
    results = ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    assert [r["status"] for r in results] == ["ok"] * 4

    rows = db_with_match.execute(
        """SELECT week, player_name, op_kills, op_pulls, footage_min
           FROM ops_player_stats
           WHERE player_name = 'Alpha'
           ORDER BY stat_id"""
    ).fetchall()
    assert rows == [(1, "Alpha", 4, 4, 11.83), (1, "Alpha", 9, 4, 9.47)]


def test_ingest_ops_derives_week_from_date(db_with_match):
    csv_no_week = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-15,OUG,Tunisia,Alpha,4,4,11.83"""
    results = ingest_ops_kills(db_with_match, io.StringIO(csv_no_week))
    assert results[0]["status"] == "ok"

    week = db_with_match.execute("SELECT week FROM ops_player_stats").fetchone()[0]
    assert week == _date.fromisoformat("2026-02-15").isocalendar()[1]


def test_ingest_ops_skips_duplicates(db_with_match):
    ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    results = ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    assert [r["status"] for r in results] == ["skipped"] * 4
    assert db_with_match.execute("SELECT COUNT(*) FROM ops_player_stats").fetchone()[0] == 4


def test_ingest_ops_errors_on_unknown_opponent(db_with_match):
    bad = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-15,ZZZ,Tunisia,Alpha,4,4,11.83"""
    results = ingest_ops_kills(db_with_match, io.StringIO(bad))
    assert results[0]["status"] == "error"
    assert "ZZZ" in results[0]["errors"]


def test_ingest_ops_errors_on_missing_match(db_with_match):
    bad = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2099-01-01,OUG,Tunisia,Alpha,4,4,11.83"""
    results = ingest_ops_kills(db_with_match, io.StringIO(bad))
    assert results[0]["status"] == "error"


def test_ops_independent_of_tournament_player_stats(db_with_match):
    """Footage coverage lags the scoreboard: ops ingests fine with no K/D rows."""
    ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    assert db_with_match.execute(
        "SELECT COUNT(*) FROM tournament_player_stats"
    ).fetchone()[0] == 0
    assert db_with_match.execute("SELECT COUNT(*) FROM ops_player_stats").fetchone()[0] == 4
