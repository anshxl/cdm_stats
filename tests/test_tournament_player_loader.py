import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-02-15,1,OUG,Tunisia,SnD,Alpha,20,15,5
2026-02-15,1,OUG,Tunisia,SnD,Bravo,18,12,8
2026-02-15,1,OUG,Summit,HP,Alpha,30,25,10
2026-02-15,1,OUG,Summit,HP,Bravo,28,20,12"""


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


def test_ingest_tournament_players_inserts_rows(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    results = ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    ok = [r for r in results if r["status"] == "ok"]
    assert len(ok) == 4

    count = db_with_match.execute(
        "SELECT COUNT(*) FROM tournament_player_stats"
    ).fetchone()[0]
    assert count == 4

    row = db_with_match.execute(
        """SELECT week, player_name, kills, deaths, assists
           FROM tournament_player_stats
           WHERE player_name = 'Alpha'
           ORDER BY stat_id"""
    ).fetchall()
    assert row[0] == (1, "Alpha", 20, 15, 5)
    assert row[1] == (1, "Alpha", 30, 25, 10)


def test_ingest_tournament_players_skips_duplicates(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    results = ingest_tournament_players(db_with_match, io.StringIO(PLAYER_CSV))
    skipped = [r for r in results if r["status"] == "skipped"]
    assert len(skipped) == 4

    count = db_with_match.execute(
        "SELECT COUNT(*) FROM tournament_player_stats"
    ).fetchone()[0]
    assert count == 4


def test_ingest_tournament_players_errors_on_unknown_opponent(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    bad_csv = "Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists\n2026-02-15,1,ZZZ,Tunisia,SnD,Alpha,20,15,5"
    results = ingest_tournament_players(db_with_match, io.StringIO(bad_csv))
    assert len(results) == 1
    assert results[0]["status"] == "error"
    assert "ZZZ" in results[0]["errors"]


def test_ingest_tournament_players_errors_on_missing_match(db_with_match):
    from cdm_stats.ingestion.tournament_player_loader import ingest_tournament_players
    bad_csv = "Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists\n2099-01-01,1,OUG,Tunisia,SnD,Alpha,20,15,5"
    results = ingest_tournament_players(db_with_match, io.StringIO(bad_csv))
    assert len(results) == 1
    assert results[0]["status"] == "error"
