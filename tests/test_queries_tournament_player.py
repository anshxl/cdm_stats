import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


@pytest.fixture
def db_with_tournament_players():
    """DB with one match, 2 map_results, and player stats for 2 players over 2 maps."""
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)

    # Insert a match: DVS vs OUG on 2026-02-15
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

    # Two map_results
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 1, ?, ?, ?, 6, 3, 0, 0, 'Opener')""",
        (match_id, tunisia_id, dvs_id, dvs_id),
    )
    tunisia_result_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 2, ?, ?, ?, 250, 200, 1, 0, 'Neutral')""",
        (match_id, summit_id, oug_id, dvs_id),
    )
    summit_result_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Player stats: Alpha + Bravo on both maps, week 1
    stats = [
        (tunisia_result_id, 1, "Alpha", 20, 15, 5),
        (tunisia_result_id, 1, "Bravo", 18, 12, 8),
        (summit_result_id, 1, "Alpha", 30, 25, 10),
        (summit_result_id, 1, "Bravo", 28, 20, 12),
    ]
    conn.executemany(
        """INSERT INTO tournament_player_stats
           (result_id, week, player_name, kills, deaths, assists)
           VALUES (?, ?, ?, ?, ?, ?)""",
        stats,
    )
    conn.commit()
    yield conn
    conn.close()


def test_player_summary_all(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players)
    assert len(rows) == 2
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 50
    assert alpha["deaths"] == 40
    assert alpha["games"] == 2
    assert alpha["kd"] == pytest.approx(50 / 40, abs=0.01)


def test_player_summary_filter_by_player(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["player_name"] == "Alpha"


def test_player_summary_filter_by_mode(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, mode="SnD")
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 20


def test_player_summary_filter_by_week(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    rows = player_summary(db_with_tournament_players, week_range=(1, 1))
    assert len(rows) == 2
    rows_empty = player_summary(db_with_tournament_players, week_range=(2, 2))
    assert rows_empty == []


def test_player_weekly_trend(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_weekly_trend
    rows = player_weekly_trend(db_with_tournament_players, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["week"] == 1
    assert rows[0]["kd"] == pytest.approx(50 / 40, abs=0.01)


def test_player_map_breakdown(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_map_breakdown
    rows = player_map_breakdown(db_with_tournament_players, player="Alpha")
    assert len(rows) == 2
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["games"] == 1
    assert tunisia["avg_kills"] == 20.0
