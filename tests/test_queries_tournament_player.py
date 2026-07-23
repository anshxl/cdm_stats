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


def test_player_summary_filters_by_season(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import player_summary
    db = db_with_tournament_players
    assert len(player_summary(db, season=1)) == 2
    assert player_summary(db, season=2) == []

    db.execute("UPDATE matches SET season = 2")
    db.commit()
    assert player_summary(db, season=1) == []
    assert len(player_summary(db, season=2)) == 2


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



def test_recent_map_stats_newest_first_with_opponent(db_with_tournament_players):
    """Newest map first; opponent is whichever side isn't ours."""
    from cdm_stats.db.queries_tournament_player import recent_map_stats
    rows = recent_map_stats(db_with_tournament_players, "DVS")
    # Same match, so slot order decides: Summit is slot 2, Tunisia slot 1.
    assert [r["map_name"] for r in rows] == ["Summit", "Tunisia"]
    assert all(r["opponent"] == "OUG" for r in rows)
    assert [p["player_name"] for p in rows[0]["players"]] == ["Alpha", "Bravo"]
    assert rows[0]["players"][0]["kills"] == 30


def test_recent_map_stats_names_opponent_when_we_are_team2(db_with_tournament_players):
    """We sit on either side of a match, so opponent can't be read off team1."""
    from cdm_stats.db.queries_tournament_player import recent_map_stats
    rows = recent_map_stats(db_with_tournament_players, "OUG")
    assert all(r["opponent"] == "DVS" for r in rows)


def test_recent_map_stats_ops_are_none_when_no_footage(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import recent_map_stats
    rows = recent_map_stats(db_with_tournament_players, "DVS")
    assert rows[0]["players"][0]["op_kills"] is None
    assert rows[0]["players"][0]["op_pulls"] is None


def test_recent_map_stats_includes_ops_only_map(db_with_tournament_players):
    """Footage can land before the scoreboard — that map must still appear."""
    from cdm_stats.db.queries_tournament_player import recent_map_stats
    conn = db_with_tournament_players
    tunisia_rid = conn.execute(
        """SELECT result_id FROM map_results mr JOIN maps m ON mr.map_id = m.map_id
           WHERE m.map_name = 'Tunisia'"""
    ).fetchone()[0]
    conn.execute("DELETE FROM tournament_player_stats WHERE result_id = ?", (tunisia_rid,))
    conn.execute(
        """INSERT INTO ops_player_stats
           (result_id, week, player_name, op_kills, op_pulls, footage_min)
           VALUES (?, 1, 'Alpha', 4, 3, 11.0)""",
        (tunisia_rid,),
    )
    conn.commit()

    rows = recent_map_stats(conn, "DVS")
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    alpha = tunisia["players"][0]
    assert (alpha["op_kills"], alpha["op_pulls"]) == (4, 3)
    assert alpha["kills"] is None and alpha["deaths"] is None


def test_recent_map_stats_respects_limit_and_filters(db_with_tournament_players):
    from cdm_stats.db.queries_tournament_player import recent_map_stats
    conn = db_with_tournament_players
    assert len(recent_map_stats(conn, "DVS", limit=1)) == 1
    assert [r["map_name"] for r in recent_map_stats(conn, "DVS", mode="HP")] == ["Summit"]
    assert recent_map_stats(conn, "DVS", week_range=(9, 9)) == []
    assert recent_map_stats(conn, "DVS", season=2) == []

    alpha_only = recent_map_stats(conn, "DVS", player="Alpha")
    assert all([p["player_name"] for p in r["players"]] == ["Alpha"] for r in alpha_only)
