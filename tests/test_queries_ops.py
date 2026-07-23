import io
import pytest
from cdm_stats.db.queries_ops import ops_player_weekly_trend
from cdm_stats.ingestion.ops_loader import ingest_ops_kills
from test_ops_loader import db_with_match, OPS_CSV  # noqa: F401


@pytest.fixture
def db_with_ops(db_with_match):  # noqa: F811
    ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    return db_with_match


def test_trend_pools_within_week(db_with_ops):
    """Both fixture maps are week 1, so each player gets one pooled point."""
    rows = ops_player_weekly_trend(db_with_ops)
    # Alpha: (4+9) kills / (4+4) pulls = 1.63. Bravo: (5+0) / (3+2) = 1.00.
    assert rows == [
        {"player_name": "Alpha", "week": 1, "op_kills": 13, "op_pulls": 8,
         "maps": 2, "kills_per_pull": 1.62},
        {"player_name": "Bravo", "week": 1, "op_kills": 5, "op_pulls": 5,
         "maps": 2, "kills_per_pull": 1.0},
    ]


def test_trend_pools_rather_than_averaging_per_map_rates(db_with_ops):
    """Bravo's per-map rates are 1.667 and 0.0 (mean 0.83); pooled is 1.00."""
    bravo = next(r for r in ops_player_weekly_trend(db_with_ops)
                 if r["player_name"] == "Bravo")
    assert bravo["kills_per_pull"] == 1.0


def test_trend_orders_by_player_then_week(db_with_match):  # noqa: F811
    """Two weeks for one player come back chronologically, so the line is drawn in order."""
    _add_second_week(db_with_match)
    rows = ops_player_weekly_trend(db_with_match)
    alpha = [r for r in rows if r["player_name"] == "Alpha"]
    assert [r["week"] for r in alpha] == [1, 2]
    assert [r["kills_per_pull"] for r in alpha] == [1.62, 0.5]


def test_zero_pull_week_is_none_not_zero(db_with_match):  # noqa: F811
    """No pulls means no rate — the chart must break the line, not plot a 0.0."""
    csv_no_pulls = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-15,OUG,Tunisia,Charlie,0,0,11.83"""
    ingest_ops_kills(db_with_match, io.StringIO(csv_no_pulls))
    rows = ops_player_weekly_trend(db_with_match)
    assert rows[0]["kills_per_pull"] is None


def test_mode_filter(db_with_ops):
    rows = ops_player_weekly_trend(db_with_ops, mode="HP")
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["op_kills"] == 9 and alpha["maps"] == 1


def test_player_filter(db_with_ops):
    rows = ops_player_weekly_trend(db_with_ops, player="Alpha")
    assert [r["player_name"] for r in rows] == ["Alpha"]


def test_season_filter_excludes_other_seasons(db_with_ops):
    assert ops_player_weekly_trend(db_with_ops, season=2) == []


def _add_second_week(conn):
    """Ingest a week-2 map for Alpha on a fresh match, so the trend has two points."""
    dvs_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = conn.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    tunisia_id = conn.execute(
        "SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'"
    ).fetchone()[0]
    conn.execute(
        """INSERT INTO matches (match_date, team1_id, team2_id, two_v_two_winner_id,
                                series_winner_id, match_format, series_number)
           VALUES ('2026-02-22', ?, ?, ?, ?, 'CDL_BO5', 1)""",
        (dvs_id, oug_id, dvs_id, dvs_id),
    )
    m2 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO map_results (match_id, slot, map_id, picked_by_team_id, winner_team_id,
                                    picking_team_score, non_picking_team_score,
                                    team1_score_before, team2_score_before, pick_context)
           VALUES (?, 1, ?, ?, ?, 6, 3, 0, 0, 'Opener')""",
        (m2, tunisia_id, dvs_id, dvs_id),
    )
    conn.commit()
    ingest_ops_kills(conn, io.StringIO(OPS_CSV))
    ingest_ops_kills(conn, io.StringIO(
        """Date,Week,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-22,2,OUG,Tunisia,Alpha,2,4,10.0"""
    ))
