import io
import pytest
from cdm_stats.db.queries_ops import ops_player_summary
from cdm_stats.ingestion.ops_loader import ingest_ops_kills
from test_ops_loader import db_with_match, OPS_CSV  # noqa: F401


@pytest.fixture
def db_with_ops(db_with_match):  # noqa: F811
    ingest_ops_kills(db_with_match, io.StringIO(OPS_CSV))
    return db_with_match


def test_summary_aggregates_across_maps_and_ranks(db_with_ops):
    rows = ops_player_summary(db_with_ops)
    # Alpha: (4+9) kills / (4+4) pulls = 1.63. Bravo: (5+0)/(3+2) = 1.00.
    assert [r["player_name"] for r in rows] == ["Alpha", "Bravo"]
    assert rows[0] == {
        "player_name": "Alpha", "op_kills": 13, "op_pulls": 8,
        "maps": 2, "kills_per_pull": 1.62,
    }
    assert rows[1]["kills_per_pull"] == 1.0


def test_summary_pools_pulls_rather_than_averaging_rates(db_with_ops):
    """Bravo's per-map rates are 1.667 and 0.0 (mean 0.83); pooled is 1.00."""
    bravo = next(r for r in ops_player_summary(db_with_ops) if r["player_name"] == "Bravo")
    assert bravo["kills_per_pull"] == 1.0


def test_zero_pulls_yields_none_not_divide_by_zero(db_with_match):  # noqa: F811
    csv_no_pulls = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-15,OUG,Tunisia,Charlie,0,0,11.83"""
    ingest_ops_kills(db_with_match, io.StringIO(csv_no_pulls))
    rows = ops_player_summary(db_with_match)
    assert rows[0]["kills_per_pull"] is None


def test_zero_pull_players_sort_last(db_with_ops):
    csv_no_pulls = """Date,Opponent,Map,Player,OpKills,OpPulls,FootageMin
2026-02-15,OUG,Tunisia,Charlie,0,0,11.83"""
    ingest_ops_kills(db_with_ops, io.StringIO(csv_no_pulls))
    rows = ops_player_summary(db_with_ops)
    assert rows[-1]["player_name"] == "Charlie"


def test_mode_filter(db_with_ops):
    rows = ops_player_summary(db_with_ops, mode="HP")
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["op_kills"] == 9 and alpha["maps"] == 1


def test_player_and_week_filters(db_with_ops):
    assert [r["player_name"] for r in ops_player_summary(db_with_ops, player="Alpha")] == ["Alpha"]
    assert ops_player_summary(db_with_ops, week_range=(50, 52)) == []


def test_season_filter_excludes_other_seasons(db_with_ops):
    assert ops_player_summary(db_with_ops, season=2) == []
