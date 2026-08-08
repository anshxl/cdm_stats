import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players


TEAM_CSV = """Date,Opponent,Map,Score
2026-02-25,DVS,Tunisia,6-3
2026-02-25,DVS,Summit,250-200
2026-03-03,OUG,Tunisia,4-6"""


PLAYER_CSV = """Date,Opponent,Map,Player,Kills,Deaths,Assists
2026-02-25,DVS,Tunisia,Alpha,20,15,5
2026-02-25,DVS,Tunisia,Bravo,18,12,8
2026-02-25,DVS,Tunisia,Charlie,15,18,3
2026-02-25,DVS,Tunisia,Delta,22,10,6
2026-02-25,DVS,Tunisia,Echo,12,20,4"""


@pytest.fixture
def scrim_db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_scrims_team(conn, io.StringIO(TEAM_CSV))
    ingest_scrims_players(conn, io.StringIO(PLAYER_CSV))
    yield conn
    conn.close()


def test_scrim_performance_build_summary(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_summary_data
    data = _build_summary_data(scrim_db)
    assert data["overall"]["wins"] == 2
    assert data["overall"]["losses"] == 1
    assert "SnD" in data["by_mode"]
    assert "HP" in data["by_mode"]


def test_scrim_performance_build_summary_filters_by_season(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_summary_data
    # Add a season-2 scrim
    s2 = """Date,Opponent,Map,Score
2026-06-10,DVS,Raid,3-1"""
    ingest_scrims_team(scrim_db, io.StringIO(s2), season=2)

    s1 = _build_summary_data(scrim_db, season=1)
    assert s1["overall"]["wins"] == 2
    assert s1["overall"]["losses"] == 1

    s2_data = _build_summary_data(scrim_db, season=2)
    assert s2_data["overall"]["wins"] == 1
    assert s2_data["overall"]["losses"] == 0


def test_scrim_queries_filter_by_opponent(scrim_db):
    from cdm_stats.db.queries_scrim import (
        scrim_win_loss, scrim_map_breakdown, scrim_weekly_trend, scrim_map_results_detail,
    )
    # DVS: 2-0 in week 1; OUG: 0-1 in week 2.
    dvs = scrim_win_loss(scrim_db, opponent="DVS")
    assert (dvs["wins"], dvs["losses"]) == (2, 0)
    assert all(r["losses"] == 0 for r in scrim_map_breakdown(scrim_db, opponent="DVS"))
    assert [r["week"] for r in scrim_weekly_trend(scrim_db, opponent="OUG")] == [2]
    tunisia = scrim_map_results_detail(scrim_db, "Tunisia", opponent="DVS")
    assert [d["opponent"] for d in tunisia] == ["DVS"]


def test_scrim_detail_sorts_by_week_not_date_text(scrim_db):
    """scrim_date is text like '7-Jul'; ordering must come from week + id."""
    from cdm_stats.db.queries_scrim import scrim_map_results_detail
    rows = scrim_map_results_detail(scrim_db, "Tunisia")
    assert [d["week"] for d in rows] == [2, 1]  # newest first


def test_scrim_performance_build_map_table(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_map_breakdown
    rows = scrim_map_breakdown(scrim_db)
    assert len(rows) == 2  # Tunisia and Summit
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 1


def test_scrim_performance_build_trend(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_weekly_trend
    rows = scrim_weekly_trend(scrim_db)
    assert len(rows) == 2
    assert rows[0]["week"] == 1
    assert rows[0]["win_pct"] == 100.0


def test_scrim_performance_layout():
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    result = layout()
    assert result is not None


def test_scrim_player_summary_filters_by_season(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    # Season 1 has 5 players; season 2 has none
    assert len(player_summary(scrim_db, season=1)) == 5
    assert player_summary(scrim_db, season=2) == []


def test_scrim_player_summary(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    data = player_summary(scrim_db)
    assert len(data) == 5
    alpha = next(d for d in data if d["player_name"] == "Alpha")
    assert alpha["kills"] == 20
    assert alpha["deaths"] == 15


def test_scrim_player_weekly_trend(scrim_db):
    from cdm_stats.db.queries_scrim import player_weekly_trend
    rows = player_weekly_trend(scrim_db)
    assert len(rows) >= 1
    alpha_w1 = next(r for r in rows if r["player_name"] == "Alpha" and r["week"] == 1)
    assert alpha_w1["kd"] == pytest.approx(20 / 15, abs=0.01)


def test_player_stats_layout():
    from cdm_stats.dashboard.tabs.player_stats import layout
    result = layout()
    assert result is not None


def test_scrim_performance_layout_filters():
    """Scrim layout has Week and Opponent dropdowns, no week pills."""
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    import json
    result = layout()
    serialized = json.dumps(result.to_plotly_json(), default=str)
    assert "scrim-week-filter" in serialized
    assert "scrim-opponent-filter" in serialized
    assert "scrim-week-pills" not in serialized


def test_player_stats_layout_has_pills_no_source_toggle():
    from cdm_stats.dashboard.tabs.player_stats import layout
    import json
    result = layout()
    serialized = json.dumps(result.to_plotly_json(), default=str)
    assert "player-week-pills" in serialized
    assert "player-week-slider" not in serialized
    assert "player-source-filter" not in serialized
    assert "player-opponent-filter" in serialized
