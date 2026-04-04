import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-17,2,OUG,Tunisia,SnD,4-6,L"""


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Alpha,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Bravo,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Charlie,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Delta,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Echo,12,20,4"""


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


def test_scrim_performance_build_map_table(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_map_table_data
    rows = _build_map_table_data(scrim_db)
    assert len(rows) == 2  # Tunisia and Summit
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 1


def test_scrim_performance_build_trend(scrim_db):
    from cdm_stats.dashboard.tabs.scrim_performance import _build_trend_data
    rows = _build_trend_data(scrim_db)
    assert len(rows) == 2
    assert rows[0]["week"] == 1
    assert rows[0]["win_pct"] == 100.0


def test_scrim_performance_layout():
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    result = layout()
    assert result is not None


def test_player_stats_build_summary(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_player_cards_data
    data = _build_player_cards_data(scrim_db)
    assert len(data) == 5
    alpha = next(d for d in data if d["player_name"] == "Alpha")
    assert alpha["kills"] == 20
    assert alpha["deaths"] == 15


def test_player_stats_build_trend(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_kd_trend_data
    rows = _build_kd_trend_data(scrim_db)
    assert len(rows) >= 1
    alpha_w1 = next(r for r in rows if r["player_name"] == "Alpha" and r["week"] == 1)
    assert alpha_w1["kd"] == pytest.approx(20 / 15, abs=0.01)


def test_player_stats_build_map_table(scrim_db):
    from cdm_stats.dashboard.tabs.player_stats import _build_player_map_data
    rows = _build_player_map_data(scrim_db, player="Alpha")
    assert len(rows) == 1
    assert rows[0]["map_name"] == "Tunisia"


def test_player_stats_layout():
    from cdm_stats.dashboard.tabs.player_stats import layout
    result = layout()
    assert result is not None


def test_scrim_performance_layout_uses_week_pills():
    """Scrim layout renders the week_pills component, not a RangeSlider."""
    from cdm_stats.dashboard.tabs.scrim_performance import layout
    import json
    result = layout()
    serialized = json.dumps(result.to_plotly_json(), default=str)
    assert "scrim-week-pills" in serialized
    assert "scrim-week-slider" not in serialized
