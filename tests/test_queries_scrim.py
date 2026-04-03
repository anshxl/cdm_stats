import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.scrim_loader import ingest_scrims_team, ingest_scrims_players


TEAM_CSV = """Date,Week,Opponent,Map,Mode,Score,Result
2026-03-10,1,DVS,Tunisia,SnD,6-3,W
2026-03-10,1,DVS,Summit,HP,250-200,W
2026-03-10,1,DVS,Raid,Control,3-1,W
2026-03-17,2,OUG,Tunisia,SnD,4-6,L
2026-03-17,2,OUG,Summit,HP,230-250,L
2026-03-17,2,OUG,Hacienda,HP,250-180,W"""


PLAYER_CSV = """Date,Week,Opponent,Map,Mode,Player,Kills,Deaths,Assists
2026-03-10,1,DVS,Tunisia,SnD,Alpha,20,15,5
2026-03-10,1,DVS,Tunisia,SnD,Bravo,18,12,8
2026-03-10,1,DVS,Tunisia,SnD,Charlie,15,18,3
2026-03-10,1,DVS,Tunisia,SnD,Delta,22,10,6
2026-03-10,1,DVS,Tunisia,SnD,Echo,12,20,4
2026-03-17,2,OUG,Tunisia,SnD,Alpha,10,20,3
2026-03-17,2,OUG,Tunisia,SnD,Bravo,14,16,5
2026-03-17,2,OUG,Tunisia,SnD,Charlie,8,22,2
2026-03-17,2,OUG,Tunisia,SnD,Delta,16,14,7
2026-03-17,2,OUG,Tunisia,SnD,Echo,9,18,1"""


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


def test_scrim_win_loss_all(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db)
    assert result["wins"] == 4
    assert result["losses"] == 2
    assert result["win_pct"] == pytest.approx(66.67, abs=0.01)


def test_scrim_win_loss_by_mode(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db, mode="SnD")
    assert result["wins"] == 1
    assert result["losses"] == 1


def test_scrim_win_loss_by_week(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_win_loss
    result = scrim_win_loss(scrim_db, week_range=(1, 1))
    assert result["wins"] == 3
    assert result["losses"] == 0


def test_scrim_map_breakdown(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_map_breakdown
    rows = scrim_map_breakdown(scrim_db)
    tunisia = next(r for r in rows if r["map_name"] == "Tunisia")
    assert tunisia["wins"] == 1
    assert tunisia["losses"] == 1
    assert tunisia["played"] == 2


def test_scrim_map_breakdown_by_mode(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_map_breakdown
    rows = scrim_map_breakdown(scrim_db, mode="HP")
    assert len(rows) == 2  # Summit and Hacienda
    summit = next(r for r in rows if r["map_name"] == "Summit")
    assert summit["wins"] == 1
    assert summit["losses"] == 1


def test_scrim_weekly_trend(scrim_db):
    from cdm_stats.db.queries_scrim import scrim_weekly_trend
    rows = scrim_weekly_trend(scrim_db)
    assert len(rows) == 2
    week1 = next(r for r in rows if r["week"] == 1)
    assert week1["win_pct"] == 100.0
    week2 = next(r for r in rows if r["week"] == 2)
    assert week2["win_pct"] == pytest.approx(33.33, abs=0.01)


def test_player_summary_all(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    rows = player_summary(scrim_db)
    alpha = next(r for r in rows if r["player_name"] == "Alpha")
    assert alpha["kills"] == 30  # 20 + 10
    assert alpha["deaths"] == 35  # 15 + 20
    assert alpha["assists"] == 8  # 5 + 3
    assert alpha["kd"] == pytest.approx(30 / 35, abs=0.01)


def test_player_summary_filtered(scrim_db):
    from cdm_stats.db.queries_scrim import player_summary
    rows = player_summary(scrim_db, player="Alpha", week_range=(1, 1))
    assert len(rows) == 1
    assert rows[0]["kills"] == 20


def test_player_weekly_trend(scrim_db):
    from cdm_stats.db.queries_scrim import player_weekly_trend
    rows = player_weekly_trend(scrim_db, player="Alpha")
    assert len(rows) == 2
    week1 = next(r for r in rows if r["week"] == 1)
    assert week1["kd"] == pytest.approx(20 / 15, abs=0.01)


def test_player_map_breakdown(scrim_db):
    from cdm_stats.db.queries_scrim import player_map_breakdown
    rows = player_map_breakdown(scrim_db, player="Alpha")
    assert len(rows) == 1  # Alpha only played Tunisia
    assert rows[0]["map_name"] == "Tunisia"
    assert rows[0]["games"] == 2
    assert rows[0]["avg_kills"] == 15.0  # (20+10)/2
