import io
import json
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.scrim_loader import ingest_scrims_players, ingest_scrims_team
from cdm_stats.ingestion.scrim_ops_loader import ingest_scrim_ops
from cdm_stats.ingestion.seed import seed_maps, seed_teams

TEAM_CSV = """Date,Opponent,Map,Score
2026-10-07,ALU,Crossroads Strike,4-3
2026-10-07,ALU,Summit,250-200"""

PLAYERS_CSV = """Date,Opponent,Map,Player,Kills,Deaths,Assists
2026-10-07,ALU,Summit,Skullguy,30,20,5"""


def pull(kills, start="10:00.0", end="10:05.0", duration=5.0):
    return {"number": 1, "start": start, "end": end, "kills": kills, "duration": duration}


def export(**overrides):
    """A minimal godlike-operator-data export: one AlUla block, two maps."""
    skull = {"kills": 3, "pulls": 2, "operatingTime": 10.0, "pullDetails": [pull(1), pull(2)]}
    viper = {"kills": 0, "pulls": 0, "operatingTime": 5.0, "pullDetails": [pull(1)]}  # stale totals
    scrim = {"name": "AlUla 7 October", "date": "", "matches": [
        {"map": "Summit", "vodStart": "10:00", "vodEnd": "20:30", "players": {"SkullG": skull}},
        {"map": "Crossroads", "vodStart": "58:15", "vodEnd": "1:02:45", "players": {"Viper": viper}},
    ]}
    scrim.update(overrides)
    return io.StringIO(json.dumps({
        "type": "godlike-operator-data",
        "team": {"players": [{"name": "SkullGuy", "operatorKey": "SkullG"},
                             {"name": "Viper", "operatorKey": "Viper"}]},
        "scrims": [scrim],
    }))


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_scrims_team(conn, io.StringIO(TEAM_CSV), season=2)
    ingest_scrims_players(conn, io.StringIO(PLAYERS_CSV), season=2)
    yield conn
    conn.close()


def rows(db):
    return db.execute(
        """SELECT sm.scrim_date, sm.map_name, o.player_name, o.op_kills, o.op_pulls, o.op_time_sec, o.footage_min
           FROM scrim_ops_stats o JOIN scrim_maps sm USING(scrim_map_id) ORDER BY sm.map_name"""
    ).fetchall()


def test_matches_name_date_opponent_map_and_player(db):
    res = ingest_scrim_ops(db, export(), season=2)
    assert [r["status"] for r in res] == ["ok", "ok"]
    assert rows(db) == [
        ("2026-10-07", "Crossroads Strike", "Viper", 1, 1, 5.0, 4.5),
        ("2026-10-07", "Summit", "Skullguy", 3, 2, 10.0, 10.5),
    ]


def test_pull_records_win_over_stale_totals(db):
    res = ingest_scrim_ops(db, export(), season=2)
    viper = next(r for r in res if r["row"].endswith("Viper"))
    assert "pull records say 1 / 1" in viper["warning"]
    assert next(r for r in res if r["row"].endswith("Skullguy"))["warning"] is None


def test_rerun_skips_identical_and_updates_changed(db):
    ingest_scrim_ops(db, export(), season=2)
    assert {r["status"] for r in ingest_scrim_ops(db, export(), season=2)} == {"skipped"}
    res = ingest_scrim_ops(db, export(date="2026-10-07", name="ALU 7 October"), season=2)
    assert {r["status"] for r in res} == {"skipped"}  # explicit date + abbreviation hit the same rows
    fixed = json.loads(export().getvalue())
    fixed["scrims"][0]["matches"][0]["players"]["SkullG"]["pullDetails"].append(pull(4))
    res = ingest_scrim_ops(db, io.StringIO(json.dumps(fixed)), season=2)
    assert "updated from" in next(r for r in res if r["status"] == "ok")["warning"]
    assert rows(db)[1][3:5] == (7, 3)


def test_errors(db):
    assert ingest_scrim_ops(db, export(name="Nobody 7 October"), season=2)[0]["errors"] == "Unknown opponent: Nobody"
    res = ingest_scrim_ops(db, export(name="AlUla 9 October"), season=2)
    assert {r["errors"] for r in res} == {"No matching scrim map (ingest the team CSV first)"}
    assert "No date" in ingest_scrim_ops(db, export(name="AlUla"), season=2)[0]["errors"]
    assert rows(db) == []
