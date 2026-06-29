import io
import sqlite3

import pytest

from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.s2_loader import ingest_s2_matches, ingest_s2_bans


HEADER = "date,competition,stage,format,team1,team2,map,team1_score,team2_score,picked_by,series_winner,dq"

# DVS beats OUG 3-1 in a Bo5. Maps span all three modes.
BASIC = HEADER + "\n" + "\n".join([
    "2026-06-25,CDM,Stage 1 Masters,Bo5,DVS,OUG,Tunisia,6,3,DVS,,",   # SnD, DVS
    "2026-06-25,CDM,Stage 1 Masters,Bo5,DVS,OUG,Summit,250,200,OUG,,",  # HP, DVS
    "2026-06-25,CDM,Stage 1 Masters,Bo5,DVS,OUG,Raid,1,3,OUG,,",        # Control, OUG
    "2026-06-25,CDM,Stage 1 Masters,Bo5,DVS,OUG,Slums,6,4,OUG,,",       # SnD, DVS
])


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


def _id(db, abbr):
    return db.execute("SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)).fetchone()[0]


def test_basic_series_stored_with_s2_fields(db):
    results = ingest_s2_matches(db, io.StringIO(BASIC))
    assert [r["status"] for r in results] == ["ok"]

    row = db.execute(
        "SELECT season, competition, match_format, round, series_winner_id FROM matches"
    ).fetchone()
    assert row[0] == 2                       # season
    assert row[1] == "CDM"                   # competition
    assert row[2] == "Bo5"                   # match_format
    assert row[3] == "Stage 1 Masters"       # round/stage
    assert row[4] == _id(db, "DVS")          # derived series winner


def test_mode_derived_from_map(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    modes = db.execute(
        "SELECT mp.map_name, mp.mode FROM map_results mr "
        "JOIN maps mp ON mr.map_id = mp.map_id ORDER BY mr.slot"
    ).fetchall()
    assert modes == [("Tunisia", "SnD"), ("Summit", "HP"), ("Raid", "Control"), ("Slums", "SnD")]


def test_map_winner_derived_from_scores(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    winners = [r[0] for r in db.execute(
        "SELECT winner_team_id FROM map_results ORDER BY slot"
    ).fetchall()]
    dvs, oug = _id(db, "DVS"), _id(db, "OUG")
    assert winners == [dvs, dvs, oug, dvs]


def test_pick_context_must_win(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    # Slot 3 (Raid) picked by OUG while OUG trails 0-2 (one loss from elimination).
    ctx = db.execute("SELECT pick_context FROM map_results WHERE slot = 3").fetchone()[0]
    assert ctx == "Must-Win"
    assert db.execute("SELECT pick_context FROM map_results WHERE slot = 1").fetchone()[0] == "Opener"


def test_series_winner_override_for_dq(db):
    # Maps fall OUG 2-1, but OUG's clinching map is a DQ; override gives GL the series.
    csv = HEADER + "\n" + "\n".join([
        "2026-06-26,CDM,Stage 1,Bo5,GL,OUG,Coastal,6,4,GL,,",
        "2026-06-26,CDM,Stage 1,Bo5,GL,OUG,Combine,200,250,OUG,,",
        "2026-06-26,CDM,Stage 1,Bo5,GL,OUG,Standoff,2,3,OUG,GL,1",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert [r["status"] for r in results] == ["ok"]
    assert db.execute("SELECT series_winner_id FROM matches").fetchone()[0] == _id(db, "GL")
    assert db.execute("SELECT dq FROM map_results WHERE slot = 3").fetchone()[0] == 1


def test_ro3_series(db):
    csv = HEADER + "\n" + "\n".join([
        "2026-06-27,CDM,Stage 1,Ro3,DVS,OUG,Tunisia,6,3,DVS,,",
        "2026-06-27,CDM,Stage 1,Ro3,DVS,OUG,Summit,250,100,DVS,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert [r["status"] for r in results] == ["ok"]
    assert db.execute("SELECT series_winner_id FROM matches").fetchone()[0] == _id(db, "DVS")


def test_ro3_sweep_all_maps_played(db):
    # Ro3 plays all 3 maps, so a team can finish 3-0 (more than the threshold of 2).
    csv = HEADER + "\n" + "\n".join([
        "2026-07-01,CDM,Stage 1,Ro3,DVS,OUG,Tunisia,6,3,DVS,,",
        "2026-07-01,CDM,Stage 1,Ro3,DVS,OUG,Summit,250,100,DVS,,",
        "2026-07-01,CDM,Stage 1,Ro3,DVS,OUG,Raid,3,1,DVS,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert [r["status"] for r in results] == ["ok"]
    assert db.execute("SELECT series_winner_id FROM matches").fetchone()[0] == _id(db, "DVS")
    assert db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0] == 3


def test_two_series_same_day_distinguished_by_stage(db):
    # GL vs GAL play two series on the same day (different bracket stages).
    csv = HEADER + "\n" + "\n".join([
        "2026-06-14,SPLIT II,UB,Bo5,GL,GAL,Arsenal,250,101,,,",
        "2026-06-14,SPLIT II,UB,Bo5,GL,GAL,Firing Range,4,9,,,",
        "2026-06-14,SPLIT II,UB,Bo5,GL,GAL,Raid,2,3,,,",
        "2026-06-14,SPLIT II,UB,Bo5,GL,GAL,Summit,250,233,,,",
        "2026-06-14,SPLIT II,UB,Bo5,GL,GAL,Coastal,8,10,,,",          # GAL wins 3-2
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Arsenal,250,239,,,",
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Slums,9,5,,,",
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Crossroads Strike,3,1,,,",
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Combine,246,250,,,",
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Raid,1,3,,,",
        "2026-06-14,SPLIT II,Finals,Bo7,GL,GAL,Meltdown,3,9,,GAL,",   # override -> GAL
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert [r["status"] for r in results] == ["ok", "ok"]

    rows = db.execute(
        "SELECT round, match_format FROM matches WHERE season=2 ORDER BY round"
    ).fetchall()
    assert rows == [("Finals", "Bo7"), ("UB", "Bo5")]

    # Re-ingesting the same file skips both (dup check is stage-aware).
    again = ingest_s2_matches(db, io.StringIO(csv))
    assert [r["status"] for r in again] == ["skipped", "skipped"]


def test_unknown_map_is_error(db):
    csv = HEADER + "\n" + "\n".join([
        "2026-06-28,CDM,Stage 1,Bo5,DVS,OUG,NotAMap,6,3,DVS,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 0


def test_tie_is_error(db):
    csv = HEADER + "\n" + "\n".join([
        "2026-06-28,CDM,Stage 1,Bo5,DVS,OUG,Tunisia,6,6,DVS,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert results[0]["status"] == "error"


def test_incomplete_series_is_error(db):
    # 2-1 in a Bo5, no override -> nobody reached 3.
    csv = HEADER + "\n" + "\n".join([
        "2026-06-28,CDM,Stage 1,Bo5,DVS,OUG,Tunisia,6,3,DVS,,",
        "2026-06-28,CDM,Stage 1,Bo5,DVS,OUG,Summit,250,200,DVS,,",
        "2026-06-28,CDM,Stage 1,Bo5,DVS,OUG,Raid,1,3,OUG,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert results[0]["status"] == "error"


def test_unknown_format_is_error(db):
    csv = HEADER + "\n" + "\n".join([
        "2026-06-28,CDM,Stage 1,Bo9,DVS,OUG,Tunisia,6,3,DVS,,",
    ])
    results = ingest_s2_matches(db, io.StringIO(csv))
    assert results[0]["status"] == "error"


def test_duplicate_series_skipped(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    results = ingest_s2_matches(db, io.StringIO(BASIC))
    assert results[0]["status"] == "skipped"
    assert db.execute("SELECT COUNT(*) FROM matches").fetchone()[0] == 1


BANS_HEADER = "date,competition,team1,team2,banned_by,map"

# 6 attributed bans (3 per team) for the BASIC series.
BANS = BANS_HEADER + "\n" + "\n".join([
    "2026-06-25,CDM,DVS,OUG,DVS,Arsenal",
    "2026-06-25,CDM,DVS,OUG,DVS,Coastal",
    "2026-06-25,CDM,DVS,OUG,DVS,Standoff",
    "2026-06-25,CDM,DVS,OUG,OUG,Takeoff",
    "2026-06-25,CDM,DVS,OUG,OUG,Meltdown",
    "2026-06-25,CDM,DVS,OUG,OUG,Crossroads Strike",
])


def test_s2_bans_attributed(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    results = ingest_s2_bans(db, io.StringIO(BANS))
    assert results[0]["status"] == "ok"
    assert results[0]["bans"] == 6

    # Bans are attributed to the banning team.
    dvs = _id(db, "DVS")
    dvs_bans = db.execute(
        "SELECT mp.map_name FROM map_bans b JOIN maps mp ON b.map_id = mp.map_id "
        "WHERE b.team_id = ? ORDER BY mp.map_name", (dvs,)
    ).fetchall()
    assert [r[0] for r in dvs_bans] == ["Arsenal", "Coastal", "Standoff"]


def test_s2_bans_partial_reingest_adds_missing(db):
    # Ingest 3 bans, then re-run with all 6 -> the missing 3 get added, not skipped.
    ingest_s2_matches(db, io.StringIO(BASIC))
    half = BANS_HEADER + "\n" + "\n".join([
        "2026-06-25,CDM,DVS,OUG,DVS,Arsenal",
        "2026-06-25,CDM,DVS,OUG,DVS,Coastal",
        "2026-06-25,CDM,DVS,OUG,DVS,Standoff",
    ])
    ingest_s2_bans(db, io.StringIO(half))
    results = ingest_s2_bans(db, io.StringIO(BANS))
    assert results[0]["status"] == "ok"
    assert results[0]["bans"] == 3  # only the 3 new ones inserted
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 6


def test_s2_bans_missing_competition_gives_clear_error(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    # Row missing the competition value: 5 fields under a 6-column header.
    csv = BANS_HEADER + "\n2026-06-25,DVS,OUG,DVS,Arsenal"
    results = ingest_s2_bans(db, io.StringIO(csv))
    assert results[0]["status"] == "error"
    errs = " ".join(results[0]["errors"]).lower()
    assert "no matching series" not in errs        # don't mislead
    assert "field" in errs or "column" in errs or "competition" in errs


def test_s2_bans_match_not_found(db):
    # No matches ingested, so the ban's series has no match row.
    results = ingest_s2_bans(db, io.StringIO(BANS))
    assert results[0]["status"] == "error"


def test_s2_bans_unknown_map(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    csv = BANS_HEADER + "\n2026-06-25,CDM,DVS,OUG,DVS,NotAMap"
    results = ingest_s2_bans(db, io.StringIO(csv))
    assert results[0]["status"] == "error"


def test_s2_bans_unknown_banning_team(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    csv = BANS_HEADER + "\n2026-06-25,CDM,DVS,OUG,GL,Arsenal"
    results = ingest_s2_bans(db, io.StringIO(csv))
    assert results[0]["status"] == "error"


def test_s2_bans_already_exist_skipped(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    ingest_s2_bans(db, io.StringIO(BANS))
    results = ingest_s2_bans(db, io.StringIO(BANS))
    assert results[0]["status"] == "skipped"


def test_s2_bans_soft_count_warning(db):
    ingest_s2_matches(db, io.StringIO(BASIC))
    # Only 4 bans for a Bo5 (expected 6): still inserts, but warns.
    csv = BANS_HEADER + "\n" + "\n".join([
        "2026-06-25,CDM,DVS,OUG,DVS,Arsenal",
        "2026-06-25,CDM,DVS,OUG,DVS,Coastal",
        "2026-06-25,CDM,DVS,OUG,OUG,Takeoff",
        "2026-06-25,CDM,DVS,OUG,OUG,Meltdown",
    ])
    results = ingest_s2_bans(db, io.StringIO(csv))
    assert results[0]["status"] == "ok"
    assert results[0]["bans"] == 4
    assert "warning" in results[0]
