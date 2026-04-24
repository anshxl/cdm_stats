import io
import sqlite3

import pytest

from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.playoff_loader import ingest_playoffs


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# Bo5 playoff match DVS 2-3 OUG on 2026-05-02. Played: Tunisia, Summit, Raid, Slums, Hacienda.
BO5_MATCH = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,2,Summit,OUG,250,200,OUG,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,3,Raid,OUG,4,2,,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,4,Slums,DVS,6,4,OUG,,
2026-05-02,Upper SF,Bo5,DVS,OUG,DVS,5,Hacienda,OUG,250,230,DVS,OUG,"""

# Bo7 playoff match DVS 4-3 OUG on 2026-05-11.
BO7_MATCH = """date,round,format,team1,team2,die_roll_winner,slot,map_name,winner,winner_score,loser_score,picked_by,series_winner,dq
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,2,Summit,OUG,250,200,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,3,Raid,OUG,4,2,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,4,Slums,DVS,6,4,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,5,Hacienda,OUG,250,230,DVS,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,6,Standoff,DVS,4,3,OUG,,
2026-05-11,Grand Final,Bo7,DVS,OUG,DVS,7,Firing Range,DVS,6,5,DVS,DVS,"""

# 6 bans for the Bo5 match, 3 per team, 1 per mode per team.
BO5_BANS = """date,team1,team2,banned_by,map
2026-05-02,DVS,OUG,DVS,Arsenal
2026-05-02,DVS,OUG,DVS,Meltdown
2026-05-02,DVS,OUG,DVS,Crossroads Strike
2026-05-02,DVS,OUG,OUG,Takeoff
2026-05-02,DVS,OUG,OUG,Coastal
2026-05-02,DVS,OUG,OUG,Standoff"""

# 4 bans for the Bo7 match, 2 per team, HP + SnD per team.
BO7_BANS = """date,team1,team2,banned_by,map
2026-05-11,DVS,OUG,DVS,Arsenal
2026-05-11,DVS,OUG,DVS,Meltdown
2026-05-11,DVS,OUG,OUG,Takeoff
2026-05-11,DVS,OUG,OUG,Coastal"""


# ---- Happy-path tests ----

def test_bo5_inserts_6_bans(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    results = ingest_playoff_bans(db, io.StringIO(BO5_BANS))
    assert len(results) == 1 and results[0]["status"] == "ok"
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 6


def test_bo5_ban_attributed_to_correct_team(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    ingest_playoff_bans(db, io.StringIO(BO5_BANS))
    row = db.execute("""
        SELECT t.abbreviation, mp.map_name, mp.mode
        FROM map_bans mb
        JOIN teams t ON t.team_id = mb.team_id
        JOIN maps mp ON mp.map_id = mb.map_id
        WHERE t.abbreviation = 'DVS' AND mp.map_name = 'Crossroads Strike'
    """).fetchone()
    assert row == ("DVS", "Crossroads Strike", "Control")


def test_bo7_inserts_4_bans(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO7_MATCH))
    results = ingest_playoff_bans(db, io.StringIO(BO7_BANS))
    assert len(results) == 1 and results[0]["status"] == "ok"
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 4


def test_match_lookup_works_regardless_of_team_order(db):
    """Bans CSV with team1/team2 swapped relative to stored match should still resolve."""
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    swapped = BO5_BANS.replace("DVS,OUG", "OUG,DVS")
    results = ingest_playoff_bans(db, io.StringIO(swapped))
    assert results[0]["status"] == "ok"
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 6


# ---- Error cases ----

def test_match_not_found_returns_error(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    results = ingest_playoff_bans(db, io.StringIO(BO5_BANS))
    assert results[0]["status"] == "error"
    assert any("Match not found" in e for e in results[0]["errors"])
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 0


def test_unknown_team_returns_error(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    bad = BO5_BANS.replace("DVS,OUG", "DVS,ZZZ", 1).replace("2026-05-02,DVS,OUG,", "2026-05-02,DVS,ZZZ,")
    # Rebuild with unknown team in team2 column — simpler to just hand-craft:
    bad = """date,team1,team2,banned_by,map
2026-05-02,DVS,ZZZ,DVS,Arsenal"""
    results = ingest_playoff_bans(db, io.StringIO(bad))
    assert results[0]["status"] == "error"


def test_unknown_map_returns_error(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    bad = BO5_BANS.replace("Arsenal", "Nonexistent Map")
    results = ingest_playoff_bans(db, io.StringIO(bad))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 0


def test_disallowed_mode_bo7_returns_error(db):
    """Control bans are not allowed in CDL_PLAYOFF_BO7."""
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO7_MATCH))
    bad = BO7_BANS.replace("Meltdown", "Crossroads Strike")
    results = ingest_playoff_bans(db, io.StringIO(bad))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 0


def test_wrong_total_count_returns_error(db):
    """A Bo5 series must have exactly 6 bans; 5 is an error."""
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    bans = "\n".join(BO5_BANS.splitlines()[:-1])
    results = ingest_playoff_bans(db, io.StringIO(bans))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 0


def test_ban_by_non_participant_returns_error(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    bad = BO5_BANS.replace("OUG,Takeoff", "GAL,Takeoff")
    results = ingest_playoff_bans(db, io.StringIO(bad))
    assert results[0]["status"] == "error"
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 0


# ---- Idempotency ----

def test_rerun_skipped_with_reason(db):
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    ingest_playoff_bans(db, io.StringIO(BO5_BANS))
    results = ingest_playoff_bans(db, io.StringIO(BO5_BANS))
    assert results[0]["status"] == "skipped"
    assert "bans already exist" in results[0]["reason"]
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 6


# ---- Partial CSV ----

def test_mixed_valid_invalid_series_processes_independently(db):
    """Valid series commits; invalid series reports error with no partial rows."""
    from cdm_stats.ingestion.playoff_bans_loader import ingest_playoff_bans
    ingest_playoffs(db, io.StringIO(BO5_MATCH))
    ingest_playoffs(db, io.StringIO(BO7_MATCH))
    # Valid Bo5 + invalid Bo7 (only 3 bans, should need 4)
    mixed = BO5_BANS + "\n" + "\n".join(BO7_BANS.splitlines()[1:-1])
    results = ingest_playoff_bans(db, io.StringIO(mixed))
    statuses = {r["status"] for r in results}
    assert "ok" in statuses and "error" in statuses
    # Only the Bo5 series' 6 bans should be in the DB.
    assert db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0] == 6
