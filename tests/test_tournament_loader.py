import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# ELV 3-2 ALU in TOURNAMENT_BO5 (HP -> SnD -> Control -> HP -> SnD)
TOURNAMENT_BO5_MAPS = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

TOURNAMENT_BO5_BANS = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""

# ALU 4-2 ELV in TOURNAMENT_BO7 (HP -> SnD -> Control -> HP -> SnD -> Control -> SnD)
TOURNAMENT_BO7_MAPS = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Summit,ALU,250,200
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Tunisia,ELV,6,3
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Raid,ALU,3,1
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Hacienda,ALU,250,180
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Firing Range,ELV,6,4
2026-02-21,ELV,ALU,TOURNAMENT_BO7,Standoff,ALU,3,2"""

TOURNAMENT_BO7_BANS = """date,team1,team2,format,banned_by,map
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ELV,Hacienda
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ALU,Summit
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ELV,Tunisia
2026-02-21,ELV,ALU,TOURNAMENT_BO7,ALU,Firing Range"""


def test_tournament_bo5_creates_match_with_format(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    match = db.execute("SELECT match_format, two_v_two_winner_id FROM matches").fetchone()
    assert match[0] == "TOURNAMENT_BO5"
    assert match[1] is None


def test_tournament_bo5_creates_5_map_results(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 5


def test_tournament_all_picks_are_null(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    non_null = db.execute(
        "SELECT COUNT(*) FROM map_results WHERE picked_by_team_id IS NOT NULL"
    ).fetchone()[0]
    assert non_null == 0


def test_tournament_all_pick_context_unknown(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    contexts = db.execute(
        "SELECT DISTINCT pick_context FROM map_results"
    ).fetchall()
    assert contexts == [("Unknown",)]


def test_tournament_scores_are_team1_team2(db):
    """When pick is unknown, picking_team_score = team1's score, non_picking = team2's."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    # Slot 1: ELV (team1) 250, ALU (team2) 200
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (250, 200)


def test_tournament_bo5_creates_6_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 6


def test_tournament_bo7_creates_match(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    match = db.execute("SELECT match_format FROM matches").fetchone()
    assert match[0] == "TOURNAMENT_BO7"


def test_tournament_bo7_creates_6_map_results(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_results").fetchone()[0]
    assert count == 6


def test_tournament_bo7_creates_4_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO7_MAPS), io.StringIO(TOURNAMENT_BO7_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 4


def test_tournament_series_winner_correct(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    winner = db.execute("SELECT series_winner_id FROM matches").fetchone()[0]
    assert winner == elv_id


def test_tournament_elo_updated(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    elo_count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert elo_count == 2  # one row per team


def test_tournament_duplicate_match_skipped(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    results = ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    assert any(r["status"] == "skipped" for r in results)
    count = db.execute("SELECT COUNT(*) FROM matches").fetchone()[0]
    assert count == 1


def test_tournament_series_scores_tracked(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(TOURNAMENT_BO5_MAPS), io.StringIO(TOURNAMENT_BO5_BANS))
    rows = db.execute(
        "SELECT slot, team1_score_before, team2_score_before FROM map_results ORDER BY slot"
    ).fetchall()
    # ELV=team1, ALU=team2. Slot1: ELV wins -> 1-0. Slot2: ALU wins -> 1-1.
    # Slot3: ELV wins -> 2-1. Slot4: ALU wins -> 2-2. Slot5: ELV wins -> 3-2.
    assert rows[0][1:] == (0, 0)
    assert rows[1][1:] == (1, 0)
    assert rows[2][1:] == (1, 1)
    assert rows[3][1:] == (2, 1)
    assert rows[4][1:] == (2, 2)


# CDL Playoff BO5: SnD -> HP -> Control -> SnD -> HP
# ELV (higher seed) 3-1 ALU
PLAYOFF_BO5_MAPS = """date,team1,team2,format,higher_seed,map,winner,team1_score,team2_score
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Tunisia,ELV,6,3
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Summit,ALU,200,250
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Raid,ELV,3,1
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Slums,ELV,6,4"""

PLAYOFF_BO5_BANS = """date,team1,team2,format,banned_by,map
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Firing Range
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ALU,Meltdown
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ELV,Hacienda
2026-03-10,ELV,ALU,CDL_PLAYOFF_BO5,ALU,Takeoff"""


def test_playoff_bo5_creates_match_with_format(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    match = db.execute("SELECT match_format, two_v_two_winner_id FROM matches").fetchone()
    assert match[0] == "CDL_PLAYOFF_BO5"
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    assert match[1] == elv_id  # higher seed stored as two_v_two_winner_id


def test_playoff_bo5_picks_alternate_by_seed(db):
    """HS picks odd slots (1, 3), LS picks even slots (2, 4). No slot 5 played."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    elv_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ELV'").fetchone()[0]
    alu_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'ALU'").fetchone()[0]

    pickers = db.execute(
        "SELECT slot, picked_by_team_id FROM map_results ORDER BY slot"
    ).fetchall()
    assert pickers[0] == (1, elv_id)   # slot 1: HS
    assert pickers[1] == (2, alu_id)   # slot 2: LS
    assert pickers[2] == (3, elv_id)   # slot 3: HS
    assert pickers[3] == (4, alu_id)   # slot 4: LS


def test_playoff_bo5_pick_context_slot1_opener(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    ctx = db.execute("SELECT pick_context FROM map_results WHERE slot = 1").fetchone()[0]
    assert ctx == "Opener"


def test_playoff_bo5_scores_oriented_by_picker(db):
    """Slot 1: ELV picked, ELV won 6-3. picking=6, non=3."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 1"
    ).fetchone()
    assert row == (6, 3)


def test_playoff_bo5_scores_oriented_when_picker_loses(db):
    """Slot 2: ALU picked, ALU won 250-200. ELV=team1=200, ALU=team2=250.
    picker=ALU so picking_team_score=250, non=200."""
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    row = db.execute(
        "SELECT picking_team_score, non_picking_team_score FROM map_results WHERE slot = 2"
    ).fetchone()
    assert row == (250, 200)


def test_playoff_bo5_creates_4_bans(db):
    from cdm_stats.ingestion.tournament_loader import ingest_tournament
    ingest_tournament(db, io.StringIO(PLAYOFF_BO5_MAPS), io.StringIO(PLAYOFF_BO5_BANS))
    count = db.execute("SELECT COUNT(*) FROM map_bans").fetchone()[0]
    assert count == 4
