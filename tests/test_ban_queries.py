import io
import sqlite3
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.tournament_loader import ingest_tournament
from cdm_stats.db.queries import get_ban_summary, get_team_id_by_abbr


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


MAPS_CSV = """date,team1,team2,format,map,winner,team1_score,team2_score
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Summit,ELV,250,200
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Tunisia,ALU,6,3
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Raid,ELV,3,1
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Hacienda,ALU,250,180
2026-02-20,ELV,ALU,TOURNAMENT_BO5,Firing Range,ELV,6,4"""

BANS_CSV = """date,team1,team2,format,banned_by,map
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Hacienda
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Summit
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Tunisia
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Firing Range
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ELV,Raid
2026-02-20,ELV,ALU,TOURNAMENT_BO5,ALU,Standoff"""


def test_ban_summary_returns_bans_for_team(db):
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    elv_id = get_team_id_by_abbr(db, "ELV")
    alu_id = get_team_id_by_abbr(db, "ALU")
    summary = get_ban_summary(db, elv_id, alu_id)
    assert len(summary) == 3  # ELV banned 3 maps
    assert all(s["total_series"] >= 1 for s in summary)


def test_ban_summary_correct_counts(db):
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    alu_id = get_team_id_by_abbr(db, "ALU")
    elv_id = get_team_id_by_abbr(db, "ELV")
    summary = get_ban_summary(db, alu_id, elv_id)
    # ALU banned Summit, Firing Range, Standoff — each once
    ban_maps = {s["map_name"] for s in summary}
    assert ban_maps == {"Summit", "Firing Range", "Standoff"}
    assert all(s["ban_count"] == 1 for s in summary)


def test_ban_summary_empty_for_no_bans(db):
    """No bans between teams that haven't played tournament matches."""
    elv_id = get_team_id_by_abbr(db, "ELV")
    dvs_id = get_team_id_by_abbr(db, "DVS")
    summary = get_ban_summary(db, elv_id, dvs_id)
    assert summary == []
