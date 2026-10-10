import io
import sqlite3
import pytest
from cdm_stats.metrics.filters import MatchFilter

SPRING = MatchFilter(families=frozenset({"CDM Spring"}))
SUMMER = MatchFilter(families=frozenset({"CDM Summer"}))
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.tournament_loader import ingest_tournament
from cdm_stats.db.queries import get_team_id_by_abbr


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


def test_team_ban_rates_counts_only_series_with_ban_data(db):
    from cdm_stats.db.queries import team_ban_rates, get_map_id
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    elv_id = get_team_id_by_abbr(db, "ELV")
    alu_id = get_team_id_by_abbr(db, "ALU")

    rates = team_ban_rates(db, elv_id)
    assert rates["total_series"] == 1
    hacienda_id = get_map_id(db, "Hacienda", "HP")
    assert rates["by_map"][hacienda_id] == 1
    assert len(rates["by_map"]) == 3  # ELV banned 3 maps

    # Opponent filter: vs ALU is the only series; vs another team, nothing.
    assert team_ban_rates(db, elv_id, opponent_id=alu_id)["total_series"] == 1
    gl_id = get_team_id_by_abbr(db, "GL")
    empty = team_ban_rates(db, elv_id, opponent_id=gl_id)
    assert empty == {"total_series": 0, "by_map": {}}

    assert team_ban_rates(db, elv_id, f=SUMMER)["total_series"] == 0


def test_opponent_ban_rates_counts_bans_against_team(db):
    from cdm_stats.db.queries import opponent_ban_rates, get_map_id
    ingest_tournament(db, io.StringIO(MAPS_CSV), io.StringIO(BANS_CSV))
    elv_id = get_team_id_by_abbr(db, "ELV")

    # Opponents of ELV (i.e. ALU) banned Summit, Firing Range, Standoff.
    rates = opponent_ban_rates(db, elv_id)
    assert rates["total_series"] == 1
    assert len(rates["by_map"]) == 3
    summit_id = get_map_id(db, "Summit", "HP")
    assert rates["by_map"][summit_id] == 1

    gl_id = get_team_id_by_abbr(db, "GL")
    assert opponent_ban_rates(db, gl_id) == {"total_series": 0, "by_map": {}}
    assert opponent_ban_rates(db, elv_id, f=SUMMER)["total_series"] == 0


