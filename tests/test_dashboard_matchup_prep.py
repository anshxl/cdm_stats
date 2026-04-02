import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables, migrate
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    migrate(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    match_id = conn.execute("SELECT match_id FROM matches").fetchone()[0]
    update_elo(conn, match_id)
    yield conn
    conn.close()


def test_build_matchup_data(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    assert "SnD" in data
    assert "HP" in data
    assert "Control" in data
    snd_maps = data["SnD"]
    tunisia = next((m for m in snd_maps if m["map_name"] == "Tunisia"), None)
    assert tunisia is not None
    assert tunisia["h2h"]["wins"] == 1
    assert tunisia["h2h"]["losses"] == 0


def test_build_matchup_data_includes_wl_and_avoid(db):
    from cdm_stats.dashboard.tabs.matchup_prep import _build_matchup_data
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    data = _build_matchup_data(db, dvs_id, oug_id)
    for mode_maps in data.values():
        for m in mode_maps:
            assert "your_wl" in m
            assert "opp_wl" in m
            assert "your_avoid" in m
            assert "opp_avoid" in m
            assert "your_target" in m
            assert "opp_target" in m
            assert "your_pick_wl" in m
            assert "your_defend_wl" in m
            assert "opp_pick_wl" in m
            assert "opp_defend_wl" in m
