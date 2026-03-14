import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.avoidance import (
    pick_win_loss,
    defend_win_loss,
    avoidance_index,
    target_index,
    pick_context_distribution,
)


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


# DVS picks Tunisia (slot 1, SnD), wins 6-3
# OUG picks Summit (slot 2, HP), OUG wins 250-220
# DVS picks Raid (slot 3, Control), DVS wins 3-1
# OUG picks Slums (slot 4, SnD), DVS wins 6-2
MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


def _get_ids(db):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    tunisia = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'").fetchone()[0]
    slums_snd = db.execute("SELECT map_id FROM maps WHERE map_name = 'Slums' AND mode = 'SnD'").fetchone()[0]
    summit_hp = db.execute("SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'").fetchone()[0]
    return dvs, oug, tunisia, slums_snd, summit_hp


def test_pick_win_loss_dvs_tunisia(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = pick_win_loss(db, dvs, tunisia)
    assert result == {"wins": 1, "losses": 0}


def test_defend_win_loss_oug_tunisia(db):
    """OUG didn't pick Tunisia, DVS did. OUG's defend record on Tunisia."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    _, oug, tunisia, _, _ = _get_ids(db)
    result = defend_win_loss(db, oug, tunisia)
    assert result == {"wins": 0, "losses": 1}


def test_pick_win_loss_no_data(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, _, _, summit_hp = _get_ids(db)
    result = pick_win_loss(db, dvs, summit_hp)
    assert result == {"wins": 0, "losses": 0}


def test_avoidance_index_basic(db):
    """DVS had 1 SnD pick opportunity (slot 1) and picked Tunisia, not Slums SnD.
    So DVS's avoidance of Slums SnD = 1/1 = 100%."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, _, slums_snd, _ = _get_ids(db)
    result = avoidance_index(db, dvs, slums_snd)
    assert result["ratio"] == 1.0
    assert result["opportunities"] == 1


def test_target_index_basic(db):
    """OUG is the opponent. OUG had 1 SnD pick (slot 4) against DVS and chose Slums.
    So target index for DVS on Slums SnD = 0/1 = 0% (opponents DO pick it against DVS)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, _, slums_snd, _ = _get_ids(db)
    result = target_index(db, dvs, slums_snd)
    assert result["ratio"] == 0.0
    assert result["opportunities"] == 1


def test_pick_context_distribution(db):
    """DVS picked Tunisia in slot 1 (Opener context)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, dvs, tunisia)
    assert dist["Opener"] == 1
    assert dist.get("Neutral", 0) == 0
    assert dist.get("Must-Win", 0) == 0
