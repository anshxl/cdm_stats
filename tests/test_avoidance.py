import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.avoidance import (
    pick_win_loss,
    defend_win_loss,
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



def test_pick_context_distribution(db):
    """DVS picked Tunisia in slot 1 (Opener context)."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, dvs, tunisia)
    assert dist["Opener"] == 1
    assert dist.get("Neutral", 0) == 0
    assert dist.get("Must-Win", 0) == 0


# DVS picks Tunisia (slot 1) at face value, DVS "wins" 6-3, but the map is DQ'd.
# OUG picks Summit (slot 2, HP), OUG wins 250-220.
# DVS picks Raid (slot 3, Control), DVS wins 3-1.
# OUG picks Slums (slot 4, SnD), DVS wins 6-2.
DQ_PICK_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3,DVS,,1
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220,,,
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1,,,
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2,,,"""


def test_pick_win_loss_excludes_dq(db):
    """DVS's DQ'd Tunisia pick-win must not count toward pick W-L."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = pick_win_loss(db, dvs, tunisia)
    assert result == {"wins": 0, "losses": 0}


def test_defend_win_loss_excludes_dq(db):
    """OUG's DQ'd Tunisia defend-loss must not count toward defend W-L."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    _, oug, tunisia, _, _ = _get_ids(db)
    result = defend_win_loss(db, oug, tunisia)
    assert result == {"wins": 0, "losses": 0}


def test_pick_context_distribution_excludes_dq(db):
    """DVS's DQ'd Opener pick on Tunisia must not appear in the context distribution."""
    ingest_csv(db, io.StringIO(DQ_PICK_CSV))
    dvs, _, tunisia, _, _ = _get_ids(db)
    dist = pick_context_distribution(db, dvs, tunisia)
    assert dist == {"Opener": 0, "Neutral": 0, "Must-Win": 0, "Close-Out": 0}
