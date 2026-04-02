from cdm_stats.metrics.margin import dominance_flag, score_margins
import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv


def test_snd_dominant():
    assert dominance_flag("SnD", 9, 0) == "Dominant"
    assert dominance_flag("SnD", 8, 3) == "Dominant"
    assert dominance_flag("SnD", 7, 2) == "Dominant"


def test_snd_contested():
    assert dominance_flag("SnD", 6, 5) == "Contested"


def test_snd_normal():
    # margin of 2-4 is a solid win but not dominant
    assert dominance_flag("SnD", 6, 3) is None
    assert dominance_flag("SnD", 6, 4) is None


def test_hp_dominant():
    assert dominance_flag("HP", 250, 170) == "Dominant"
    assert dominance_flag("HP", 250, 180) == "Dominant"


def test_hp_contested():
    assert dominance_flag("HP", 250, 230) == "Contested"


def test_hp_normal():
    assert dominance_flag("HP", 250, 200) is None


def test_control_dominant():
    assert dominance_flag("Control", 4, 0) == "Dominant"
    assert dominance_flag("Control", 4, 1) == "Dominant"


def test_control_contested():
    assert dominance_flag("Control", 4, 3) == "Contested"


def test_control_normal():
    # 4-2 is solid but not dominant
    assert dominance_flag("Control", 4, 2) is None


MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,9,0
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,4,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    ingest_csv(conn, io.StringIO(MATCH_CSV))
    yield conn
    conn.close()


def test_score_margins_returns_list(db):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    tunisia = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'").fetchone()[0]
    margins = score_margins(db, dvs, tunisia)
    assert len(margins) == 1
    assert margins[0]["margin"] == 9  # 9 - 0
    assert margins[0]["dominance"] == "Dominant"
