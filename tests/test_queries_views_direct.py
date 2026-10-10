"""Direct tests for db/queries_views.py paths the API tests do not reach."""
import io

from cdm_stats.db import queries_views as qv
from cdm_stats.ingestion.csv_loader import ingest_csv

MATCHES_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-20,GL,DVS,GL,1,Tunisia,GL,6,4
2026-01-20,GL,DVS,GL,2,Summit,DVS,250,240
2026-01-20,GL,DVS,GL,3,Raid,GL,3,2
2026-01-20,GL,DVS,GL,4,Slums,DVS,6,5
2026-01-20,GL,DVS,GL,5,Hacienda,GL,250,200
2026-02-01,GL,OUG,OUG,1,Slums,OUG,6,1
2026-02-01,GL,OUG,OUG,2,Summit,OUG,250,100
2026-02-01,GL,OUG,OUG,3,Raid,OUG,3,0"""


def _tid(db, abbr):
    return db.execute("SELECT team_id FROM teams WHERE abbreviation = ?", (abbr,)).fetchone()[0]


def _mid(db, name):
    return db.execute("SELECT map_id FROM maps WHERE map_name = ?", (name,)).fetchone()[0]


def test_map_results_detail_slot5_has_no_picker(db):
    ingest_csv(db, io.StringIO(MATCHES_CSV))
    rows = qv.map_results_detail(db, _tid(db, "DVS"), _mid(db, "Hacienda"))
    assert len(rows) == 1
    row = rows[0]
    assert row["opponent"] == "GL"
    assert row["picked_by"] == "N/A"
    assert row["result"] == "L"
    assert row["match_date"] == "2026-01-20"
