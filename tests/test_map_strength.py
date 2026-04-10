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


def _get_ids(db):
    dvs = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    tunisia = db.execute("SELECT map_id FROM maps WHERE map_name = 'Tunisia' AND mode = 'SnD'").fetchone()[0]
    summit_hp = db.execute("SELECT map_id FROM maps WHERE map_name = 'Summit' AND mode = 'HP'").fetchone()[0]
    raid = db.execute("SELECT map_id FROM maps WHERE map_name = 'Raid' AND mode = 'Control'").fetchone()[0]
    return dvs, oug, tunisia, summit_hp, raid


def test_map_strength_returns_dict_keys(db):
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert "rating" in result
    assert "weighted_sample" in result
    assert "total_played" in result
    assert "low_confidence" in result


def test_map_strength_win_is_positive(db):
    """DVS won on Tunisia — rating should be > 0.5."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert result["rating"] > 0.5
    assert result["total_played"] == 1


def test_map_strength_loss_is_below_half(db):
    """OUG lost on Tunisia — rating should be < 0.5."""
    from cdm_stats.metrics.map_strength import map_strength
    _, oug, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, oug, tunisia)
    assert result["rating"] < 0.5
    assert result["total_played"] == 1


def test_map_strength_no_data_returns_none_rating(db):
    """Team with no results on a map should get rating=None."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, _, _, _ = _get_ids(db)
    # DVS never played Hacienda HP
    hacienda = db.execute("SELECT map_id FROM maps WHERE map_name = 'Hacienda' AND mode = 'HP'").fetchone()[0]
    result = map_strength(db, dvs, hacienda)
    assert result["rating"] is None
    assert result["total_played"] == 0


def test_map_strength_low_confidence_under_3(db):
    """With only 1 game played, low_confidence should be True."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    assert result["low_confidence"] is True


def test_map_strength_opener_weighted_less(db):
    """Tunisia was played as Opener (slot 1, weight 0.5). Verify weighted_sample reflects this."""
    from cdm_stats.metrics.map_strength import map_strength
    dvs, _, tunisia, _, _ = _get_ids(db)
    result = map_strength(db, dvs, tunisia)
    # Opener weight = 0.5, opponent_quality ~1.0 (both start at 1000)
    # weighted_sample should be approximately 0.5
    assert result["weighted_sample"] < 1.0


def test_context_weights_constant():
    from cdm_stats.metrics.map_strength import CONTEXT_WEIGHTS
    assert CONTEXT_WEIGHTS["Must-Win"] == 3.0
    assert CONTEXT_WEIGHTS["Close-Out"] == 2.0
    assert CONTEXT_WEIGHTS["Neutral"] == 1.0
    assert CONTEXT_WEIGHTS["Opener"] == 0.5
    assert CONTEXT_WEIGHTS["Coin-Toss"] == 1.0
    assert CONTEXT_WEIGHTS["Unknown"] == 0.5


def test_all_team_map_strengths(db):
    """Bulk calculation should return dict keyed by (team_id, map_id)."""
    from cdm_stats.metrics.map_strength import all_team_map_strengths
    strengths = all_team_map_strengths(db)
    assert isinstance(strengths, dict)
    dvs, _, tunisia, _, _ = _get_ids(db)
    assert (dvs, tunisia) in strengths
    assert strengths[(dvs, tunisia)]["rating"] is not None
