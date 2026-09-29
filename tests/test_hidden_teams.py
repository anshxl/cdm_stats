from cdm_stats.db.queries_scope import HIDDEN_TEAMS
from cdm_stats.db.queries import get_team_id_by_abbr, get_team_map_wl, insert_match, insert_map_result


def test_hidden_teams_constant():
    assert HIDDEN_TEAMS == {"Felines", "RAG", "i7"}


def test_metrics_still_count_hidden_opponents(db):
    gl, fel = get_team_id_by_abbr(db, "GL"), get_team_id_by_abbr(db, "Felines")
    assert fel is not None
    mid = insert_match(db, "2026-03-12", gl, fel, gl, gl)
    tunisia = db.execute("SELECT map_id FROM maps WHERE map_name='Tunisia'").fetchone()[0]
    insert_map_result(db, mid, 1, tunisia, gl, gl, 6, 3, 0, 0, "Opener")
    assert get_team_map_wl(db, gl) == [{"map_name": "Tunisia", "mode": "SnD", "wins": 1, "losses": 0}]
