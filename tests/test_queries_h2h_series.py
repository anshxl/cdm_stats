import pytest
from cdm_stats.db.queries import get_team_id_by_abbr, insert_match
from cdm_stats.db.queries_views import h2h_series
from cdm_stats.metrics.filters import MatchFilter


@pytest.fixture
def meetings(db):
    """GL vs GAL three times (GL on either side), plus a GL vs DVS series."""
    ids = {a: get_team_id_by_abbr(db, a) for a in ("GL", "GAL", "DVS")}
    insert_match(db, "2026-03-12", ids["GL"], ids["GAL"], ids["GL"], ids["GL"], season=1)
    insert_match(db, "2026-03-26", ids["GAL"], ids["GL"], ids["GAL"], ids["GAL"], season=1)
    insert_match(db, "2026-07-16", ids["GAL"], ids["GL"], None, ids["GL"],
                 match_format="Bo5", round_="Stage 2", season=2, competition="CDM")
    insert_match(db, "2026-03-19", ids["GL"], ids["DVS"], ids["GL"], ids["DVS"], season=1)
    return db, ids


def test_h2h_series_counts_only_meetings_from_team_perspective(meetings):
    db, ids = meetings
    assert h2h_series(db, ids["GL"], ids["GAL"]) == {"wins": 2, "losses": 1}
    assert h2h_series(db, ids["GAL"], ids["GL"]) == {"wins": 1, "losses": 2}


def test_h2h_series_respects_filter_and_empty(meetings):
    db, ids = meetings
    assert h2h_series(db, ids["GL"], ids["GAL"], MatchFilter.from_event("summer")) == {"wins": 1, "losses": 0}
    assert h2h_series(db, ids["GL"], ids["GAL"], MatchFilter(end="2026-03-20")) == {"wins": 1, "losses": 0}
    assert h2h_series(db, ids["GAL"], ids["DVS"]) == {"wins": 0, "losses": 0}
