import pytest
from cdm_stats.db.queries import get_team_id_by_abbr, insert_match
from cdm_stats.db.queries_scope import teams_in_scope, opponents_in_scope, series_match_ids
from cdm_stats.metrics.filters import MatchFilter


@pytest.fixture
def scoped(db):
    """GL plays GAL (spring), Felines (spring, hidden), DVS (summer, GL as team2)."""
    ids = {a: get_team_id_by_abbr(db, a) for a in ("GL", "GAL", "Felines", "DVS", "OUG")}
    m = {
        "gal": insert_match(db, "2026-03-12", ids["GL"], ids["GAL"], ids["GL"], ids["GL"], season=1),
        "fel": insert_match(db, "2026-03-19", ids["GL"], ids["Felines"], ids["GL"], ids["GL"], season=1),
        "dvs": insert_match(db, "2026-07-16", ids["DVS"], ids["GL"], None, ids["DVS"],
                            match_format="Bo5", round_="Stage 2", season=2, competition="CDM"),
    }
    return db, ids, m


def test_teams_in_scope_excludes_hidden_and_teams_without_matches(scoped):
    db, ids, _ = scoped
    rows = teams_in_scope(db, MatchFilter())
    assert [r["abbreviation"] for r in rows] == ["DVS", "GAL", "GL"]
    gl = rows[2]
    assert gl["team_id"] == ids["GL"] and gl["team_name"] == "GodLike"


def test_teams_in_scope_respects_filter(scoped):
    db, _, _ = scoped
    rows = teams_in_scope(db, MatchFilter.from_event("summer"))
    assert [r["abbreviation"] for r in rows] == ["DVS", "GL"]
    assert teams_in_scope(db, MatchFilter(start="2026-12-01")) == []


def test_opponents_in_scope_covers_both_sides_minus_hidden(scoped):
    db, ids, _ = scoped
    assert [r["abbreviation"] for r in opponents_in_scope(db, ids["GL"], MatchFilter())] == ["DVS", "GAL"]
    assert [r["abbreviation"] for r in opponents_in_scope(db, ids["DVS"], MatchFilter())] == ["GL"]
    spring = opponents_in_scope(db, ids["GL"], MatchFilter.from_event("spring"))
    assert [r["abbreviation"] for r in spring] == ["GAL"]
    assert set(spring[0]) >= {"team_id", "abbreviation", "team_name"}


def test_series_match_ids_all_and_opponent(scoped):
    db, ids, m = scoped
    f = MatchFilter()
    assert sorted(series_match_ids(db, ids["GL"], f, "all")) == sorted(m.values())
    assert series_match_ids(db, ids["GL"], f, "DVS") == [m["dvs"]]
    assert series_match_ids(db, ids["GL"], f, "ZZZ") == []
    assert series_match_ids(db, ids["GL"], MatchFilter.from_event("spring"), "DVS") == []


def test_series_match_ids_last10_is_newest_ten_ties_by_match_id(db):
    gl, gal = get_team_id_by_abbr(db, "GL"), get_team_id_by_abbr(db, "GAL")
    older = [insert_match(db, f"2026-03-{d:02d}", gl, gal, gl, gl, season=1) for d in range(1, 11)]
    tie = insert_match(db, "2026-03-10", gl, gal, gl, gl, season=1, series_number=2)
    got = series_match_ids(db, gl, MatchFilter(), "last10")
    assert got == [tie, older[9]] + older[8:0:-1]
    assert older[0] not in got
