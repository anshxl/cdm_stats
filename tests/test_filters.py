import pytest
from cdm_stats.db.queries import get_team_id_by_abbr, insert_match
from cdm_stats.metrics.filters import MatchFilter, FAMILIES, FAMILY_SQL, match_label


@pytest.fixture
def db_three_families(db):
    """One match per family: S1 league (NULL comp), S2 CDM, S2 split."""
    gl, gal = get_team_id_by_abbr(db, "GL"), get_team_id_by_abbr(db, "GAL")
    insert_match(db, "2026-03-12", gl, gal, gl, gl, season=1)
    insert_match(db, "2026-07-16", gl, gal, None, gl, match_format="Bo5",
                 round_="Stage 2", season=2, competition="CDM")
    insert_match(db, "2026-09-06", gl, gal, None, gal, match_format="Bo7",
                 round_="Finals", season=2, competition="Regionals")
    return db


def _dates(db, f):
    where, params = f.sql()
    return [r[0] for r in db.execute(
        f"SELECT m.match_date FROM matches m WHERE 1=1 {where} ORDER BY m.match_date", params
    )]


def test_default_filter_has_no_clause():
    assert MatchFilter().sql() == ("", [])


def test_families_constant_lists_three():
    assert FAMILIES == ("CDM Spring", "CDM Summer", "Regionals")


@pytest.mark.parametrize("family,expected", [
    ("CDM Spring", ["2026-03-12"]),
    ("CDM Summer", ["2026-07-16"]),
    ("Regionals", ["2026-09-06"]),
])
def test_single_family_selects_only_its_rows(db_three_families, family, expected):
    assert _dates(db_three_families, MatchFilter(families=frozenset({family}))) == expected


def test_family_sql_derives_all_three(db_three_families):
    rows = db_three_families.execute(
        f"SELECT {FAMILY_SQL} FROM matches m ORDER BY m.match_date"
    ).fetchall()
    assert [r[0] for r in rows] == ["CDM Spring", "CDM Summer", "Regionals"]


def test_date_bounds_are_inclusive(db_three_families):
    f = MatchFilter(start="2026-07-16", end="2026-09-06")
    assert _dates(db_three_families, f) == ["2026-07-16", "2026-09-06"]


def test_start_only(db_three_families):
    assert _dates(db_three_families, MatchFilter(start="2026-07-17")) == ["2026-09-06"]


def test_match_label():
    assert match_label("Regionals", "Finals", 2) == "Regionals · Finals"
    assert match_label(None, None, 1) == "CDM Spring"
    assert match_label("CDM", "Stage 2", 2) == "CDM Summer · Stage 2"


def test_from_dict_roundtrip():
    f = MatchFilter.from_dict({"families": ["Regionals"], "start": "2026-08-01", "end": None})
    assert f == MatchFilter(families=frozenset({"Regionals"}), start="2026-08-01")
    assert MatchFilter.from_dict(None) == MatchFilter()
    assert MatchFilter.from_dict({}) == MatchFilter()


@pytest.mark.parametrize("event,families", [
    ("all", set(FAMILIES)),
    ("spring", {"CDM Spring"}),
    ("summer", {"CDM Summer"}),
    ("regionals", {"Regionals"}),
])
def test_from_event_maps_event_to_one_family(event, families):
    f = MatchFilter.from_event(event, "2026-03-01", "2026-09-30")
    assert f.families == frozenset(families)
    assert (f.start, f.end) == ("2026-03-01", "2026-09-30")


def test_from_event_defaults_to_no_dates():
    assert MatchFilter.from_event("all") == MatchFilter()


def test_from_event_rejects_unknown_event():
    with pytest.raises(ValueError):
        MatchFilter.from_event("winter")


def test_from_event_spring_selects_only_spring_matches(db_three_families):
    assert _dates(db_three_families, MatchFilter.from_event("spring")) == ["2026-03-12"]
