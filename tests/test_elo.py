import sqlite3
import io
import pytest
from cdm_stats.db.schema import create_tables
from cdm_stats.ingestion.seed import seed_teams, seed_maps
from cdm_stats.ingestion.csv_loader import ingest_csv
from cdm_stats.metrics.elo import update_elo, get_current_elo, get_elo_history, normalize_margin, MODE_MAX_MARGINS, recalculate_all_elo

MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-15,DVS,OUG,DVS,1,Tunisia,DVS,6,3
2026-01-15,DVS,OUG,DVS,2,Summit,OUG,250,220
2026-01-15,DVS,OUG,DVS,3,Raid,DVS,3,1
2026-01-15,DVS,OUG,DVS,4,Slums,DVS,6,2"""


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    yield conn
    conn.close()


@pytest.fixture
def db_with_match(db):
    ingest_csv(db, io.StringIO(MATCH_CSV))
    match_id = db.execute("SELECT match_id FROM matches").fetchone()[0]
    return db, match_id


def test_update_elo_inserts_two_rows(db_with_match):
    db, match_id = db_with_match
    # ingest_csv already calls update_elo, so rows should exist
    count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert count == 2


def test_elo_functions_filter_by_season(db_with_match):
    from cdm_stats.metrics.elo import get_elo_history, is_low_confidence, SEED_ELO
    db, _ = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]

    # Season 1 has the match data
    assert len(get_elo_history(db, dvs_id, season=1)) == 1
    assert get_current_elo(db, dvs_id, season=1) > 1000
    # Season 2 is empty → seed/baseline behavior
    assert get_elo_history(db, dvs_id, season=2) == []
    assert get_current_elo(db, dvs_id, season=2) == SEED_ELO
    assert is_low_confidence(db, dvs_id, season=2) is True


def test_elo_winner_goes_up_loser_goes_down(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    oug_elo = get_current_elo(db, oug_id)
    assert dvs_elo > 1000  # winner
    assert oug_elo < 1000  # loser


def test_elo_changes_sum_to_zero(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    oug_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'OUG'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    oug_elo = get_current_elo(db, oug_id)
    assert abs((dvs_elo - 1000) + (oug_elo - 1000)) < 0.001


def test_get_current_elo_no_matches(db):
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    assert get_current_elo(db, dvs_id) == 1000.0


def test_get_elo_history(db_with_match):
    db, match_id = db_with_match
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    history = get_elo_history(db, dvs_id)
    assert len(history) == 1
    assert history[0]["elo_after"] > 1000


def test_mode_max_margins_values():
    assert MODE_MAX_MARGINS == {"SnD": 9, "HP": 250, "Control": 4}


def test_normalize_margin_snd():
    # 6-3 SnD = margin 3, normalized = 3/9 = 0.333...
    result = normalize_margin(6, 3, "SnD")
    assert abs(result - 3 / 9) < 0.001


def test_normalize_margin_hp():
    # 250-80 HP = margin 170, normalized = 170/250 = 0.68
    result = normalize_margin(250, 80, "HP")
    assert abs(result - 170 / 250) < 0.001


def test_normalize_margin_control():
    # 4-0 Control = margin 4, normalized = 4/4 = 1.0
    result = normalize_margin(4, 0, "Control")
    assert result == 1.0


def test_normalize_margin_control_first_to_3():
    # first-to-3 sweep (3-0) is a full sweep -> 1.0, not 3/4
    assert normalize_margin(3, 0, "Control") == 1.0
    # 3-2 nail-biter -> margin 1 over a 3-point game
    assert normalize_margin(3, 2, "Control") == 1 / 3


def test_normalize_margin_close_game():
    # 250-248 HP = margin 2, normalized = 2/250 = 0.008
    result = normalize_margin(250, 248, "HP")
    assert abs(result - 2 / 250) < 0.001


BLOWOUT_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-02-10,DVS,ELV,DVS,1,Tunisia,DVS,9,0
2026-02-10,DVS,ELV,DVS,2,Summit,DVS,250,80
2026-02-10,DVS,ELV,DVS,3,Raid,DVS,4,0"""

CLOSE_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-02-15,DVS,Q9,DVS,1,Tunisia,DVS,6,5
2026-02-15,DVS,Q9,DVS,2,Summit,DVS,250,248
2026-02-15,DVS,Q9,DVS,3,Raid,DVS,4,3"""


def test_blowout_moves_elo_more_than_close_win(db):
    """A 3-0 blowout should produce a larger Elo gain than a 3-0 nail-biter."""
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]

    # Ingest blowout match
    ingest_csv(db, io.StringIO(BLOWOUT_CSV))
    blowout_elo = get_current_elo(db, dvs_id)
    blowout_gain = blowout_elo - 1000

    # Reset: delete elo rows for clean comparison
    db.execute("DELETE FROM team_elo")
    db.execute("DELETE FROM map_results")
    db.execute("DELETE FROM matches")

    # Ingest close match
    ingest_csv(db, io.StringIO(CLOSE_CSV))
    close_elo = get_current_elo(db, dvs_id)
    close_gain = close_elo - 1000

    assert blowout_gain > close_gain
    assert blowout_gain > 0
    assert close_gain > 0


def test_elo_winner_always_gains(db):
    """Even when losses are more lopsided than wins, the series winner should not lose Elo."""
    # DVS wins 3-2 but loses maps by larger margins than wins
    mixed_csv = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-03-01,DVS,OUG,DVS,1,Tunisia,DVS,6,5
2026-03-01,DVS,OUG,DVS,2,Summit,OUG,250,100
2026-03-01,DVS,OUG,DVS,3,Raid,DVS,4,3
2026-03-01,DVS,OUG,DVS,4,Slums,OUG,9,1
2026-03-01,DVS,OUG,DVS,5,Hacienda,DVS,250,240"""
    ingest_csv(db, io.StringIO(mixed_csv))
    dvs_id = db.execute("SELECT team_id FROM teams WHERE abbreviation = 'DVS'").fetchone()[0]
    dvs_elo = get_current_elo(db, dvs_id)
    assert dvs_elo >= 1000  # must not lose Elo for winning


MATCH_CSV_2 = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score
2026-01-20,ELV,OUG,ELV,1,Tunisia,ELV,6,4
2026-01-20,ELV,OUG,ELV,2,Summit,ELV,250,200
2026-01-20,ELV,OUG,ELV,3,Raid,ELV,4,2"""


def test_recalculate_all_elo(db):
    """recalculate_all_elo should delete all elo rows and recompute from scratch."""
    ingest_csv(db, io.StringIO(MATCH_CSV))
    ingest_csv(db, io.StringIO(MATCH_CSV_2))

    original_count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert original_count == 4  # 2 matches x 2 teams

    recalculate_all_elo(db)

    new_count = db.execute("SELECT COUNT(*) FROM team_elo").fetchone()[0]
    assert new_count == 4  # same count after recalc

    # Verify chronological order was respected: first match processed before second
    rows = db.execute(
        "SELECT match_date, elo_id FROM team_elo ORDER BY elo_id"
    ).fetchall()
    dates = [r[0] for r in rows]
    assert dates == sorted(dates)


DQ_MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-03-20,DVS,OUG,DVS,1,Tunisia,DVS,6,4,,,
2026-03-20,DVS,OUG,DVS,2,Summit,OUG,250,80,,,1
2026-03-20,DVS,OUG,DVS,3,Raid,DVS,4,2,,,
2026-03-20,DVS,OUG,DVS,4,Slums,DVS,6,3,,,"""

FACE_VALUE_MATCH_CSV = """date,team1,team2,two_v_two_winner,slot,map_name,winner,winner_score,loser_score,series_winner,picked_by,dq
2026-03-20,DVS,OUG,DVS,1,Tunisia,DVS,6,4,,,
2026-03-20,DVS,OUG,DVS,2,Summit,OUG,250,80,,,
2026-03-20,DVS,OUG,DVS,3,Raid,DVS,4,2,,,
2026-03-20,DVS,OUG,DVS,4,Slums,DVS,6,3,,,"""


def _fresh_db():
    conn = sqlite3.connect(":memory:")
    create_tables(conn)
    seed_teams(conn)
    seed_maps(conn)
    return conn


def test_k_by_format_lookup():
    """K_BY_FORMAT exposes the per-format K values."""
    from cdm_stats.metrics.elo import K_BY_FORMAT
    assert K_BY_FORMAT["CDL_BO5"] == 32
    assert K_BY_FORMAT["CDL_PLAYOFF_BO5"] == 40
    assert K_BY_FORMAT["CDL_PLAYOFF_BO7"] == 40
    assert K_BY_FORMAT["TOURNAMENT_BO5"] == 32
    assert K_BY_FORMAT["TOURNAMENT_BO7"] == 32


def test_playoff_match_uses_k40(db):
    """Playoff matches produce a larger Elo swing than identical regular-season matches."""
    from cdm_stats.db.queries import get_team_id_by_abbr, insert_match, insert_map_result, get_map_id

    dvs_id = get_team_id_by_abbr(db, "DVS")
    oug_id = get_team_id_by_abbr(db, "OUG")
    tunisia_id = get_map_id(db, "Tunisia", "SnD")

    # Insert one regular-season match (DVS sweeps OUG 1-0 with a single map for clarity)
    reg_match_id = insert_match(
        db, "2026-01-01", dvs_id, oug_id, dvs_id, dvs_id,
        match_format="CDL_BO5",
    )
    insert_map_result(
        db, reg_match_id, 1, tunisia_id, dvs_id, dvs_id, 6, 3, 0, 0, "Opener",
    )
    db.commit()
    update_elo(db, reg_match_id)
    reg_dvs_elo = get_current_elo(db, dvs_id)

    # Reset for clean comparison
    db.execute("DELETE FROM team_elo")
    db.execute("DELETE FROM map_results")
    db.execute("DELETE FROM matches")
    db.commit()

    # Insert identical playoff match
    pf_match_id = insert_match(
        db, "2026-05-01", dvs_id, oug_id, dvs_id, dvs_id,
        match_format="CDL_PLAYOFF_BO5", round_="Upper QF",
    )
    insert_map_result(
        db, pf_match_id, 1, tunisia_id, dvs_id, dvs_id, 6, 3, 0, 0, "Opener",
    )
    db.commit()
    update_elo(db, pf_match_id)
    pf_dvs_elo = get_current_elo(db, dvs_id)

    # K=40 vs K=32 → playoff Elo swing is 40/32 = 1.25x bigger
    reg_delta = reg_dvs_elo - 1000
    pf_delta = pf_dvs_elo - 1000
    assert pf_delta > reg_delta
    assert abs(pf_delta / reg_delta - 40 / 32) < 0.001


def test_dq_map_excluded_from_elo_margin():
    """A DQ'd map's margin must not drag down the series winner's Elo gain."""
    conn_dq = _fresh_db()
    ingest_csv(conn_dq, io.StringIO(DQ_MATCH_CSV))
    dvs_id = conn_dq.execute("SELECT team_id FROM teams WHERE abbreviation='DVS'").fetchone()[0]
    dvs_elo_dq = get_current_elo(conn_dq, dvs_id)
    conn_dq.close()

    conn_face = _fresh_db()
    ingest_csv(conn_face, io.StringIO(FACE_VALUE_MATCH_CSV))
    dvs_elo_face = get_current_elo(conn_face, dvs_id)
    conn_face.close()

    # With DQ excluded, DVS's dominance score drops the huge negative margin
    # from Summit (OUG 250-80), so DVS should gain meaningfully more Elo.
    assert dvs_elo_dq > dvs_elo_face
    assert dvs_elo_dq - dvs_elo_face > 2.0  # sanity: the delta is non-trivial


# --- Inter-season regression & new-team seeding ---
# See docs/superpowers/specs/2026-06-28-inter-season-elo-regression-design.md

# Real S1-final ratings (from the live recompute) for the continuing S2 field.
S1_FINALS = {
    "ALU": 1117.6, "GL": 1086.7, "Q9": 1064.5, "OUG": 1059.4, "Wolves": 1059.4,
    "ELV": 1058.1, "GAL": 1054.4, "DVS": 974.9, "RVL": 961.2, "SPG": 948.9,
    "XROCK": 937.0, "PAC": 907.2, "ETs": 896.0, "Felines": 874.6,
}


def _tid(conn, abbr):
    return conn.execute("SELECT team_id FROM teams WHERE abbreviation=?", (abbr,)).fetchone()[0]


def _add_match_elo(conn, abbr, season, elo, date):
    """Give a team one match + elo_after row in `season` (for building state)."""
    tid = _tid(conn, abbr)
    opp = conn.execute("SELECT team_id FROM teams WHERE team_id!=? LIMIT 1", (tid,)).fetchone()[0]
    mid = conn.execute(
        "INSERT INTO matches (match_date, team1_id, team2_id, series_winner_id, season) "
        "VALUES (?,?,?,?,?)", (date, tid, opp, tid, season),
    ).lastrowid
    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?,?,?,?)",
        (tid, mid, elo, date),
    )
    conn.commit()
    return tid


def _seed_s1_field(conn):
    for abbr, elo in S1_FINALS.items():
        _add_match_elo(conn, abbr, 1, elo, "2026-04-25")


def test_season_entry_regresses_continuing_team(db):
    from cdm_stats.metrics.elo import season_entry_elo, REGRESSION_RHO, REGRESSION_MEAN
    _seed_s1_field(db)
    expected = REGRESSION_MEAN + REGRESSION_RHO * (1117.6 - REGRESSION_MEAN)  # ALU 1058.8
    assert season_entry_elo(db, _tid(db, "ALU"), 2) == pytest.approx(expected)


def test_newcomer_seeds_at_challenger_mean(db):
    from cdm_stats.metrics.elo import season_entry_elo
    _seed_s1_field(db)
    # Continuing Challengers (non-playoff, not dropped) are SPG, PAC, ETs.
    # Newcomers seed at the mean of their regressed seeds (~958.7).
    def regress(final):
        return 1000 + 0.5 * (final - 1000)
    expected = (regress(948.9) + regress(907.2) + regress(896.0)) / 3
    assert season_entry_elo(db, _tid(db, "RAG"), 2) == pytest.approx(expected, abs=0.05)
    assert season_entry_elo(db, _tid(db, "i7"), 2) == pytest.approx(expected, abs=0.05)
    # Lower than a Master's regressed seed, as intended.
    assert season_entry_elo(db, _tid(db, "RAG"), 2) < season_entry_elo(db, _tid(db, "GAL"), 2)


def test_masters_and_dropped_excluded_from_challenger_mean(db):
    from cdm_stats.metrics.elo import _newcomer_seed
    _seed_s1_field(db)
    base = _newcomer_seed(db, 2)              # Challengers: SPG, PAC, ETs
    # Dropping a Master (GAL) must NOT move the mean — Masters are never counted.
    db.execute("DELETE FROM team_elo WHERE team_id=?", (_tid(db, "GAL"),))
    db.commit()
    assert _newcomer_seed(db, 2) == pytest.approx(base)
    # Dropping a Challenger (ETs) DOES move it — it was in the pool.
    db.execute("DELETE FROM team_elo WHERE team_id=?", (_tid(db, "ETs"),))
    db.commit()
    assert _newcomer_seed(db, 2) != pytest.approx(base)


def test_s2_chains_off_in_season_not_s1_final(db):
    from cdm_stats.metrics.elo import _base_elo
    _add_match_elo(db, "ALU", 1, 1117.6, "2026-04-25")   # S1 final
    # First S2 match → uses the regressed seed, not the S1 final.
    assert _base_elo(db, _tid(db, "ALU"), 2) == pytest.approx(1058.8, abs=0.05)
    # After one S2 result, the next match chains off it (bug fix).
    _add_match_elo(db, "ALU", 2, 1070.0, "2026-06-20")
    assert _base_elo(db, _tid(db, "ALU"), 2) == pytest.approx(1070.0)


def test_early_season_k_bump(db):
    from cdm_stats.metrics.elo import _team_k, K_EARLY, K_BY_FORMAT, EARLY_WINDOW
    base_k = K_BY_FORMAT["CDL_BO5"]
    tid = _tid(db, "ALU")
    # Season 1 is never boosted.
    assert _team_k(db, tid, 1, "CDL_BO5") == base_k
    # S2: first EARLY_WINDOW matches boosted, then normal.
    for n in range(EARLY_WINDOW):
        assert _team_k(db, tid, 2, "CDL_BO5") == K_EARLY  # 0..3 played
        _add_match_elo(db, "ALU", 2, 1050.0 + n, f"2026-06-2{n}")
    assert _team_k(db, tid, 2, "CDL_BO5") == base_k        # 4 played → normal


def test_split_games_excluded_from_elo(db):
    from cdm_stats.metrics.elo import update_elo
    dvs, oug = _tid(db, "DVS"), _tid(db, "OUG")

    def make_match(comp):
        return db.execute(
            "INSERT INTO matches (match_date, team1_id, team2_id, series_winner_id, "
            "season, competition) VALUES ('2026-06-20',?,?,?,2,?)", (dvs, oug, dvs, comp),
        ).lastrowid
    cdm, split = make_match("CDM"), make_match("SPLIT II")
    db.commit()
    update_elo(db, cdm)
    update_elo(db, split)
    assert db.execute("SELECT COUNT(*) FROM team_elo WHERE match_id=?", (cdm,)).fetchone()[0] == 2
    assert db.execute("SELECT COUNT(*) FROM team_elo WHERE match_id=?", (split,)).fetchone()[0] == 0
