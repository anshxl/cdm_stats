import sqlite3

K_FACTOR = 32
SEED_ELO = 1000.0
LOW_CONFIDENCE_THRESHOLD = 7

K_BY_FORMAT = {
    "CDL_BO5":         32,
    "CDL_PLAYOFF_BO5": 40,
    "CDL_PLAYOFF_BO7": 40,
    "TOURNAMENT_BO5":  32,
    "TOURNAMENT_BO7":  32,
}

MODE_MAX_MARGINS = {"SnD": 9, "HP": 250, "Control": 4}

# --- Inter-season regression & new-team seeding ---
# See docs/superpowers/specs/2026-06-28-inter-season-elo-regression-design.md
REGRESSION_RHO = 0.5       # carryover of prior-season spread (1.0 = no regression)
REGRESSION_MEAN = 1000.0   # structural centre of the rating system
K_EARLY = 48               # boosted K for the first EARLY_WINDOW matches of a regressed season
EARLY_WINDOW = 4

# Only league ("CDM") play feeds Elo. Split brackets (SPLIT II/III) are excluded
# — not all teams play them, so they would distort ratings. Season-1 rows predate
# the competition column (NULL) and are all CDM league play, so NULL counts.
ELO_COMPETITIONS = {"CDM"}

# Teams that left the league entering a season — excluded from the regression
# pool and given no seed. Keyed by the season being entered. Note: "not yet
# played" is NOT "dropped" (ETs is still in S2, so it is absent here).
DROPPED_ON_ENTRY = {2: {"Felines"}}

# "Masters" = teams that made the prior season's playoffs. Everyone else
# continuing is a "Challenger". Newcomers seed at the mean of the continuing
# Challengers' regressed seeds (a lower anchor than the full field). Keyed by
# the season being entered.
MASTERS_ON_ENTRY = {
    2: {"ALU", "DVS", "ELV", "GAL", "GL", "OUG", "Q9", "RVL", "Wolves", "XROCK"},
}

# THIS season's group placement — distinct from MASTERS_ON_ENTRY (prior-season
# playoff status): RVL and XROCK made S1 playoffs but were placed in the S2
# Challengers group. The two groups barely cross-play (Seat Deciders only), so
# the skill gap between them can't emerge from results — it is injected at
# season entry as a symmetric seed offset: Masters-group teams +GROUP_OFFSET,
# Challengers-group teams -GROUP_OFFSET. Seasons with no entry get no offset.
# ponytail: sets inferred from the S2 match graph, not an official source.
CHALLENGER_GROUP = {
    2: {"ETs", "PAC", "RAG", "RVL", "SPG", "XROCK", "i7"},
}
GROUP_OFFSET = 50.0  # half the intended Masters-Challengers entry gap (100 Elo)


def normalize_margin(winner_score: int, loser_score: int, mode: str) -> float:
    margin = abs(winner_score - loser_score)
    # Control allows both first-to-4 and first-to-3; the most you can win by is
    # your winning total, so normalize by winner_score instead of a fixed cap.
    denom = winner_score if mode == "Control" else MODE_MAX_MARGINS[mode]
    return margin / denom


def get_current_elo(conn: sqlite3.Connection, team_id: int, season: int = 1) -> float:
    row = conn.execute(
        """SELECT te.elo_after
           FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           WHERE te.team_id = ? AND m.season = ?
           ORDER BY te.match_date DESC, te.elo_id DESC LIMIT 1""",
        (team_id, season),
    ).fetchone()
    return row[0] if row else SEED_ELO


def get_elo_history(conn: sqlite3.Connection, team_id: int, season: int = 1) -> list[dict]:
    rows = conn.execute(
        """SELECT te.elo_after, te.match_date, te.match_id
           FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           WHERE te.team_id = ? AND m.season = ?
           ORDER BY te.match_date, te.elo_id""",
        (team_id, season),
    ).fetchall()
    return [{"elo_after": r[0], "match_date": r[1], "match_id": r[2]} for r in rows]


def is_low_confidence(conn: sqlite3.Connection, team_id: int, season: int = 1) -> bool:
    count = conn.execute(
        """SELECT COUNT(*)
           FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           WHERE te.team_id = ? AND m.season = ?""",
        (team_id, season),
    ).fetchone()[0]
    return count < LOW_CONFIDENCE_THRESHOLD


def _latest_season_elo(conn: sqlite3.Connection, team_id: int, season: int) -> float | None:
    """Team's most recent elo_after within `season`, or None if it has none."""
    row = conn.execute(
        """SELECT te.elo_after FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           WHERE te.team_id = ? AND m.season = ?
           ORDER BY te.match_date DESC, te.elo_id DESC LIMIT 1""",
        (team_id, season),
    ).fetchone()
    return row[0] if row else None


def _regress(prior_final: float) -> float:
    return REGRESSION_MEAN + REGRESSION_RHO * (prior_final - REGRESSION_MEAN)


def _newcomer_seed(conn: sqlite3.Connection, season: int) -> float:
    """Newcomer seed = mean of the continuing Challengers' regressed seeds.

    Challengers = continuing teams (prior-season history, not dropped) that did
    NOT make the prior season's playoffs (i.e. not in MASTERS_ON_ENTRY).
    """
    masters = MASTERS_ON_ENTRY.get(season, set())
    dropped = DROPPED_ON_ENTRY.get(season, set())
    rows = conn.execute(
        """SELECT DISTINCT te.team_id, t.abbreviation FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           JOIN teams t ON te.team_id = t.team_id
           WHERE m.season = ?""",
        (season - 1,),
    ).fetchall()
    seeds = [
        _regress(prior)
        for tid, abbr in rows
        if abbr not in dropped and abbr not in masters
        and (prior := _latest_season_elo(conn, tid, season - 1)) is not None
    ]
    # No Challenger field to anchor against (e.g. fresh DB) → league average.
    return sum(seeds) / len(seeds) if seeds else SEED_ELO


def _group_offset(abbr: str, season: int) -> float:
    challengers = CHALLENGER_GROUP.get(season)
    if challengers is None:
        return 0.0
    return -GROUP_OFFSET if abbr in challengers else GROUP_OFFSET


def season_entry_elo(conn: sqlite3.Connection, team_id: int, season: int) -> float:
    """Starting rating for `team_id` in its first match of `season`.

    Season 1 → seed (1000). Continuing team → regressed prior-season final.
    Newcomer (no prior history) → the continuing Challengers' mean seed.
    Seasons with group placement defined get the group seed offset on top.
    """
    if season <= 1:
        return SEED_ELO
    prior_final = _latest_season_elo(conn, team_id, season - 1)
    abbr = conn.execute(
        "SELECT abbreviation FROM teams WHERE team_id = ?", (team_id,)
    ).fetchone()[0]
    if prior_final is not None and abbr not in DROPPED_ON_ENTRY.get(season, set()):
        return _regress(prior_final) + _group_offset(abbr, season)
    return _newcomer_seed(conn, season) + _group_offset(abbr, season)


def _base_elo(conn: sqlite3.Connection, team_id: int, season: int) -> float:
    """In-season chaining base: latest in-season Elo, else the season-entry seed."""
    latest = _latest_season_elo(conn, team_id, season)
    return latest if latest is not None else season_entry_elo(conn, team_id, season)


def _team_k(conn: sqlite3.Connection, team_id: int, season: int, match_format: str) -> float:
    """Per-team K: boosted for the first EARLY_WINDOW matches of a regressed season."""
    base_k = K_BY_FORMAT.get(match_format, K_FACTOR)
    if season <= 1:
        return base_k
    played = conn.execute(
        """SELECT COUNT(*) FROM team_elo te
           JOIN matches m ON te.match_id = m.match_id
           WHERE te.team_id = ? AND m.season = ?""",
        (team_id, season),
    ).fetchone()[0]
    return K_EARLY if played < EARLY_WINDOW else base_k


def update_elo(conn: sqlite3.Connection, match_id: int) -> None:
    # Idempotent: skip if Elo rows already exist for this match
    existing = conn.execute(
        "SELECT COUNT(*) FROM team_elo WHERE match_id = ?", (match_id,)
    ).fetchone()[0]
    if existing > 0:
        return

    match = conn.execute(
        "SELECT team1_id, team2_id, series_winner_id, match_date, match_format, season, competition FROM matches WHERE match_id = ?",
        (match_id,),
    ).fetchone()
    team1_id, team2_id, winner_id, match_date, match_format, season, competition = match

    # Only CDM league play feeds Elo; split brackets are excluded (NULL == legacy
    # S1 league play). Skipped matches create no team_elo rows, so they never
    # affect chaining or the early-window K count.
    if competition is not None and competition not in ELO_COMPETITIONS:
        return

    # Season-aware base: chain off this season's latest Elo, or the season-entry
    # seed for a team's first match of the season. Per-team K (boosted early).
    elo1 = _base_elo(conn, team1_id, season)
    elo2 = _base_elo(conn, team2_id, season)
    k1 = _team_k(conn, team1_id, season, match_format)
    k2 = _team_k(conn, team2_id, season, match_format)

    expected1 = 1 / (1 + 10 ** ((elo2 - elo1) / 400))
    expected2 = 1 - expected1

    map_rows = conn.execute(
        """SELECT mr.winner_team_id, mr.picking_team_score, mr.non_picking_team_score, m.mode
           FROM map_results mr
           JOIN maps m ON mr.map_id = m.map_id
           WHERE mr.match_id = ? AND mr.dq = 0
           ORDER BY mr.slot""",
        (match_id,),
    ).fetchall()

    # Map-count anchor: actual is driven by the series map-count differential
    # (excluding DQ'd maps); margin is a small ±0.05 modifier. No floor — a
    # scrappy series winner with poor map margins can score below 0.5.
    total_maps = len(map_rows)
    if total_maps == 0:
        winner_actual = 0.5
    else:
        winner_map_wins = sum(1 for r in map_rows if r[0] == winner_id)
        diff = 2 * winner_map_wins - total_maps
        base = 0.5 + 0.4 * (diff / total_maps)

        signed_margins = []
        for map_winner_id, pick_score, non_pick_score, mode in map_rows:
            norm = normalize_margin(
                max(pick_score, non_pick_score),
                min(pick_score, non_pick_score),
                mode,
            )
            signed_margins.append(norm if map_winner_id == winner_id else -norm)
        avg_margin = sum(signed_margins) / total_maps

        winner_actual = max(0.0, min(1.0, base + avg_margin * 0.05))

    loser_actual = 1.0 - winner_actual

    result1 = winner_actual if winner_id == team1_id else loser_actual
    result2 = 1.0 - result1

    new_elo1 = elo1 + k1 * (result1 - expected1)
    new_elo2 = elo2 + k2 * (result2 - expected2)

    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team1_id, match_id, new_elo1, match_date),
    )
    conn.execute(
        "INSERT INTO team_elo (team_id, match_id, elo_after, match_date) VALUES (?, ?, ?, ?)",
        (team2_id, match_id, new_elo2, match_date),
    )
    conn.commit()


def recalculate_all_elo(conn: sqlite3.Connection) -> int:
    """Delete all Elo history and recompute from scratch in chronological order."""
    conn.execute("DELETE FROM team_elo")
    conn.commit()

    matches = conn.execute(
        "SELECT match_id FROM matches ORDER BY match_date, match_id"
    ).fetchall()

    for (match_id,) in matches:
        update_elo(conn, match_id)

    return len(matches)
