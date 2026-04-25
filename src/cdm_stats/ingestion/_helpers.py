import csv
import sqlite3

from cdm_stats.db.queries import get_team_id_by_abbr, insert_map_ban


FORMAT_BAN_MODES = {
    "TOURNAMENT_BO5": {"HP", "SnD", "Control"},
    "TOURNAMENT_BO7": {"HP", "SnD"},
    "CDL_PLAYOFF_BO5": {"HP", "SnD", "Control"},
    "CDL_PLAYOFF_BO7": {"HP", "SnD"},
}

FORMAT_EXPECTED_BANS = {
    "TOURNAMENT_BO5": 6,
    "TOURNAMENT_BO7": 4,
    "CDL_PLAYOFF_BO5": 6,
    "CDL_PLAYOFF_BO7": 4,
}


def derive_pick_context(
    slot: int, picker_score: int, opponent_score: int,
    *, win_threshold: int = 3,
) -> str:
    """
    Derive the pick context for a map result based on the series state.

    Args:
        slot: Map slot in the series (1-based)
        picker_score: The picking team's series score before this map
        opponent_score: The opponent's series score before this map
        win_threshold: Number of wins needed to take the series (3 for BO5, 4 for BO7)

    Returns:
        One of: "Opener", "Neutral", "Must-Win", "Close-Out"

    Rules:
    - Slot 1 is always the opener (first map)
    - If opponent is one win away and picker is not: Must-Win
    - If picker is one win away and opponent is not: Close-Out
    - If both are one win away (e.g. 2-2 in BO5): Must-Win
    - All other cases: Neutral
    """
    if slot == 1:
        return "Opener"
    if opponent_score == win_threshold - 1 and picker_score <= win_threshold - 1:
        return "Must-Win"
    if picker_score == win_threshold - 1 and opponent_score < win_threshold - 1:
        return "Close-Out"
    return "Neutral"


def group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["team1"], row["team2"])
        matches.setdefault(key, []).append(row)
    return matches


def is_duplicate_match(conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int) -> bool:
    row = conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return row is not None


def insert_bans_for_match(
    conn: sqlite3.Connection,
    match_id: int,
    match_format: str,
    bans: list[dict],
    team1_abbr: str,
    team2_abbr: str,
) -> dict:
    """Validate and insert bans for an existing match.

    Returns a partial result dict (without the "match" identity key — caller adds it):
      {"status": "skipped", "reason": "bans already exist"}
      {"status": "error", "errors": [...]}
      {"status": "ok", "bans": N}

    Skips if the match already has bans. Inserts under a single transaction and
    rolls back on any insert failure.
    """
    existing = conn.execute(
        "SELECT COUNT(*) FROM map_bans WHERE match_id = ?", (match_id,)
    ).fetchone()[0]
    if existing > 0:
        return {"status": "skipped", "reason": "bans already exist"}

    errors = _validate_bans(conn, bans, match_format, team1_abbr, team2_abbr)
    if errors:
        return {"status": "error", "errors": errors}

    try:
        for ban in bans:
            ban_team_id = get_team_id_by_abbr(conn, ban["banned_by"])
            map_row = conn.execute(
                "SELECT map_id FROM maps WHERE map_name = ?", (ban["map"],)
            ).fetchone()
            insert_map_ban(conn, match_id, ban_team_id, map_row[0])
        conn.commit()
        return {"status": "ok", "bans": len(bans)}
    except Exception as e:
        conn.rollback()
        return {"status": "error", "errors": [str(e)]}


def _validate_bans(
    conn: sqlite3.Connection,
    bans: list[dict],
    match_format: str,
    team1_abbr: str,
    team2_abbr: str,
) -> list[str]:
    errors = []
    allowed_modes = FORMAT_BAN_MODES[match_format]
    expected_count = FORMAT_EXPECTED_BANS[match_format]

    if len(bans) != expected_count:
        errors.append(f"Expected {expected_count} bans for {match_format}, got {len(bans)}")

    for ban in bans:
        if ban["banned_by"] not in (team1_abbr, team2_abbr):
            errors.append(f"Ban by '{ban['banned_by']}' is not one of the teams")
        map_row = conn.execute(
            "SELECT mode FROM maps WHERE map_name = ?", (ban["map"],)
        ).fetchone()
        if map_row is None:
            errors.append(f"Unknown map: {ban['map']}")
        elif map_row[0] not in allowed_modes:
            errors.append(
                f"Map '{ban['map']}' is mode '{map_row[0]}', not allowed for "
                f"{match_format} bans (allowed: {sorted(allowed_modes)})"
            )

    return errors
