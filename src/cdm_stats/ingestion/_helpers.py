import csv
import sqlite3

from cdm_stats.db.queries import get_team_id_by_abbr, insert_map_ban
from cdm_stats.ingestion.formats import FORMATS


def derive_pick_context(
    slot: int, picker_score: int, opponent_score: int,
    *, win_threshold: int = 3, opponent_win_threshold: int | None = None,
) -> str:
    """
    Derive the pick context for a map result based on the series state.

    Args:
        slot: Map slot in the series (1-based)
        picker_score: The picking team's series score before this map
        opponent_score: The opponent's series score before this map
        win_threshold: Wins the picker needs to take the series (3 for BO5, 4 for BO7)
        opponent_win_threshold: Wins the opponent needs; defaults to win_threshold.
            Differs only in a seat-decider, where the advantaged team needs one fewer.

    Returns:
        One of: "Opener", "Neutral", "Must-Win", "Close-Out"

    Rules (each side measured against its own threshold):
    - Slot 1 is always the opener (first map)
    - If opponent is one win away and picker is not: Must-Win
    - If picker is one win away and opponent is not: Close-Out
    - If both are one win away (e.g. 2-2 in BO5): Must-Win
    - All other cases: Neutral
    """
    opp_threshold = win_threshold if opponent_win_threshold is None else opponent_win_threshold
    if slot == 1:
        return "Opener"
    if opponent_score == opp_threshold - 1 and picker_score < win_threshold:
        return "Must-Win"
    if picker_score == win_threshold - 1 and opponent_score < opp_threshold - 1:
        return "Close-Out"
    return "Neutral"


def resolve_result_id(
    conn: sqlite3.Connection,
    date: str,
    opponent_id: int,
    map_id: int,
    table: str,
    player_name: str,
) -> tuple[int | None, str | None]:
    """Resolve a player-stat CSV row to a map_result.

    Match on date where the opponent is on either side, then the specific map.
    A map can recur across same-day series (e.g. UB then Finals), so resolve
    sequentially: take the first matching map_result (in match/slot order) that
    doesn't already have a row for this player in `table`. CSV rows are authored
    in that same order, so the Nth CSV occurrence lands in the Nth series. This
    also subsumes duplicate-skip.

    Returns (result_id, None) on success, or (None, reason) when no map_result
    matches or every match already holds this player.

    `table` is interpolated into the SQL — pass a module-level constant, never
    user input.
    """
    result_rows = conn.execute(
        """SELECT mr.result_id
           FROM map_results mr
           JOIN matches mt ON mr.match_id = mt.match_id
           WHERE mt.match_date = ?
             AND (mt.team1_id = ? OR mt.team2_id = ?)
             AND mr.map_id = ?
           ORDER BY mt.match_id, mr.slot""",
        (date, opponent_id, opponent_id, map_id),
    ).fetchall()

    if not result_rows:
        return None, "No matching map_result found"

    for (rid,) in result_rows:
        already = conn.execute(
            f"SELECT 1 FROM {table} WHERE result_id = ? AND player_name = ?",
            (rid, player_name),
        ).fetchone()
        if not already:
            return rid, None

    return None, "duplicate"


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
    allowed_modes = FORMATS[match_format].ban_modes
    expected_count = FORMATS[match_format].expected_bans

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
