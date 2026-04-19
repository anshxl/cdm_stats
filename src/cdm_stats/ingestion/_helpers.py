import csv
import sqlite3


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
