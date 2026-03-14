import csv
import sqlite3
from typing import IO
from cdm_stats.db.queries import (
    get_team_id_by_abbr,
    get_map_id,
    get_mode_for_slot,
    insert_match,
    insert_map_result,
)


def derive_pick_context(slot: int, picker_score: int, opponent_score: int) -> str:
    """
    Derive the pick context for a map result based on the series state.

    Args:
        slot: Map slot in the Best-of-5 series (1-5)
        picker_score: The picking team's series score before this map (0-2)
        opponent_score: The opponent's series score before this map (0-2)

    Returns:
        One of: "Opener", "Neutral", "Must-Win", "Close-Out", "Coin-Toss"

    Rules:
    - Slot 5 is always a coin toss (regardless of series score)
    - Slot 1 is always the opener (first map)
    - If opponent has 2 wins and picker has < 2: Must-Win
    - If picker has 2 wins and opponent has < 2: Close-Out
    - All other cases: Neutral
    """
    if slot == 5:
        return "Coin-Toss"
    if slot == 1:
        return "Opener"
    if opponent_score == 2 and picker_score < 2:
        return "Must-Win"
    if picker_score == 2 and opponent_score < 2:
        return "Close-Out"
    return "Neutral"


def _group_rows_by_match(reader: csv.DictReader) -> dict[tuple, list[dict]]:
    matches: dict[tuple, list[dict]] = {}
    for row in reader:
        key = (row["date"], row["team1"], row["team2"])
        matches.setdefault(key, []).append(row)
    return matches


def _validate_match(
    conn: sqlite3.Connection, key: tuple, rows: list[dict]
) -> list[str]:
    errors = []
    date, team1_abbr, team2_abbr = key

    if get_team_id_by_abbr(conn, team1_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team1_abbr}")
    if get_team_id_by_abbr(conn, team2_abbr) is None:
        errors.append(f"Unknown team abbreviation: {team2_abbr}")

    two_v_two = rows[0]["two_v_two_winner"]
    if two_v_two not in (team1_abbr, team2_abbr):
        errors.append(f"2v2 winner '{two_v_two}' is not one of the teams")

    slots = [int(r["slot"]) for r in rows]
    expected_slots = list(range(1, len(rows) + 1))
    if slots != expected_slots:
        errors.append(f"Slots are not sequential: {slots}")

    for row in rows:
        slot = int(row["slot"])
        mode = get_mode_for_slot(slot)
        if get_map_id(conn, row["map_name"], mode) is None:
            errors.append(f"Unknown map '{row['map_name']}' for mode '{mode}' at slot {slot}")
        if row["winner"] not in (team1_abbr, team2_abbr):
            errors.append(f"Winner '{row['winner']}' at slot {slot} is not one of the teams")

    # Check exactly one team reaches 3 wins
    t1_wins = sum(1 for r in rows if r["winner"] == team1_abbr)
    t2_wins = sum(1 for r in rows if r["winner"] == team2_abbr)
    if max(t1_wins, t2_wins) != 3:
        errors.append(f"No team reached 3 wins: {team1_abbr}={t1_wins}, {team2_abbr}={t2_wins}")

    return errors


def _is_duplicate_match(conn: sqlite3.Connection, date: str, team1_id: int, team2_id: int) -> bool:
    row = conn.execute(
        """SELECT 1 FROM matches
           WHERE match_date = ? AND (
               (team1_id = ? AND team2_id = ?) OR (team1_id = ? AND team2_id = ?)
           )""",
        (date, team1_id, team2_id, team2_id, team1_id),
    ).fetchone()
    return row is not None


def ingest_csv(conn: sqlite3.Connection, file: IO[str]) -> list[dict]:
    reader = csv.DictReader(file)
    grouped = _group_rows_by_match(reader)
    results = []

    for key, rows in grouped.items():
        date, team1_abbr, team2_abbr = key
        team1_id = get_team_id_by_abbr(conn, team1_abbr)
        team2_id = get_team_id_by_abbr(conn, team2_abbr)

        if team1_id and team2_id and _is_duplicate_match(conn, date, team1_id, team2_id):
            results.append({"match": key, "status": "skipped", "reason": "duplicate"})
            continue

        errors = _validate_match(conn, key, rows)
        if errors:
            results.append({"match": key, "status": "error", "errors": errors})
            continue

        two_v_two_id = get_team_id_by_abbr(conn, rows[0]["two_v_two_winner"])

        # Walk slots to derive fields
        t1_series = 0
        t2_series = 0
        prev_loser_id = None
        map_result_data = []

        for row in sorted(rows, key=lambda r: int(r["slot"])):
            slot = int(row["slot"])
            mode = get_mode_for_slot(slot)
            map_id = get_map_id(conn, row["map_name"], mode)
            winner_id = get_team_id_by_abbr(conn, row["winner"])
            winner_score = int(row["winner_score"])
            loser_score = int(row["loser_score"])

            # Derive picker
            if slot == 1:
                picker_id = two_v_two_id
            elif slot == 5:
                picker_id = None
            else:
                picker_id = prev_loser_id

            # Derive scores relative to picker
            if picker_id is None:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            elif picker_id == winner_id:
                picking_team_score = winner_score
                non_picking_team_score = loser_score
            else:
                picking_team_score = loser_score
                non_picking_team_score = winner_score

            # Derive pick context
            if picker_id is None:
                pick_context = derive_pick_context(slot, 0, 0)
            else:
                if picker_id == team1_id:
                    pick_context = derive_pick_context(slot, t1_series, t2_series)
                else:
                    pick_context = derive_pick_context(slot, t2_series, t1_series)

            map_result_data.append((
                slot, map_id, picker_id, winner_id,
                picking_team_score, non_picking_team_score,
                t1_series, t2_series, pick_context,
            ))

            # Update running scores and prev loser
            if winner_id == team1_id:
                t1_series += 1
                prev_loser_id = team2_id
            else:
                t2_series += 1
                prev_loser_id = team1_id

        # Derive series winner
        series_winner_id = team1_id if t1_series == 3 else team2_id

        # Atomic insert
        try:
            match_id = insert_match(conn, date, team1_id, team2_id, two_v_two_id, series_winner_id)
            for data in map_result_data:
                insert_map_result(conn, match_id, *data)
            conn.commit()
            results.append({"match": key, "status": "ok", "match_id": match_id})
        except Exception as e:
            conn.rollback()
            results.append({"match": key, "status": "error", "errors": [str(e)]})

    return results
